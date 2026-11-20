//! Dual-backend SQLite & PostgreSQL database layer for AnyAgents.
//!
//! By default (`ANYAGENTS_DATABASE_URL` unset), AnyAgents stores all state in
//! local SQLite files (`state.db`, `memory-index.db`, `host-history-index-v1.db`),
//! preserving zero-dependency open-source operation. When an external PostgreSQL
//! URL is configured via CLI (`--database-url`), environment
//! (`ANYAGENTS_DATABASE_URL` / `ANYAGENTS_DATABASE_URL_FILE`), or explicit
//! programmatic configuration, control-plane state (`public`), mailbox search
//! (`memory_index`), and host transcript search (`host_index`) run inside
//! PostgreSQL using pooled connections compatible with both direct PostgreSQL
//! and transaction-mode PgBouncer.

use std::borrow::Cow;
use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, RwLock};
use std::time::Duration;

use rusqlite::{Connection, Row};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions, PgRow};
use sqlx::{
    Arguments as _, Column as _, PgPool, Postgres, Row as _, Transaction, TypeInfo as _,
    ValueRef as _,
};

use crate::error::Error;
use crate::id::{AgentId, ChannelId, MessageId, UsageId};
use crate::migrate::now_ms;
use crate::storage::storage_env_enabled;

pub const DATABASE_URL_ENV: &str = "ANYAGENTS_DATABASE_URL";
pub const DATABASE_URL_FILE_ENV: &str = "ANYAGENTS_DATABASE_URL_FILE";
pub const DIRECT_DATABASE_URL_ENV: &str = "ANYAGENTS_DIRECT_DATABASE_URL";
pub const DIRECT_DATABASE_URL_FILE_ENV: &str = "ANYAGENTS_DIRECT_DATABASE_URL_FILE";
pub const PG_MAX_CONNECTIONS_ENV: &str = "ANYAGENTS_PG_MAX_CONNECTIONS";
pub const PG_STATEMENT_CACHE_ENV: &str = "ANYAGENTS_PG_STATEMENT_CACHE";
pub const TEST_POSTGRES_URL_ENV: &str = "ANYAGENTS_TEST_POSTGRES_URL";

mod pg_migrations;
pub mod sqlite_to_pg;
pub use pg_migrations::{
    PG_AGENT_RUNTIME_SQL, PG_BROWSER_PROFILE_GRANTS_SQL, PG_DEPLOYMENT_SECRETS_SQL, PG_INIT_SQL,
    PG_LATEST_VERSION, PG_SERVERLESS_TURNS_SQL, PG_STATELESS_SQL,
};
pub use sqlite_to_pg::{
    MigrateToPostgresOptions, MigrateToPostgresReport, TableMigrationStat,
    migrate_sqlite_dir_to_postgres,
};

/// Pointer file inside `<data_dir>` storing an external database URL when set by
/// operator/deployment setup (0600 regular file). Env vars take precedence.
pub const DATABASE_URL_POINTER_FILE: &str = "database-url";

const PG_MIGRATION_LOCK_KEY: i64 = 0x616e_7961_676e_7473_u64 as i64; // "anyagnts"

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DbBackend {
    Sqlite,
    Postgres,
}

impl DbBackend {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sqlite => "sqlite",
            Self::Postgres => "postgres",
        }
    }

    pub fn sql_greatest(self, a: &str, b: &str) -> String {
        match self {
            Self::Sqlite => format!("MAX({a}, {b})"),
            Self::Postgres => format!("GREATEST({a}, {b})"),
        }
    }

    pub fn sql_least(self, a: &str, b: &str) -> String {
        match self {
            Self::Sqlite => format!("MIN({a}, {b})"),
            Self::Postgres => format!("LEAST({a}, {b})"),
        }
    }

    pub fn sql_not_passive_reaction(self, col: &str) -> String {
        match self {
            Self::Sqlite => format!(
                "(CASE WHEN json_valid({col}) THEN json_extract({col}, '$.event') END) IS NOT 'reaction_passive'"
            ),
            Self::Postgres => format!(
                "(CASE WHEN {col} IS NOT NULL AND {col} IS JSON THEN {col}::jsonb->>'event' ELSE NULL END) IS DISTINCT FROM 'reaction_passive'"
            ),
        }
    }

    pub fn sql_json_extract_i64(self, col: &str, field: &str) -> String {
        match self {
            Self::Sqlite => format!("json_extract({col}, '$.{field}')"),
            Self::Postgres => format!(
                "(CASE WHEN {col} IS NOT NULL AND {col} IS JSON THEN ({col}::jsonb->>'{field}')::bigint ELSE NULL END)"
            ),
        }
    }

    pub fn sql_like_ci(self) -> &'static str {
        match self {
            Self::Sqlite => "LIKE",
            Self::Postgres => "ILIKE",
        }
    }

    pub fn sql_instr_ci(self, haystack: &str, needle_placeholder: &str) -> String {
        match self {
            Self::Sqlite => format!("instr(lower({haystack}), lower({needle_placeholder})) > 0"),
            Self::Postgres => format!("strpos(lower({haystack}), lower({needle_placeholder})) > 0"),
        }
    }
}

/// Process-wide override set from `--database-url` CLI flag when invoked.
static CLI_DATABASE_URL: LazyLock<RwLock<Option<String>>> = LazyLock::new(|| RwLock::new(None));

/// Install a process-wide `--database-url` override from the CLI parser.
pub fn set_cli_database_url(url: Option<String>) {
    let cleaned = url.map(|s| s.trim().to_string()).filter(|s| !s.is_empty());
    if let Ok(mut guard) = CLI_DATABASE_URL.write() {
        *guard = cleaned;
    }
}

fn non_empty_trimmed_env(name: &str) -> Option<String> {
    let raw = std::env::var(name).ok()?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn read_url_file(path: &Path) -> Option<String> {
    let meta = std::fs::symlink_metadata(path).ok()?;
    if !meta.file_type().is_file() {
        return None;
    }
    let content = std::fs::read_to_string(path).ok()?;
    let trimmed = content.lines().next()?.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

pub fn is_postgres_url(url: &str) -> bool {
    let trimmed = url.trim();
    trimmed.starts_with("postgres://") || trimmed.starts_with("postgresql://")
}

/// Configured PostgreSQL URL from CLI flag or environment (`ANYAGENTS_DATABASE_URL`
/// / `ANYAGENTS_DATABASE_URL_FILE`), honoring test isolation via `storage_env_enabled()`.
pub fn configured_database_url() -> Option<String> {
    if let Ok(guard) = CLI_DATABASE_URL.read()
        && let Some(ref url) = *guard
    {
        return is_postgres_url(url).then(|| url.clone());
    }
    if !storage_env_enabled() {
        return None;
    }
    if let Some(url) = non_empty_trimmed_env(DATABASE_URL_ENV)
        && is_postgres_url(&url)
    {
        return Some(url);
    }
    if let Some(file) = non_empty_trimmed_env(DATABASE_URL_FILE_ENV)
        && let Some(url) = read_url_file(Path::new(&file))
        && is_postgres_url(&url)
    {
        return Some(url);
    }
    None
}

/// Configured PostgreSQL URL for `data_dir`: CLI/env override first, then an
/// optional `<data_dir>/database-url` pointer file when host storage env is active.
pub fn configured_database_url_for_data_dir(data_dir: &Path) -> Option<String> {
    if let Some(url) = configured_database_url() {
        return Some(url);
    }
    if storage_env_enabled()
        && !data_dir.as_os_str().is_empty()
        && let Some(url) = read_url_file(&data_dir.join(DATABASE_URL_POINTER_FILE))
        && is_postgres_url(&url)
    {
        return Some(url);
    }
    None
}

/// Direct PostgreSQL URL (bypassing PgBouncer) for DDL migrations, advisory
/// locks, and bulk copy, falling back to `primary`.
pub fn configured_direct_database_url(primary: &str) -> String {
    if storage_env_enabled() {
        if let Some(url) = non_empty_trimmed_env(DIRECT_DATABASE_URL_ENV)
            && is_postgres_url(&url)
        {
            return url;
        }
        if let Some(file) = non_empty_trimmed_env(DIRECT_DATABASE_URL_FILE_ENV)
            && let Some(url) = read_url_file(Path::new(&file))
            && is_postgres_url(&url)
        {
            return url;
        }
    }
    primary.to_string()
}

/// Mask any password embedded in a database connection URI before logging or
/// returning it in error messages.
pub fn redact_database_url(url: &str) -> String {
    let Some((scheme, rest)) = url.split_once("://") else {
        return url.to_string();
    };
    let Some((userinfo, host_part)) = rest.split_once('@') else {
        return url.to_string();
    };
    let redacted_user = match userinfo.split_once(':') {
        Some((user, _)) => format!("{user}:***"),
        None => "***".to_string(),
    };
    format!("{scheme}://{redacted_user}@{host_part}")
}

/// Dedicated multi-thread Tokio runtime driving SQLx PostgreSQL I/O so sync
/// `Store` / `HostIndex` / CLI callers never panic or starve an outer Tokio task.
static PG_RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .thread_name("anyagents-pg-io")
        .enable_all()
        .build()
        .expect("failed to build anyagents postgres runtime")
});

pub(crate) fn pg_runtime() -> &'static tokio::runtime::Runtime {
    &PG_RUNTIME
}

pub(crate) fn block_on_pg<F: std::future::Future>(fut: F) -> F::Output {
    if let Ok(handle) = tokio::runtime::Handle::try_current()
        && handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread
    {
        return tokio::task::block_in_place(|| PG_RUNTIME.block_on(fut));
    }
    PG_RUNTIME.block_on(fut)
}

fn pg_connect_options(url: &str) -> Result<PgConnectOptions, Error> {
    let mut opts = PgConnectOptions::from_str(url).map_err(|err| {
        Error::Message(format!(
            "invalid postgres url {}: {err}",
            redact_database_url(url)
        ))
    })?;
    // Default to 0 (unnamed extended queries) so transaction-mode PgBouncer
    // endpoints never fail on prepared statement collisions.
    let stmt_cache = non_empty_trimmed_env(PG_STATEMENT_CACHE_ENV)
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
    opts = opts
        .application_name("anyagents")
        .statement_cache_capacity(stmt_cache);
    Ok(opts)
}

pub(crate) fn build_pg_pool(url: &str, max_connections: Option<u32>) -> Result<PgPool, Error> {
    let opts = pg_connect_options(url)?;
    let max_conns = max_connections
        .or_else(|| {
            non_empty_trimmed_env(PG_MAX_CONNECTIONS_ENV).and_then(|v| v.parse::<u32>().ok())
        })
        .unwrap_or(24)
        .max(2);
    block_on_pg(async move {
        PgPoolOptions::new()
            .max_connections(max_conns)
            .acquire_timeout(Duration::from_secs(30))
            .idle_timeout(Some(Duration::from_secs(300)))
            .connect_with(opts)
            .await
            .map_err(Error::from)
    })
}

#[derive(Clone)]
pub(crate) struct PgHandle {
    #[allow(dead_code)]
    pub(crate) redacted_url: String,
    pub(crate) pool: PgPool,
    pub(crate) direct_url: String,
    has_direct_override: bool,
}

static SHARED_PG_HANDLES: LazyLock<std::sync::Mutex<HashMap<String, PgHandle>>> =
    LazyLock::new(std::sync::Mutex::default);

pub(crate) fn shared_pg_handle(url: &str) -> Result<PgHandle, Error> {
    let trimmed = url.trim();
    {
        let guard = SHARED_PG_HANDLES.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(handle) = guard.get(trimmed) {
            return Ok(handle.clone());
        }
    }
    let handle = PgHandle::connect(trimmed)?;
    let mut guard = SHARED_PG_HANDLES.lock().unwrap_or_else(|e| e.into_inner());
    let entry = guard.entry(trimmed.to_string()).or_insert(handle);
    Ok(entry.clone())
}

impl PgHandle {
    pub(crate) fn connect(url: &str) -> Result<Self, Error> {
        let direct_url = configured_direct_database_url(url);
        let has_direct_override = direct_url.trim() != url.trim();
        let pool = build_pg_pool(url, None)?;
        let handle = Self {
            redacted_url: redact_database_url(url),
            pool,
            direct_url,
            has_direct_override,
        };
        handle.ensure_schema()?;
        let direct = handle
            .has_direct_override
            .then_some(handle.direct_url.as_str());
        crate::store::ensure_pg_conversation_indexes(&handle.pool, direct, PG_MIGRATION_LOCK_KEY)?;
        Ok(handle)
    }

    pub(crate) fn ensure_schema(&self) -> Result<(), Error> {
        // Fast check on the primary pool first so CLI commands never open an
        // extra direct connection when schema is already at PG_LATEST_VERSION.
        let version: Option<i64> = block_on_pg(async {
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_name = 'pg_schema_migrations'
                )",
            )
            .fetch_one(&self.pool)
            .await?;
            if !exists {
                return Ok::<_, sqlx::Error>(None);
            }
            sqlx::query_scalar(
                "SELECT COALESCE(MAX(version), 0)::bigint FROM public.pg_schema_migrations",
            )
            .fetch_one(&self.pool)
            .await
            .map(Some)
        })?;
        if version.is_some_and(|v| v >= PG_LATEST_VERSION) {
            return Ok(());
        }

        let migration_pool = if self.has_direct_override {
            build_pg_pool(&self.direct_url, Some(2))?
        } else {
            self.pool.clone()
        };

        block_on_pg(async move {
            let mut conn = migration_pool.acquire().await?;
            // Serialize schema initialization across concurrent coordinator startups.
            sqlx::query("SELECT pg_advisory_lock($1)")
                .bind(PG_MIGRATION_LOCK_KEY)
                .execute(&mut *conn)
                .await?;
            let res: Result<(), sqlx::Error> = async {
                let exists: bool = sqlx::query_scalar(
                    "SELECT EXISTS(
                        SELECT 1 FROM information_schema.tables
                        WHERE table_schema = 'public' AND table_name = 'pg_schema_migrations'
                    )",
                )
                .fetch_one(&mut *conn)
                .await?;
                let current: i64 = if exists {
                    sqlx::query_scalar(
                        "SELECT COALESCE(MAX(version), 0)::bigint FROM public.pg_schema_migrations",
                    )
                    .fetch_one(&mut *conn)
                    .await?
                } else {
                    0
                };
                for &(ver, sql) in pg_migrations::PG_MIGRATIONS {
                    if current >= ver {
                        continue;
                    }
                    sqlx::raw_sql(sql).execute(&mut *conn).await?;
                    let now = now_ms();
                    sqlx::query(
                        "INSERT INTO public.pg_schema_migrations (version, applied_at_ms)
                         VALUES ($1, $2) ON CONFLICT (version) DO NOTHING",
                    )
                    .bind(ver)
                    .bind(now)
                    .execute(&mut *conn)
                    .await?;
                    if ver == 1 {
                        let compat_ver = crate::migrate::MIGRATIONS
                            .last()
                            .map(|(v, _)| *v)
                            .unwrap_or(40);
                        sqlx::query(
                            "INSERT INTO public.schema_migrations (version, applied_at_ms)
                             VALUES ($1, $2) ON CONFLICT (version) DO NOTHING",
                        )
                        .bind(compat_ver)
                        .bind(now)
                        .execute(&mut *conn)
                        .await?;
                    }
                }
                Ok(())
            }
            .await;
            let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
                .bind(PG_MIGRATION_LOCK_KEY)
                .execute(&mut *conn)
                .await;
            res?;
            Ok::<_, Error>(())
        })
    }
}

/// Ensure the PostgreSQL schema is initialized cleanly on an arbitrary URL
/// (used by CLI `migrate-to-postgres` before bulk streaming).
pub fn migrate_postgres(url: &str) -> Result<(), Error> {
    let _ = PgHandle::connect(url)?;
    Ok(())
}

/// Convert SQLite `?N` and anonymous `?` bind placeholders to PostgreSQL `$N`
/// Translate SQLite `?` / `?N` placeholders into PostgreSQL `$N` placeholders
/// and schema-qualify `memory_index.*` / `host_index.*` tables so queries stay
/// independent of session `search_path` under transaction-mode PgBouncer, while
/// preserving string literals, quoted identifiers, SQL comments, and PostgreSQL
/// `$$...$$` dollar-quoted blocks.
pub fn to_pg_sql(sql: &str) -> Cow<'_, str> {
    if !needs_pg_sql_translation(sql) {
        return Cow::Borrowed(sql);
    }
    static CACHE: LazyLock<RwLock<HashMap<String, Arc<str>>>> =
        LazyLock::new(|| RwLock::new(HashMap::new()));
    if let Ok(read) = CACHE.read()
        && let Some(hit) = read.get(sql)
    {
        return Cow::Owned(hit.to_string());
    }
    let translated = translate_placeholders_utf8(sql);
    if let Ok(mut write) = CACHE.write()
        && write.len() < 4096
    {
        write.insert(sql.to_string(), Arc::from(translated.as_str()));
    }
    Cow::Owned(translated)
}

fn needs_pg_sql_translation(sql: &str) -> bool {
    sql.as_bytes().contains(&b'?')
        || sql.contains("index_")
        || sql.contains("rag_chunks")
        || sql.contains("host_")
}

fn qualify_non_public_table(ident: &str) -> Option<&'static str> {
    match ident {
        "index_meta" => Some("memory_index.index_meta"),
        "index_pending" => Some("memory_index.index_pending"),
        "index_redactions" => Some("memory_index.index_redactions"),
        "rag_chunks" => Some("memory_index.rag_chunks"),
        "host_generation" => Some("host_index.host_generation"),
        "host_sources" => Some("host_index.host_sources"),
        "host_agent_trees" => Some("host_index.host_agent_trees"),
        "host_documents" => Some("host_index.host_documents"),
        "host_document_tools" => Some("host_index.host_document_tools"),
        "host_calls" => Some("host_index.host_calls"),
        "host_windows" => Some("host_index.host_windows"),
        _ => None,
    }
}

fn translate_placeholders_utf8(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len() + 32);
    let mut chars = sql.char_indices().peekable();
    let mut next_anon = 1usize;

    while let Some((idx, ch)) = chars.next() {
        match ch {
            '\'' | '"' => {
                let quote = ch;
                out.push(quote);
                while let Some((_, c)) = chars.next() {
                    out.push(c);
                    if c == quote {
                        if chars.peek().is_some_and(|&(_, next)| next == quote) {
                            let (_, esc) = chars.next().unwrap();
                            out.push(esc);
                        } else {
                            break;
                        }
                    }
                }
            }
            '-' if chars.peek().is_some_and(|&(_, c)| c == '-') => {
                out.push('-');
                out.push(chars.next().unwrap().1);
                for (_, c) in chars.by_ref() {
                    out.push(c);
                    if c == '\n' {
                        break;
                    }
                }
            }
            '/' if chars.peek().is_some_and(|&(_, c)| c == '*') => {
                out.push('/');
                out.push(chars.next().unwrap().1);
                while let Some((_, c)) = chars.next() {
                    out.push(c);
                    if c == '*' && chars.peek().is_some_and(|&(_, n)| n == '/') {
                        out.push(chars.next().unwrap().1);
                        break;
                    }
                }
            }
            '$' => {
                // Check for $$ or $tag$ dollar-quoted literal/body.
                let rest = &sql[idx + 1..];
                if let Some(end_tag) = rest.find('$')
                    && rest[..end_tag]
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_')
                {
                    let delim = &sql[idx..=idx + 1 + end_tag];
                    out.push_str(delim);
                    for _ in 0..=end_tag {
                        chars.next();
                    }
                    let body_start = idx + delim.len();
                    if let Some(close_rel) = sql[body_start..].find(delim) {
                        let close_end = body_start + close_rel + delim.len();
                        out.push_str(&sql[body_start..close_end]);
                        while chars.peek().is_some_and(|&(pos, _)| pos < close_end) {
                            chars.next();
                        }
                    }
                } else {
                    out.push('$');
                }
            }
            '?' => {
                let mut num = String::new();
                while let Some(&(_, d)) = chars.peek() {
                    if d.is_ascii_digit() {
                        num.push(d);
                        chars.next();
                    } else {
                        break;
                    }
                }
                out.push('$');
                if num.is_empty() {
                    out.push_str(&next_anon.to_string());
                    next_anon += 1;
                } else {
                    if let Ok(n) = num.parse::<usize>()
                        && n >= next_anon
                    {
                        next_anon = n + 1;
                    }
                    out.push_str(&num);
                }
            }
            c if c.is_ascii_alphabetic() || c == '_' => {
                let prev_is_dot = out.ends_with('.');
                let mut ident = String::new();
                ident.push(c);
                while let Some(&(_, next)) = chars.peek() {
                    if next.is_ascii_alphanumeric() || next == '_' {
                        ident.push(next);
                        chars.next();
                    } else {
                        break;
                    }
                }
                let next_is_dot = chars.peek().is_some_and(|&(_, next)| next == '.');
                if !prev_is_dot
                    && !next_is_dot
                    && let Some(qualified) = qualify_non_public_table(&ident)
                {
                    out.push_str(qualified);
                } else {
                    out.push_str(&ident);
                }
            }
            _ => out.push(ch),
        }
    }
    out
}

/// Typed database bind parameter preserving SQL type even when `None` (crucial
/// for PostgreSQL extended query type inference on nullable `BIGINT`/`TEXT`).
#[derive(Clone, Debug)]
pub(crate) enum DbParam {
    Bool(Option<bool>),
    I64(Option<i64>),
    F64(Option<f64>),
    Text(Option<String>),
    Blob(Option<Vec<u8>>),
}

pub(crate) trait ToDbParam {
    fn to_db_param(&self) -> DbParam;
}

impl ToDbParam for DbParam {
    fn to_db_param(&self) -> DbParam {
        self.clone()
    }
}
impl ToDbParam for bool {
    fn to_db_param(&self) -> DbParam {
        DbParam::Bool(Some(*self))
    }
}
impl ToDbParam for i64 {
    fn to_db_param(&self) -> DbParam {
        DbParam::I64(Some(*self))
    }
}
impl ToDbParam for i32 {
    fn to_db_param(&self) -> DbParam {
        DbParam::I64(Some(i64::from(*self)))
    }
}
impl ToDbParam for u64 {
    fn to_db_param(&self) -> DbParam {
        DbParam::I64(Some(*self as i64))
    }
}
impl ToDbParam for usize {
    fn to_db_param(&self) -> DbParam {
        DbParam::I64(Some(*self as i64))
    }
}
impl ToDbParam for f64 {
    fn to_db_param(&self) -> DbParam {
        DbParam::F64(Some(*self))
    }
}
impl ToDbParam for &str {
    fn to_db_param(&self) -> DbParam {
        DbParam::Text(Some((*self).to_string()))
    }
}
impl ToDbParam for String {
    fn to_db_param(&self) -> DbParam {
        DbParam::Text(Some(self.clone()))
    }
}
impl ToDbParam for std::borrow::Cow<'_, str> {
    fn to_db_param(&self) -> DbParam {
        DbParam::Text(Some(self.as_ref().to_string()))
    }
}
impl ToDbParam for &[u8] {
    fn to_db_param(&self) -> DbParam {
        DbParam::Blob(Some((*self).to_vec()))
    }
}
impl ToDbParam for Vec<u8> {
    fn to_db_param(&self) -> DbParam {
        DbParam::Blob(Some(self.clone()))
    }
}
impl ToDbParam for AgentId {
    fn to_db_param(&self) -> DbParam {
        DbParam::Text(Some(self.as_str().to_string()))
    }
}
impl ToDbParam for ChannelId {
    fn to_db_param(&self) -> DbParam {
        DbParam::Text(Some(self.as_str().to_string()))
    }
}
impl ToDbParam for MessageId {
    fn to_db_param(&self) -> DbParam {
        DbParam::Text(Some(self.as_str().to_string()))
    }
}
impl ToDbParam for UsageId {
    fn to_db_param(&self) -> DbParam {
        DbParam::Text(Some(self.as_str().to_string()))
    }
}

impl<T: ToDbParam + ?Sized> ToDbParam for &T {
    fn to_db_param(&self) -> DbParam {
        (*self).to_db_param()
    }
}

impl<T: ToDbParam + NullableTypeTag> ToDbParam for Option<T> {
    fn to_db_param(&self) -> DbParam {
        match self {
            Some(v) => v.to_db_param(),
            None => T::null_param(),
        }
    }
}

pub(crate) trait NullableTypeTag {
    fn null_param() -> DbParam;
}
impl NullableTypeTag for bool {
    fn null_param() -> DbParam {
        DbParam::Bool(None)
    }
}
impl NullableTypeTag for i64 {
    fn null_param() -> DbParam {
        DbParam::I64(None)
    }
}
impl NullableTypeTag for i32 {
    fn null_param() -> DbParam {
        DbParam::I64(None)
    }
}
impl NullableTypeTag for usize {
    fn null_param() -> DbParam {
        DbParam::I64(None)
    }
}
impl NullableTypeTag for u64 {
    fn null_param() -> DbParam {
        DbParam::I64(None)
    }
}
impl NullableTypeTag for f64 {
    fn null_param() -> DbParam {
        DbParam::F64(None)
    }
}
impl NullableTypeTag for &str {
    fn null_param() -> DbParam {
        DbParam::Text(None)
    }
}
impl NullableTypeTag for String {
    fn null_param() -> DbParam {
        DbParam::Text(None)
    }
}
impl NullableTypeTag for AgentId {
    fn null_param() -> DbParam {
        DbParam::Text(None)
    }
}
impl NullableTypeTag for ChannelId {
    fn null_param() -> DbParam {
        DbParam::Text(None)
    }
}
impl NullableTypeTag for MessageId {
    fn null_param() -> DbParam {
        DbParam::Text(None)
    }
}
impl NullableTypeTag for UsageId {
    fn null_param() -> DbParam {
        DbParam::Text(None)
    }
}
impl NullableTypeTag for std::borrow::Cow<'_, str> {
    fn null_param() -> DbParam {
        DbParam::Text(None)
    }
}
impl NullableTypeTag for Vec<u8> {
    fn null_param() -> DbParam {
        DbParam::Blob(None)
    }
}
impl<T: NullableTypeTag> NullableTypeTag for &T {
    fn null_param() -> DbParam {
        T::null_param()
    }
}

impl rusqlite::ToSql for DbParam {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        match self {
            DbParam::Bool(Some(v)) => v.to_sql(),
            DbParam::I64(Some(v)) => v.to_sql(),
            DbParam::F64(Some(v)) => v.to_sql(),
            DbParam::Text(Some(v)) => v.to_sql(),
            DbParam::Blob(Some(v)) => v.to_sql(),
            DbParam::Bool(None)
            | DbParam::I64(None)
            | DbParam::F64(None)
            | DbParam::Text(None)
            | DbParam::Blob(None) => Ok(rusqlite::types::ToSqlOutput::Owned(
                rusqlite::types::Value::Null,
            )),
        }
    }
}

#[macro_export]
macro_rules! db_params {
    () => {
        Vec::<$crate::db::DbParam>::new()
    };
    ($($x:expr),+ $(,)?) => {
        Vec::from([$($crate::db::ToDbParam::to_db_param(&$x)),+])
    };
}
pub(crate) use db_params;

fn bind_pg_params(params: &[DbParam]) -> Result<sqlx::postgres::PgArguments, Error> {
    let mut args = sqlx::postgres::PgArguments::default();
    for p in params {
        let res = match p {
            // Integer flag columns in PG schema are BIGINT (`0`/`1`).
            DbParam::Bool(v) => args.add(v.map(i64::from)),
            DbParam::I64(v) => args.add(*v),
            DbParam::F64(v) => args.add(*v),
            DbParam::Text(v) => args.add(v.clone()),
            DbParam::Blob(v) => args.add(v.clone()),
        };
        res.map_err(|err| Error::Message(format!("postgres bind error: {err}")))?;
    }
    Ok(args)
}

/// Borrowed database row across SQLite and PostgreSQL.
pub(crate) enum DbRow<'a> {
    Sqlite(&'a Row<'a>),
    Postgres(&'a PgRow),
}

pub(crate) trait FromDbValue: Sized {
    fn from_db_row(row: &DbRow<'_>, index: usize) -> Result<Self, Error>;
}

impl<'a> DbRow<'a> {
    pub fn get<T: FromDbValue>(&self, index: usize) -> Result<T, Error> {
        T::from_db_row(self, index)
    }

    pub fn parse_str<T>(&self, index: usize) -> Result<T, Error>
    where
        T: std::str::FromStr,
        T::Err: std::fmt::Display,
    {
        let text: String = self.get(index)?;
        text.parse()
            .map_err(|err| Error::Message(format!("invalid column {index} value `{text}`: {err}")))
    }

    pub fn opt_parse_str<T>(&self, index: usize) -> Result<Option<T>, Error>
    where
        T: std::str::FromStr,
        T::Err: std::fmt::Display,
    {
        let text: Option<String> = self.get(index)?;
        text.map(|text| {
            text.parse().map_err(|err| {
                Error::Message(format!("invalid column {index} value `{text}`: {err}"))
            })
        })
        .transpose()
    }

    pub fn json<T: serde::de::DeserializeOwned>(&self, index: usize) -> Result<T, Error> {
        let text: String = self.get(index)?;
        serde_json::from_str(&text)
            .map_err(|err| Error::Message(format!("invalid json in column {index}: {err}")))
    }
}

fn pg_decode_opt_i64(row: &PgRow, index: usize) -> Result<Option<i64>, Error> {
    let raw = row.try_get_raw(index)?;
    if raw.is_null() {
        return Ok(None);
    }
    let name = row.column(index).type_info().name();
    match name {
        "INT8" => row.try_get::<i64, _>(index).map(Some).map_err(Error::from),
        "INT4" => row
            .try_get::<i32, _>(index)
            .map(|v| Some(i64::from(v)))
            .map_err(Error::from),
        "INT2" => row
            .try_get::<i16, _>(index)
            .map(|v| Some(i64::from(v)))
            .map_err(Error::from),
        "BOOL" => row
            .try_get::<bool, _>(index)
            .map(|v| Some(i64::from(v)))
            .map_err(Error::from),
        other => Err(Error::Message(format!(
            "unexpected postgres column type `{other}` at column {index} for integer"
        ))),
    }
}

fn pg_decode_opt_bool(row: &PgRow, index: usize) -> Result<Option<bool>, Error> {
    let raw = row.try_get_raw(index)?;
    if raw.is_null() {
        return Ok(None);
    }
    let name = row.column(index).type_info().name();
    match name {
        "BOOL" => row.try_get::<bool, _>(index).map(Some).map_err(Error::from),
        "INT8" => row
            .try_get::<i64, _>(index)
            .map(|v| Some(v != 0))
            .map_err(Error::from),
        "INT4" => row
            .try_get::<i32, _>(index)
            .map(|v| Some(v != 0))
            .map_err(Error::from),
        "INT2" => row
            .try_get::<i16, _>(index)
            .map(|v| Some(v != 0))
            .map_err(Error::from),
        other => Err(Error::Message(format!(
            "unexpected postgres column type `{other}` at column {index} for bool"
        ))),
    }
}

fn pg_decode_opt_f64(row: &PgRow, index: usize) -> Result<Option<f64>, Error> {
    let raw = row.try_get_raw(index)?;
    if raw.is_null() {
        return Ok(None);
    }
    let name = row.column(index).type_info().name();
    match name {
        "FLOAT8" => row.try_get::<f64, _>(index).map(Some).map_err(Error::from),
        "FLOAT4" => row
            .try_get::<f32, _>(index)
            .map(|v| Some(f64::from(v)))
            .map_err(Error::from),
        "INT8" => row
            .try_get::<i64, _>(index)
            .map(|v| Some(v as f64))
            .map_err(Error::from),
        "INT4" => row
            .try_get::<i32, _>(index)
            .map(|v| Some(f64::from(v)))
            .map_err(Error::from),
        other => Err(Error::Message(format!(
            "unexpected postgres column type `{other}` at column {index} for float"
        ))),
    }
}

impl FromDbValue for i64 {
    fn from_db_row(row: &DbRow<'_>, index: usize) -> Result<Self, Error> {
        match row {
            DbRow::Sqlite(r) => Ok(r.get(index)?),
            DbRow::Postgres(r) => pg_decode_opt_i64(r, index)?
                .ok_or_else(|| Error::Message(format!("unexpected NULL at column {index}"))),
        }
    }
}
impl FromDbValue for Option<i64> {
    fn from_db_row(row: &DbRow<'_>, index: usize) -> Result<Self, Error> {
        match row {
            DbRow::Sqlite(r) => Ok(r.get(index)?),
            DbRow::Postgres(r) => pg_decode_opt_i64(r, index),
        }
    }
}
impl FromDbValue for usize {
    fn from_db_row(row: &DbRow<'_>, index: usize) -> Result<Self, Error> {
        let v: i64 = FromDbValue::from_db_row(row, index)?;
        usize::try_from(v)
            .map_err(|_| Error::Message(format!("negative integer {v} at col {index}")))
    }
}
impl FromDbValue for u64 {
    fn from_db_row(row: &DbRow<'_>, index: usize) -> Result<Self, Error> {
        let v: i64 = FromDbValue::from_db_row(row, index)?;
        u64::try_from(v).map_err(|_| Error::Message(format!("negative integer {v} at col {index}")))
    }
}
impl FromDbValue for bool {
    fn from_db_row(row: &DbRow<'_>, index: usize) -> Result<Self, Error> {
        match row {
            DbRow::Sqlite(r) => Ok(r.get(index)?),
            DbRow::Postgres(r) => pg_decode_opt_bool(r, index)?
                .ok_or_else(|| Error::Message(format!("unexpected NULL at column {index}"))),
        }
    }
}
impl FromDbValue for Option<bool> {
    fn from_db_row(row: &DbRow<'_>, index: usize) -> Result<Self, Error> {
        match row {
            DbRow::Sqlite(r) => Ok(r.get(index)?),
            DbRow::Postgres(r) => pg_decode_opt_bool(r, index),
        }
    }
}
impl FromDbValue for f64 {
    fn from_db_row(row: &DbRow<'_>, index: usize) -> Result<Self, Error> {
        match row {
            DbRow::Sqlite(r) => Ok(r.get(index)?),
            DbRow::Postgres(r) => pg_decode_opt_f64(r, index)?
                .ok_or_else(|| Error::Message(format!("unexpected NULL at column {index}"))),
        }
    }
}
impl FromDbValue for Option<f64> {
    fn from_db_row(row: &DbRow<'_>, index: usize) -> Result<Self, Error> {
        match row {
            DbRow::Sqlite(r) => Ok(r.get(index)?),
            DbRow::Postgres(r) => pg_decode_opt_f64(r, index),
        }
    }
}
impl FromDbValue for String {
    fn from_db_row(row: &DbRow<'_>, index: usize) -> Result<Self, Error> {
        match row {
            DbRow::Sqlite(r) => Ok(r.get(index)?),
            DbRow::Postgres(r) => Ok(r.try_get(index)?),
        }
    }
}
impl FromDbValue for Option<String> {
    fn from_db_row(row: &DbRow<'_>, index: usize) -> Result<Self, Error> {
        match row {
            DbRow::Sqlite(r) => Ok(r.get(index)?),
            DbRow::Postgres(r) => Ok(r.try_get(index)?),
        }
    }
}
impl FromDbValue for Vec<u8> {
    fn from_db_row(row: &DbRow<'_>, index: usize) -> Result<Self, Error> {
        match row {
            DbRow::Sqlite(r) => Ok(r.get(index)?),
            DbRow::Postgres(r) => Ok(r.try_get(index)?),
        }
    }
}
impl FromDbValue for Option<Vec<u8>> {
    fn from_db_row(row: &DbRow<'_>, index: usize) -> Result<Self, Error> {
        match row {
            DbRow::Sqlite(r) => Ok(r.get(index)?),
            DbRow::Postgres(r) => Ok(r.try_get(index)?),
        }
    }
}
impl FromDbValue for AgentId {
    fn from_db_row(row: &DbRow<'_>, index: usize) -> Result<Self, Error> {
        let s: String = FromDbValue::from_db_row(row, index)?;
        AgentId::parse(&s)
    }
}
impl FromDbValue for Option<AgentId> {
    fn from_db_row(row: &DbRow<'_>, index: usize) -> Result<Self, Error> {
        let s: Option<String> = FromDbValue::from_db_row(row, index)?;
        s.as_deref().map(AgentId::parse).transpose()
    }
}
impl FromDbValue for ChannelId {
    fn from_db_row(row: &DbRow<'_>, index: usize) -> Result<Self, Error> {
        let s: String = FromDbValue::from_db_row(row, index)?;
        ChannelId::parse(&s)
    }
}
impl FromDbValue for Option<ChannelId> {
    fn from_db_row(row: &DbRow<'_>, index: usize) -> Result<Self, Error> {
        let s: Option<String> = FromDbValue::from_db_row(row, index)?;
        s.as_deref().map(ChannelId::parse).transpose()
    }
}
impl FromDbValue for MessageId {
    fn from_db_row(row: &DbRow<'_>, index: usize) -> Result<Self, Error> {
        let s: String = FromDbValue::from_db_row(row, index)?;
        MessageId::parse(&s)
    }
}
impl FromDbValue for Option<MessageId> {
    fn from_db_row(row: &DbRow<'_>, index: usize) -> Result<Self, Error> {
        let s: Option<String> = FromDbValue::from_db_row(row, index)?;
        s.as_deref().map(MessageId::parse).transpose()
    }
}
impl FromDbValue for UsageId {
    fn from_db_row(row: &DbRow<'_>, index: usize) -> Result<Self, Error> {
        let s: String = FromDbValue::from_db_row(row, index)?;
        UsageId::parse(&s)
    }
}
impl FromDbValue for Option<UsageId> {
    fn from_db_row(row: &DbRow<'_>, index: usize) -> Result<Self, Error> {
        let s: Option<String> = FromDbValue::from_db_row(row, index)?;
        s.as_deref().map(UsageId::parse).transpose()
    }
}

/// Unified SQL execution surface implemented across both SQLite connections/
/// transactions and PostgreSQL pools/transactions.
pub(crate) trait DbExec {
    fn backend(&self) -> DbBackend;

    fn execute_raw(&self, sql: &str, params: &[DbParam]) -> Result<usize, Error>;

    fn execute_batch_raw(&self, sql: &str) -> Result<(), Error>;

    /// Execute `sql` with `params` and pass each row to `f`. Returning
    /// `Ok(false)` stops iteration early cleanly.
    fn for_each_row_raw(
        &self,
        sql: &str,
        params: &[DbParam],
        f: &mut dyn FnMut(&DbRow<'_>) -> Result<bool, Error>,
    ) -> Result<(), Error>;

    /// Acquire a per-agent serialization lock in a multi-writer PostgreSQL
    /// transaction before allocating sequential `seq` values. No-op on SQLite
    /// where `BEGIN IMMEDIATE` already serializes writers.
    fn lock_agent_for_seq(&self, _agent_id: &str) -> Result<(), Error> {
        Ok(())
    }
}

/// Ergonomic extension helpers with generic closures (since trait objects don't
/// allow generic methods directly). Names are prefixed with `db_` so they never
/// collide with inherent methods on `rusqlite::Connection`.
pub(crate) trait DbExecExt: DbExec {
    fn db_execute(&self, sql: &str, params: &[DbParam]) -> Result<usize, Error> {
        self.execute_raw(sql, params)
    }

    fn db_execute_batch(&self, sql: &str) -> Result<(), Error> {
        self.execute_batch_raw(sql)
    }

    fn db_query_one<T>(
        &self,
        sql: &str,
        params: &[DbParam],
        mut f: impl FnMut(&DbRow<'_>) -> Result<T, Error>,
    ) -> Result<T, Error> {
        self.db_query_opt(sql, params, &mut f)?
            .ok_or(Error::Db(rusqlite::Error::QueryReturnedNoRows))
    }

    fn db_query_opt<T>(
        &self,
        sql: &str,
        params: &[DbParam],
        mut f: impl FnMut(&DbRow<'_>) -> Result<T, Error>,
    ) -> Result<Option<T>, Error> {
        let mut out = None;
        self.for_each_row_raw(sql, params, &mut |row| {
            out = Some(f(row)?);
            Ok(false)
        })?;
        Ok(out)
    }

    fn db_query_vec<T>(
        &self,
        sql: &str,
        params: &[DbParam],
        mut f: impl FnMut(&DbRow<'_>) -> Result<T, Error>,
    ) -> Result<Vec<T>, Error> {
        let mut out = Vec::new();
        self.for_each_row_raw(sql, params, &mut |row| {
            out.push(f(row)?);
            Ok(true)
        })?;
        Ok(out)
    }
}

impl<D: DbExec + ?Sized> DbExecExt for D {}

fn sqlite_for_each_row(
    conn: &Connection,
    sql: &str,
    params: &[DbParam],
    f: &mut dyn FnMut(&DbRow<'_>) -> Result<bool, Error>,
) -> Result<(), Error> {
    let mut stmt = conn.prepare_cached(sql)?;
    let refs: Vec<&dyn rusqlite::ToSql> =
        params.iter().map(|p| p as &dyn rusqlite::ToSql).collect();
    let mut rows = stmt.query(rusqlite::params_from_iter(refs))?;
    while let Some(row) = rows.next()? {
        if !f(&DbRow::Sqlite(row))? {
            break;
        }
    }
    Ok(())
}

fn sqlite_execute(conn: &Connection, sql: &str, params: &[DbParam]) -> Result<usize, Error> {
    let mut stmt = conn.prepare_cached(sql)?;
    let refs: Vec<&dyn rusqlite::ToSql> =
        params.iter().map(|p| p as &dyn rusqlite::ToSql).collect();
    Ok(stmt.execute(rusqlite::params_from_iter(refs))?)
}

impl DbExec for Connection {
    fn backend(&self) -> DbBackend {
        DbBackend::Sqlite
    }

    fn execute_raw(&self, sql: &str, params: &[DbParam]) -> Result<usize, Error> {
        sqlite_execute(self, sql, params)
    }

    fn execute_batch_raw(&self, sql: &str) -> Result<(), Error> {
        Ok(self.execute_batch(sql)?)
    }

    fn for_each_row_raw(
        &self,
        sql: &str,
        params: &[DbParam],
        f: &mut dyn FnMut(&DbRow<'_>) -> Result<bool, Error>,
    ) -> Result<(), Error> {
        sqlite_for_each_row(self, sql, params, f)
    }
}

impl DbExec for PgPool {
    fn backend(&self) -> DbBackend {
        DbBackend::Postgres
    }

    fn execute_raw(&self, sql: &str, params: &[DbParam]) -> Result<usize, Error> {
        let pg_sql = to_pg_sql(sql);
        let args = bind_pg_params(params)?;
        let res = block_on_pg(sqlx::query_with(pg_sql.as_ref(), args).execute(self))?;
        Ok(usize::try_from(res.rows_affected()).unwrap_or(usize::MAX))
    }

    fn execute_batch_raw(&self, sql: &str) -> Result<(), Error> {
        let pg_sql = to_pg_sql(sql);
        block_on_pg(sqlx::raw_sql(pg_sql.as_ref()).execute(self))?;
        Ok(())
    }

    fn for_each_row_raw(
        &self,
        sql: &str,
        params: &[DbParam],
        f: &mut dyn FnMut(&DbRow<'_>) -> Result<bool, Error>,
    ) -> Result<(), Error> {
        let pg_sql = to_pg_sql(sql);
        let args = bind_pg_params(params)?;
        let rows = block_on_pg(sqlx::query_with(pg_sql.as_ref(), args).fetch_all(self))?;
        for row in &rows {
            if !f(&DbRow::Postgres(row))? {
                break;
            }
        }
        Ok(())
    }
}

/// Active PostgreSQL transaction held across a synchronous `write_tx` or
/// read snapshot (`read_tx`) block.
pub(crate) struct PgTx {
    tx: std::sync::Mutex<Option<Transaction<'static, Postgres>>>,
    wrote: AtomicBool,
}

impl PgTx {
    pub(crate) fn begin(pool: &PgPool) -> Result<Self, Error> {
        let tx = block_on_pg(pool.begin())?;
        Ok(Self {
            tx: std::sync::Mutex::new(Some(tx)),
            wrote: AtomicBool::new(false),
        })
    }

    pub(crate) fn begin_read_only(pool: &PgPool) -> Result<Self, Error> {
        let tx = block_on_pg(async {
            let mut tx = pool.begin().await?;
            sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                .execute(&mut *tx)
                .await?;
            Ok::<_, sqlx::Error>(tx)
        })?;
        Ok(Self {
            tx: std::sync::Mutex::new(Some(tx)),
            wrote: AtomicBool::new(false),
        })
    }

    pub(crate) fn wrote(&self) -> bool {
        self.wrote.load(Ordering::Relaxed)
    }

    pub(crate) fn commit(self) -> Result<(), Error> {
        let tx = self
            .tx
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .ok_or_else(|| Error::Message("postgres transaction already closed".into()))?;
        block_on_pg(tx.commit())?;
        Ok(())
    }
}

impl Drop for PgTx {
    fn drop(&mut self) {
        if let Some(tx) = self.tx.lock().unwrap_or_else(|e| e.into_inner()).take() {
            let _ = block_on_pg(tx.rollback());
        }
    }
}

impl DbExec for PgTx {
    fn backend(&self) -> DbBackend {
        DbBackend::Postgres
    }

    fn execute_raw(&self, sql: &str, params: &[DbParam]) -> Result<usize, Error> {
        let pg_sql = to_pg_sql(sql);
        let args = bind_pg_params(params)?;
        let mut guard = self.tx.lock().unwrap_or_else(|e| e.into_inner());
        let tx = guard
            .as_mut()
            .ok_or_else(|| Error::Message("postgres transaction already closed".into()))?;
        let res = block_on_pg(sqlx::query_with(pg_sql.as_ref(), args).execute(&mut **tx))?;
        let count = usize::try_from(res.rows_affected()).unwrap_or(usize::MAX);
        if count > 0 {
            self.wrote.store(true, Ordering::Relaxed);
        }
        Ok(count)
    }

    fn execute_batch_raw(&self, sql: &str) -> Result<(), Error> {
        let pg_sql = to_pg_sql(sql);
        let mut guard = self.tx.lock().unwrap_or_else(|e| e.into_inner());
        let tx = guard
            .as_mut()
            .ok_or_else(|| Error::Message("postgres transaction already closed".into()))?;
        block_on_pg(sqlx::raw_sql(pg_sql.as_ref()).execute(&mut **tx))?;
        self.wrote.store(true, Ordering::Relaxed);
        Ok(())
    }

    fn for_each_row_raw(
        &self,
        sql: &str,
        params: &[DbParam],
        f: &mut dyn FnMut(&DbRow<'_>) -> Result<bool, Error>,
    ) -> Result<(), Error> {
        let pg_sql = to_pg_sql(sql);
        let args = bind_pg_params(params)?;
        let rows = {
            let mut guard = self.tx.lock().unwrap_or_else(|e| e.into_inner());
            let tx = guard
                .as_mut()
                .ok_or_else(|| Error::Message("postgres transaction already closed".into()))?;
            block_on_pg(sqlx::query_with(pg_sql.as_ref(), args).fetch_all(&mut **tx))?
        };
        for row in &rows {
            if !f(&DbRow::Postgres(row))? {
                break;
            }
        }
        Ok(())
    }

    fn lock_agent_for_seq(&self, agent_id: &str) -> Result<(), Error> {
        let _: Option<i64> = self.db_query_opt(
            "SELECT 1 FROM agents WHERE id = ?1 FOR UPDATE",
            &[agent_id.to_db_param()],
            |r| r.get(0),
        )?;
        Ok(())
    }
}

/// Borrowed transaction handle passed into `Store::write_tx` and read-snapshot
/// helpers.
pub(crate) enum DbTx<'a> {
    Sqlite(&'a rusqlite::Transaction<'a>),
    Postgres(PgTx),
}

impl DbExec for DbTx<'_> {
    fn backend(&self) -> DbBackend {
        match self {
            Self::Sqlite(_) => DbBackend::Sqlite,
            Self::Postgres(_) => DbBackend::Postgres,
        }
    }

    fn execute_raw(&self, sql: &str, params: &[DbParam]) -> Result<usize, Error> {
        match self {
            Self::Sqlite(tx) => sqlite_execute(tx, sql, params),
            Self::Postgres(tx) => tx.execute_raw(sql, params),
        }
    }

    fn execute_batch_raw(&self, sql: &str) -> Result<(), Error> {
        match self {
            Self::Sqlite(tx) => Ok(tx.execute_batch(sql)?),
            Self::Postgres(tx) => tx.execute_batch_raw(sql),
        }
    }

    fn for_each_row_raw(
        &self,
        sql: &str,
        params: &[DbParam],
        f: &mut dyn FnMut(&DbRow<'_>) -> Result<bool, Error>,
    ) -> Result<(), Error> {
        match self {
            Self::Sqlite(tx) => sqlite_for_each_row(tx, sql, params, f),
            Self::Postgres(tx) => tx.for_each_row_raw(sql, params, f),
        }
    }

    fn lock_agent_for_seq(&self, agent_id: &str) -> Result<(), Error> {
        match self {
            Self::Sqlite(_) => Ok(()),
            Self::Postgres(tx) => tx.lock_agent_for_seq(agent_id),
        }
    }
}

impl DbExec for rusqlite::Transaction<'_> {
    fn backend(&self) -> DbBackend {
        DbBackend::Sqlite
    }

    fn execute_raw(&self, sql: &str, params: &[DbParam]) -> Result<usize, Error> {
        sqlite_execute(self, sql, params)
    }

    fn execute_batch_raw(&self, sql: &str) -> Result<(), Error> {
        Ok(self.execute_batch(sql)?)
    }

    fn for_each_row_raw(
        &self,
        sql: &str,
        params: &[DbParam],
        f: &mut dyn FnMut(&DbRow<'_>) -> Result<bool, Error>,
    ) -> Result<(), Error> {
        sqlite_for_each_row(self, sql, params, f)
    }
}

/// Consistent read snapshot across both backends (`unchecked_transaction()` on
/// SQLite, `REPEATABLE READ READ ONLY` transaction on PostgreSQL).
pub(crate) enum SnapshotGuard<'a> {
    Sqlite(rusqlite::Transaction<'a>),
    Postgres(PgTx),
}

impl SnapshotGuard<'_> {
    pub(crate) fn commit(self) -> Result<(), Error> {
        match self {
            Self::Sqlite(tx) => Ok(tx.commit()?),
            Self::Postgres(tx) => tx.commit(),
        }
    }
}

impl DbExec for SnapshotGuard<'_> {
    fn backend(&self) -> DbBackend {
        match self {
            Self::Sqlite(_) => DbBackend::Sqlite,
            Self::Postgres(_) => DbBackend::Postgres,
        }
    }

    fn execute_raw(&self, sql: &str, params: &[DbParam]) -> Result<usize, Error> {
        match self {
            Self::Sqlite(tx) => sqlite_execute(tx, sql, params),
            Self::Postgres(tx) => tx.execute_raw(sql, params),
        }
    }

    fn execute_batch_raw(&self, sql: &str) -> Result<(), Error> {
        match self {
            Self::Sqlite(tx) => Ok(tx.execute_batch(sql)?),
            Self::Postgres(tx) => tx.execute_batch_raw(sql),
        }
    }

    fn for_each_row_raw(
        &self,
        sql: &str,
        params: &[DbParam],
        f: &mut dyn FnMut(&DbRow<'_>) -> Result<bool, Error>,
    ) -> Result<(), Error> {
        match self {
            Self::Sqlite(tx) => sqlite_for_each_row(tx, sql, params, f),
            Self::Postgres(tx) => tx.for_each_row_raw(sql, params, f),
        }
    }
}

/// Format an FTS query for PostgreSQL `to_tsquery('simple', ...)` from
/// normalized search tokens (`tokenize_hybrid_search_tokens`), returning `None`
/// when no indexable tokens remain.
pub(crate) fn build_pg_tsquery(queries: &[String]) -> Option<String> {
    let mut parts: Vec<String> = Vec::new();
    for raw in queries {
        let mut and_terms: Vec<String> = Vec::new();
        for tok in crate::tokenize_hybrid_search_tokens(raw) {
            let escaped: String = tok
                .chars()
                .filter(|c| !matches!(c, '\'' | '\\' | '&' | '|' | '!' | '(' | ')' | ':' | '*'))
                .collect();
            if !escaped.is_empty() && !and_terms.contains(&escaped) {
                and_terms.push(escaped);
            }
        }
        if !and_terms.is_empty() {
            if and_terms.len() == 1 {
                parts.push(and_terms.into_iter().next().unwrap());
            } else {
                parts.push(format!("({})", and_terms.join(" & ")));
            }
        }
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join(" | "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder_translator_handles_numbered_and_anonymous_safely() {
        assert_eq!(
            to_pg_sql("SELECT id FROM agents WHERE id = ?1 AND parent_id = ?1 AND state != '?'"),
            "SELECT id FROM agents WHERE id = $1 AND parent_id = $1 AND state != '?'"
        );
        assert_eq!(
            to_pg_sql(
                "SELECT * FROM host_documents WHERE agent_id=? AND seq>=? -- comment ?\nAND \"col?\"=?"
            ),
            "SELECT * FROM host_index.host_documents WHERE agent_id=$1 AND seq>=$2 -- comment ?\nAND \"col?\"=$3"
        );
        assert_eq!(
            to_pg_sql("SELECT /* skipped ?1 */ $$?literal$$, ?2, ?"),
            "SELECT /* skipped ?1 */ $$?literal$$, $2, $3"
        );
        assert_eq!(to_pg_sql("SELECT '中文?1' || ?1"), "SELECT '中文?1' || $1");
    }

    #[test]
    fn to_pg_sql_qualifies_memory_and_host_index_tables_without_search_path() {
        assert_eq!(
            to_pg_sql("SELECT value FROM index_meta WHERE key = 'layout'"),
            "SELECT value FROM memory_index.index_meta WHERE key = 'layout'"
        );
        assert_eq!(
            to_pg_sql("SELECT EXISTS(SELECT 1 FROM index_pending)"),
            "SELECT EXISTS(SELECT 1 FROM memory_index.index_pending)"
        );
        assert_eq!(
            to_pg_sql(
                "INSERT INTO index_pending (agent_id, cap_messages) VALUES (?1, ?2) ON CONFLICT (agent_id) DO UPDATE SET cap_messages = COALESCE(index_pending.cap_messages, excluded.cap_messages)"
            ),
            "INSERT INTO memory_index.index_pending (agent_id, cap_messages) VALUES ($1, $2) ON CONFLICT (agent_id) DO UPDATE SET cap_messages = COALESCE(index_pending.cap_messages, excluded.cap_messages)"
        );
        assert_eq!(
            to_pg_sql(
                "SELECT count(*) FROM memory_index.rag_chunks WHERE 'index_meta' != 'host_windows'"
            ),
            "SELECT count(*) FROM memory_index.rag_chunks WHERE 'index_meta' != 'host_windows'"
        );
        assert_eq!(
            to_pg_sql("SELECT id FROM host_windows JOIN host_calls ON host_calls.id = ?1"),
            "SELECT id FROM host_index.host_windows JOIN host_index.host_calls ON host_calls.id = $1"
        );
    }

    #[test]
    fn database_url_redacts_password_only() {
        assert_eq!(
            redact_database_url(
                "postgres://kyle:supersecret@db.sylphx.net:5432/anyagents?sslmode=require"
            ),
            "postgres://kyle:***@db.sylphx.net:5432/anyagents?sslmode=require"
        );
        assert_eq!(
            redact_database_url("postgresql://localhost:5432/anyagents"),
            "postgresql://localhost:5432/anyagents"
        );
    }

    #[test]
    fn pg_tsquery_builds_or_and_expression_without_syntax_chars() {
        let q = build_pg_tsquery(&["foo:bar (test)".into(), "繁體中文!".into()]).unwrap();
        assert!(q.contains('|'));
        assert!(!q.contains('\''));
        assert!(!q.contains(':'));
    }
}
