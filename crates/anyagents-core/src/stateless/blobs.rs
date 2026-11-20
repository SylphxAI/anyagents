//! Unified state blob storage (`agent_files` and `deployment_blobs`).
//!
//! - [`PgStateBlobStore`] stores per-agent files and deployment blobs in
//!   PostgreSQL (`public.agent_files` / `public.deployment_blobs`) with optional
//!   AEAD encryption via [`MasterKey`].
//! - [`FsStateBlobStore`] maps the same namespaces onto the classic single-host
//!   `<data_dir>` filesystem layout so zero-config SQLite mode stays 100% intact.

use std::fs;
use std::path::{Path, PathBuf};

use sqlx::PgPool;

use super::crypto::MasterKey;
use crate::db::block_on_pg;
use crate::error::Error;
use crate::id::AgentId;
use crate::learned_memory::sha256;
use crate::migrate::now_ms;
use crate::secret::{CredentialStore, Secret};

pub const AGENT_FILE_NAMESPACES: &[&str] = &[
    "config",
    "memory",
    "memory_archive",
    "session_pin",
    "sessions",
    "skills",
    "spiron_cloud",
    "acp",
    "markers",
    "secrets",
];

pub const DEPLOYMENT_BLOB_NAMESPACES: &[&str] =
    &["config", "secrets", "profiles", "skills", "media", "roles"];

/// Full blob entry including content bytes (decrypted when `encrypted == true`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobEntry {
    pub namespace: String,
    pub path: String,
    pub content: Vec<u8>,
    pub sha256: String,
    pub encrypted: bool,
    pub version: i64,
    pub updated_at_ms: i64,
}

/// Lightweight blob metadata returned by directory/namespace listings.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobMeta {
    pub namespace: String,
    pub path: String,
    pub sha256: String,
    pub encrypted: bool,
    pub version: i64,
    pub updated_at_ms: i64,
    pub size_bytes: usize,
}

/// Validate a relative slash-separated blob path (no traversal, no NUL, no leading `/`).
pub fn validate_blob_path(path: &str) -> Result<String, Error> {
    let trimmed = path.trim();
    if trimmed.is_empty()
        || trimmed.starts_with('/')
        || trimmed.ends_with('/')
        || trimmed.contains('\0')
        || trimmed.contains('\\')
    {
        return Err(Error::Message(format!("invalid blob path `{path}`")));
    }
    for segment in trimmed.split('/') {
        if segment.is_empty() || segment == "." || segment == ".." {
            return Err(Error::Message(format!(
                "invalid blob path segment in `{path}`"
            )));
        }
    }
    Ok(trimmed.to_string())
}

fn validate_agent_namespace(namespace: &str) -> Result<&'static str, Error> {
    AGENT_FILE_NAMESPACES
        .iter()
        .copied()
        .find(|ns| *ns == namespace)
        .ok_or_else(|| Error::Message(format!("unsupported agent_files namespace `{namespace}`")))
}

fn validate_deployment_namespace(namespace: &str) -> Result<&'static str, Error> {
    DEPLOYMENT_BLOB_NAMESPACES
        .iter()
        .copied()
        .find(|ns| *ns == namespace)
        .ok_or_else(|| {
            Error::Message(format!(
                "unsupported deployment_blobs namespace `{namespace}`"
            ))
        })
}

fn agent_aad(agent_id: &AgentId, namespace: &str, path: &str) -> Vec<u8> {
    format!("agent_files:{}:{namespace}:{path}", agent_id.as_str()).into_bytes()
}

fn deployment_aad(namespace: &str, path: &str) -> Vec<u8> {
    format!("deployment_blobs:{namespace}:{path}").into_bytes()
}

/// Storage abstraction for per-agent files and deployment-wide blobs.
pub trait StateBlobStore: Send + Sync {
    fn get_agent_file(
        &self,
        agent_id: &AgentId,
        namespace: &str,
        path: &str,
    ) -> Result<Option<BlobEntry>, Error>;

    fn put_agent_file(
        &self,
        agent_id: &AgentId,
        namespace: &str,
        path: &str,
        content: &[u8],
        encrypted: bool,
    ) -> Result<BlobMeta, Error>;

    fn delete_agent_file(
        &self,
        agent_id: &AgentId,
        namespace: &str,
        path: &str,
    ) -> Result<bool, Error>;

    fn list_agent_files(&self, agent_id: &AgentId, namespace: &str)
    -> Result<Vec<BlobMeta>, Error>;

    fn get_deployment_blob(&self, namespace: &str, path: &str) -> Result<Option<BlobEntry>, Error>;

    fn put_deployment_blob(
        &self,
        namespace: &str,
        path: &str,
        content: &[u8],
        encrypted: bool,
    ) -> Result<BlobMeta, Error>;

    fn delete_deployment_blob(&self, namespace: &str, path: &str) -> Result<bool, Error>;

    fn list_deployment_blobs(&self, namespace: &str) -> Result<Vec<BlobMeta>, Error>;

    /// Batch-load all files in an agent's namespace in a single round-trip when
    /// backed by PostgreSQL (falls back to `list_agent_files` + `get_agent_file`).
    fn get_agent_files_all(
        &self,
        agent_id: &AgentId,
        namespace: &str,
    ) -> Result<Vec<BlobEntry>, Error> {
        let metas = self.list_agent_files(agent_id, namespace)?;
        let mut out = Vec::with_capacity(metas.len());
        for meta in metas {
            if let Some(entry) = self.get_agent_file(agent_id, namespace, &meta.path)? {
                out.push(entry);
            }
        }
        Ok(out)
    }

    /// Batch-load all deployment blobs in a namespace in a single round-trip when
    /// backed by PostgreSQL (falls back to `list_deployment_blobs` + `get_deployment_blob`).
    fn get_deployment_blobs_all(&self, namespace: &str) -> Result<Vec<BlobEntry>, Error> {
        let metas = self.list_deployment_blobs(namespace)?;
        let mut out = Vec::with_capacity(metas.len());
        for meta in metas {
            if let Some(entry) = self.get_deployment_blob(namespace, &meta.path)? {
                out.push(entry);
            }
        }
        Ok(out)
    }
}

/// PostgreSQL implementation backed by `public.agent_files` and `public.deployment_blobs`.
#[derive(Clone)]
pub struct PgStateBlobStore {
    pool: PgPool,
    master_key: Option<MasterKey>,
}

impl PgStateBlobStore {
    pub fn new(pool: PgPool, master_key: Option<MasterKey>) -> Self {
        Self { pool, master_key }
    }

    fn require_key(&self) -> Result<MasterKey, Error> {
        if let Some(ref key) = self.master_key {
            return Ok(key.clone());
        }
        MasterKey::require_from_env()
    }
}

impl StateBlobStore for PgStateBlobStore {
    fn get_agent_file(
        &self,
        agent_id: &AgentId,
        namespace: &str,
        path: &str,
    ) -> Result<Option<BlobEntry>, Error> {
        let ns = validate_agent_namespace(namespace)?;
        let clean = validate_blob_path(path)?;
        let pool = self.pool.clone();
        let aid = agent_id.as_str().to_string();
        let clean_q = clean.clone();
        let row: Option<(Vec<u8>, String, bool, i64, i64)> = block_on_pg(async move {
            sqlx::query_as(
                "SELECT content, sha256, encrypted, version, updated_at_ms
                 FROM public.agent_files
                 WHERE agent_id = $1 AND namespace = $2 AND path = $3",
            )
            .bind(&aid)
            .bind(ns)
            .bind(&clean_q)
            .fetch_optional(&pool)
            .await
            .map_err(Error::from)
        })?;
        let Some((raw_content, digest, encrypted, version, updated_at_ms)) = row else {
            return Ok(None);
        };
        let content = if encrypted {
            let key = self.require_key()?;
            key.decrypt(&agent_aad(agent_id, ns, &clean), &raw_content)?
        } else {
            raw_content
        };
        Ok(Some(BlobEntry {
            namespace: ns.to_string(),
            path: clean,
            content,
            sha256: digest,
            encrypted,
            version,
            updated_at_ms,
        }))
    }

    fn put_agent_file(
        &self,
        agent_id: &AgentId,
        namespace: &str,
        path: &str,
        content: &[u8],
        encrypted: bool,
    ) -> Result<BlobMeta, Error> {
        let ns = validate_agent_namespace(namespace)?;
        let clean = validate_blob_path(path)?;
        let digest = sha256(content);
        let stored_bytes = if encrypted {
            let key = self.require_key()?;
            key.encrypt(&agent_aad(agent_id, ns, &clean), content)?
        } else {
            content.to_vec()
        };
        let now = now_ms();
        let pool = self.pool.clone();
        let aid = agent_id.as_str().to_string();
        let path_c = clean.clone();
        let digest_c = digest.clone();
        let (version, updated_at_ms): (i64, i64) = block_on_pg(async move {
            sqlx::query_as(
                "INSERT INTO public.agent_files
                    (agent_id, namespace, path, content, sha256, encrypted, version, updated_at_ms)
                 VALUES ($1, $2, $3, $4, $5, $6, 1, $7)
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
                     updated_at_ms = EXCLUDED.updated_at_ms
                 RETURNING version, updated_at_ms",
            )
            .bind(&aid)
            .bind(ns)
            .bind(&path_c)
            .bind(&stored_bytes)
            .bind(&digest_c)
            .bind(encrypted)
            .bind(now)
            .fetch_one(&pool)
            .await
            .map_err(Error::from)
        })?;
        Ok(BlobMeta {
            namespace: ns.to_string(),
            path: clean,
            sha256: digest,
            encrypted,
            version,
            updated_at_ms,
            size_bytes: content.len(),
        })
    }

    fn delete_agent_file(
        &self,
        agent_id: &AgentId,
        namespace: &str,
        path: &str,
    ) -> Result<bool, Error> {
        let ns = validate_agent_namespace(namespace)?;
        let clean = validate_blob_path(path)?;
        let pool = self.pool.clone();
        let aid = agent_id.as_str().to_string();
        let affected = block_on_pg(async move {
            sqlx::query(
                "DELETE FROM public.agent_files
                 WHERE agent_id = $1 AND namespace = $2 AND path = $3",
            )
            .bind(&aid)
            .bind(ns)
            .bind(&clean)
            .execute(&pool)
            .await
            .map(|r| r.rows_affected())
            .map_err(Error::from)
        })?;
        Ok(affected > 0)
    }

    fn list_agent_files(
        &self,
        agent_id: &AgentId,
        namespace: &str,
    ) -> Result<Vec<BlobMeta>, Error> {
        let ns = validate_agent_namespace(namespace)?;
        let pool = self.pool.clone();
        let aid = agent_id.as_str().to_string();
        let rows: Vec<(String, String, bool, i64, i64, i64)> = block_on_pg(async move {
            sqlx::query_as(
                "SELECT path, sha256, encrypted, version, updated_at_ms, octet_length(content)::bigint
                 FROM public.agent_files
                 WHERE agent_id = $1 AND namespace = $2
                 ORDER BY path ASC",
            )
            .bind(&aid)
            .bind(ns)
            .fetch_all(&pool)
            .await
            .map_err(Error::from)
        })?;
        Ok(rows
            .into_iter()
            .map(
                |(path, digest, encrypted, version, updated_at_ms, size)| BlobMeta {
                    namespace: ns.to_string(),
                    path,
                    sha256: digest,
                    encrypted,
                    version,
                    updated_at_ms,
                    size_bytes: size.max(0) as usize,
                },
            )
            .collect())
    }

    fn get_deployment_blob(&self, namespace: &str, path: &str) -> Result<Option<BlobEntry>, Error> {
        let ns = validate_deployment_namespace(namespace)?;
        let clean = validate_blob_path(path)?;
        let pool = self.pool.clone();
        let clean_q = clean.clone();
        let row: Option<(Vec<u8>, String, bool, i64, i64)> = block_on_pg(async move {
            sqlx::query_as(
                "SELECT content, sha256, encrypted, version, updated_at_ms
                 FROM public.deployment_blobs
                 WHERE namespace = $1 AND path = $2",
            )
            .bind(ns)
            .bind(&clean_q)
            .fetch_optional(&pool)
            .await
            .map_err(Error::from)
        })?;
        let Some((raw_content, digest, encrypted, version, updated_at_ms)) = row else {
            return Ok(None);
        };
        let content = if encrypted {
            let key = self.require_key()?;
            key.decrypt(&deployment_aad(ns, &clean), &raw_content)?
        } else {
            raw_content
        };
        Ok(Some(BlobEntry {
            namespace: ns.to_string(),
            path: clean,
            content,
            sha256: digest,
            encrypted,
            version,
            updated_at_ms,
        }))
    }

    fn put_deployment_blob(
        &self,
        namespace: &str,
        path: &str,
        content: &[u8],
        encrypted: bool,
    ) -> Result<BlobMeta, Error> {
        let ns = validate_deployment_namespace(namespace)?;
        let clean = validate_blob_path(path)?;
        let digest = sha256(content);
        let stored_bytes = if encrypted {
            let key = self.require_key()?;
            key.encrypt(&deployment_aad(ns, &clean), content)?
        } else {
            content.to_vec()
        };
        let now = now_ms();
        let pool = self.pool.clone();
        let path_c = clean.clone();
        let digest_c = digest.clone();
        const UPSERT_SQL: &str = "INSERT INTO public.deployment_blobs
                    (namespace, path, content, sha256, encrypted, version, updated_at_ms)
                 VALUES ($1, $2, $3, $4, $5, 1, $6)
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
                     updated_at_ms = EXCLUDED.updated_at_ms
                 RETURNING version, updated_at_ms";
        let (version, updated_at_ms): (i64, i64) = block_on_pg(async move {
            let attempt = sqlx::query_as::<_, (i64, i64)>(UPSERT_SQL)
                .bind(ns)
                .bind(&path_c)
                .bind(&stored_bytes)
                .bind(&digest_c)
                .bind(encrypted)
                .bind(now)
                .fetch_one(&pool)
                .await;
            match attempt {
                Ok(row) => Ok(row),
                Err(sqlx::Error::Database(ref db_err))
                    if ns == "roles" && db_err.code().as_deref() == Some("23514") =>
                {
                    let _ = sqlx::query(
                        "ALTER TABLE public.deployment_blobs
                         DROP CONSTRAINT IF EXISTS deployment_blobs_namespace_check",
                    )
                    .execute(&pool)
                    .await;
                    let _ = sqlx::query(
                        "ALTER TABLE public.deployment_blobs
                         ADD CONSTRAINT deployment_blobs_namespace_check
                         CHECK (namespace IN ('config', 'secrets', 'profiles', 'skills', 'media', 'roles'))",
                    )
                    .execute(&pool)
                    .await;
                    sqlx::query_as::<_, (i64, i64)>(UPSERT_SQL)
                        .bind(ns)
                        .bind(&path_c)
                        .bind(&stored_bytes)
                        .bind(&digest_c)
                        .bind(encrypted)
                        .bind(now)
                        .fetch_one(&pool)
                        .await
                        .map_err(Error::from)
                }
                Err(err) => Err(Error::from(err)),
            }
        })?;
        Ok(BlobMeta {
            namespace: ns.to_string(),
            path: clean,
            sha256: digest,
            encrypted,
            version,
            updated_at_ms,
            size_bytes: content.len(),
        })
    }

    fn delete_deployment_blob(&self, namespace: &str, path: &str) -> Result<bool, Error> {
        let ns = validate_deployment_namespace(namespace)?;
        let clean = validate_blob_path(path)?;
        let pool = self.pool.clone();
        let affected = block_on_pg(async move {
            sqlx::query(
                "DELETE FROM public.deployment_blobs
                 WHERE namespace = $1 AND path = $2",
            )
            .bind(ns)
            .bind(&clean)
            .execute(&pool)
            .await
            .map(|r| r.rows_affected())
            .map_err(Error::from)
        })?;
        Ok(affected > 0)
    }

    fn list_deployment_blobs(&self, namespace: &str) -> Result<Vec<BlobMeta>, Error> {
        let ns = validate_deployment_namespace(namespace)?;
        let pool = self.pool.clone();
        let rows: Vec<(String, String, bool, i64, i64, i64)> = block_on_pg(async move {
            sqlx::query_as(
                "SELECT path, sha256, encrypted, version, updated_at_ms, octet_length(content)::bigint
                 FROM public.deployment_blobs
                 WHERE namespace = $1
                 ORDER BY path ASC",
            )
            .bind(ns)
            .fetch_all(&pool)
            .await
            .map_err(Error::from)
        })?;
        Ok(rows
            .into_iter()
            .map(
                |(path, digest, encrypted, version, updated_at_ms, size)| BlobMeta {
                    namespace: ns.to_string(),
                    path,
                    sha256: digest,
                    encrypted,
                    version,
                    updated_at_ms,
                    size_bytes: size.max(0) as usize,
                },
            )
            .collect())
    }

    fn get_agent_files_all(
        &self,
        agent_id: &AgentId,
        namespace: &str,
    ) -> Result<Vec<BlobEntry>, Error> {
        let ns = validate_agent_namespace(namespace)?;
        let pool = self.pool.clone();
        let aid = agent_id.as_str().to_string();
        let rows: Vec<(String, Vec<u8>, String, bool, i64, i64)> = block_on_pg(async move {
            sqlx::query_as(
                "SELECT path, content, sha256, encrypted, version, updated_at_ms
                 FROM public.agent_files
                 WHERE agent_id = $1 AND namespace = $2
                 ORDER BY path ASC",
            )
            .bind(&aid)
            .bind(ns)
            .fetch_all(&pool)
            .await
            .map_err(Error::from)
        })?;
        let mut out = Vec::with_capacity(rows.len());
        for (path, raw_content, digest, encrypted, version, updated_at_ms) in rows {
            let Ok(clean) = validate_blob_path(&path) else {
                continue;
            };
            let content = if encrypted {
                let key = self.require_key()?;
                key.decrypt(&agent_aad(agent_id, ns, &clean), &raw_content)?
            } else {
                raw_content
            };
            out.push(BlobEntry {
                namespace: ns.to_string(),
                path: clean,
                content,
                sha256: digest,
                encrypted,
                version,
                updated_at_ms,
            });
        }
        Ok(out)
    }

    fn get_deployment_blobs_all(&self, namespace: &str) -> Result<Vec<BlobEntry>, Error> {
        let ns = validate_deployment_namespace(namespace)?;
        let pool = self.pool.clone();
        let rows: Vec<(String, Vec<u8>, String, bool, i64, i64)> = block_on_pg(async move {
            sqlx::query_as(
                "SELECT path, content, sha256, encrypted, version, updated_at_ms
                 FROM public.deployment_blobs
                 WHERE namespace = $1
                 ORDER BY path ASC",
            )
            .bind(ns)
            .fetch_all(&pool)
            .await
            .map_err(Error::from)
        })?;
        let mut out = Vec::with_capacity(rows.len());
        for (path, raw_content, digest, encrypted, version, updated_at_ms) in rows {
            let Ok(clean) = validate_blob_path(&path) else {
                continue;
            };
            let content = if encrypted {
                let key = self.require_key()?;
                key.decrypt(&deployment_aad(ns, &clean), &raw_content)?
            } else {
                raw_content
            };
            out.push(BlobEntry {
                namespace: ns.to_string(),
                path: clean,
                content,
                sha256: digest,
                encrypted,
                version,
                updated_at_ms,
            });
        }
        Ok(out)
    }
}

/// Local filesystem implementation for zero-config SQLite mode.
#[derive(Clone, Debug)]
pub struct FsStateBlobStore {
    data_dir: PathBuf,
}

impl FsStateBlobStore {
    pub fn new(data_dir: impl AsRef<Path>) -> Self {
        Self {
            data_dir: data_dir.as_ref().to_path_buf(),
        }
    }

    fn agent_ns_root(&self, agent_id: &AgentId, namespace: &str) -> Result<PathBuf, Error> {
        let ns = validate_agent_namespace(namespace)?;
        let root = self.data_dir.join("agents").join(agent_id.as_str());
        let dir = match ns {
            "memory" => root.join("memory"),
            "memory_archive" => root.join("history").join("archive").join("memory"),
            "session_pin" | "sessions" => root.join("sessions"),
            "skills" => root.join("skills"),
            "config" | "spiron_cloud" | "acp" | "markers" | "secrets" => root.join("config"),
            _ => unreachable!(),
        };
        Ok(dir)
    }

    fn deployment_ns_root(&self, namespace: &str) -> Result<PathBuf, Error> {
        let ns = validate_deployment_namespace(namespace)?;
        let dir = match ns {
            "config" => self.data_dir.clone(),
            "secrets" => self.data_dir.join("secrets"),
            "profiles" => self.data_dir.join("profiles"),
            "skills" => self.data_dir.join("skills"),
            "media" => self.data_dir.join("media"),
            "roles" => self.data_dir.join("roles"),
            _ => unreachable!(),
        };
        Ok(dir)
    }

    fn read_file(root: &Path, namespace: &str, clean: &str) -> Result<Option<BlobEntry>, Error> {
        let full = root.join(clean);
        let meta = match fs::symlink_metadata(&full) {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        if !meta.is_file() {
            return Ok(None);
        }
        let content = fs::read(&full)?;
        let updated_at_ms = meta
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let digest = sha256(&content);
        Ok(Some(BlobEntry {
            namespace: namespace.to_string(),
            path: clean.to_string(),
            content,
            sha256: digest,
            encrypted: false,
            version: 1,
            updated_at_ms,
        }))
    }

    fn write_file(
        root: &Path,
        namespace: &str,
        clean: &str,
        content: &[u8],
        encrypted: bool,
    ) -> Result<BlobMeta, Error> {
        let full = root.join(clean);
        let mode = if encrypted || namespace == "secrets" {
            0o600
        } else {
            0o644
        };
        anyagents_harness::atomic_write_file(&full, content, mode)?;
        Ok(BlobMeta {
            namespace: namespace.to_string(),
            path: clean.to_string(),
            sha256: sha256(content),
            encrypted: false,
            version: 1,
            updated_at_ms: now_ms(),
            size_bytes: content.len(),
        })
    }

    fn list_dir(root: &Path, namespace: &str) -> Result<Vec<BlobMeta>, Error> {
        let mut out = Vec::new();
        if !root.is_dir() {
            return Ok(out);
        }
        collect_regular_files(root, root, namespace, &mut out)?;
        out.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(out)
    }
}

fn collect_regular_files(
    base: &Path,
    dir: &Path,
    namespace: &str,
    out: &mut Vec<BlobMeta>,
) -> Result<(), Error> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        let meta = fs::symlink_metadata(&path)?;
        if meta.file_type().is_symlink() {
            continue;
        }
        if meta.is_dir() {
            collect_regular_files(base, &path, namespace, out)?;
        } else if meta.is_file() {
            let Ok(rel) = path.strip_prefix(base) else {
                continue;
            };
            let rel_str = rel.to_string_lossy().replace('\\', "/");
            let content = fs::read(&path)?;
            let updated_at_ms = meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            out.push(BlobMeta {
                namespace: namespace.to_string(),
                path: rel_str,
                sha256: sha256(&content),
                encrypted: false,
                version: 1,
                updated_at_ms,
                size_bytes: content.len(),
            });
        }
    }
    Ok(())
}

impl StateBlobStore for FsStateBlobStore {
    fn get_agent_file(
        &self,
        agent_id: &AgentId,
        namespace: &str,
        path: &str,
    ) -> Result<Option<BlobEntry>, Error> {
        let clean = validate_blob_path(path)?;
        let root = self.agent_ns_root(agent_id, namespace)?;
        Self::read_file(&root, namespace, &clean)
    }

    fn put_agent_file(
        &self,
        agent_id: &AgentId,
        namespace: &str,
        path: &str,
        content: &[u8],
        encrypted: bool,
    ) -> Result<BlobMeta, Error> {
        let clean = validate_blob_path(path)?;
        let root = self.agent_ns_root(agent_id, namespace)?;
        Self::write_file(&root, namespace, &clean, content, encrypted)
    }

    fn delete_agent_file(
        &self,
        agent_id: &AgentId,
        namespace: &str,
        path: &str,
    ) -> Result<bool, Error> {
        let clean = validate_blob_path(path)?;
        let full = self.agent_ns_root(agent_id, namespace)?.join(clean);
        match fs::remove_file(full) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    fn list_agent_files(
        &self,
        agent_id: &AgentId,
        namespace: &str,
    ) -> Result<Vec<BlobMeta>, Error> {
        let root = self.agent_ns_root(agent_id, namespace)?;
        Self::list_dir(&root, namespace)
    }

    fn get_deployment_blob(&self, namespace: &str, path: &str) -> Result<Option<BlobEntry>, Error> {
        let clean = validate_blob_path(path)?;
        let root = self.deployment_ns_root(namespace)?;
        Self::read_file(&root, namespace, &clean)
    }

    fn put_deployment_blob(
        &self,
        namespace: &str,
        path: &str,
        content: &[u8],
        encrypted: bool,
    ) -> Result<BlobMeta, Error> {
        let clean = validate_blob_path(path)?;
        let root = self.deployment_ns_root(namespace)?;
        Self::write_file(&root, namespace, &clean, content, encrypted)
    }

    fn delete_deployment_blob(&self, namespace: &str, path: &str) -> Result<bool, Error> {
        let clean = validate_blob_path(path)?;
        let full = self.deployment_ns_root(namespace)?.join(clean);
        match fs::remove_file(full) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    fn list_deployment_blobs(&self, namespace: &str) -> Result<Vec<BlobMeta>, Error> {
        let root = self.deployment_ns_root(namespace)?;
        Self::list_dir(&root, namespace)
    }
}

/// PostgreSQL [`CredentialStore`] storing encrypted channel/deployment secrets
/// in `public.deployment_blobs(namespace = 'secrets')`.
#[derive(Clone)]
pub struct PgCredentialStore {
    blobs: PgStateBlobStore,
}

impl PgCredentialStore {
    pub fn new(pool: PgPool, master_key: Option<MasterKey>) -> Self {
        Self {
            blobs: PgStateBlobStore::new(pool, master_key),
        }
    }
}

impl CredentialStore for PgCredentialStore {
    fn read(&self, credential_ref: &str) -> Result<Secret, Error> {
        let clean = validate_blob_path(credential_ref)?;
        let entry = self
            .blobs
            .get_deployment_blob("secrets", &clean)?
            .ok_or_else(|| {
                Error::Message(format!(
                    "credential `{clean}` not found in deployment_blobs"
                ))
            })?;
        Ok(Secret::from_bytes(entry.content))
    }

    fn write(&self, credential_ref: &str, value: &[u8]) -> Result<(), Error> {
        let clean = validate_blob_path(credential_ref)?;
        self.blobs
            .put_deployment_blob("secrets", &clean, value, true)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fs_blob_store_supports_roles_namespace_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsStateBlobStore::new(dir.path());
        let body = b"---\nname: reviewer\ndescription: Code reviewer\n---\nReview carefully.\n";
        let meta = store
            .put_deployment_blob("roles", "reviewer.md", body, false)
            .unwrap();
        assert_eq!(meta.namespace, "roles");
        assert_eq!(meta.path, "reviewer.md");
        assert!(dir.path().join("roles/reviewer.md").is_file());

        let loaded = store
            .get_deployment_blob("roles", "reviewer.md")
            .unwrap()
            .expect("role blob must exist");
        assert_eq!(loaded.content, body);

        let listed = store.list_deployment_blobs("roles").unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].path, "reviewer.md");

        assert!(
            store
                .delete_deployment_blob("roles", "reviewer.md")
                .unwrap()
        );
        assert!(
            store
                .get_deployment_blob("roles", "reviewer.md")
                .unwrap()
                .is_none()
        );
    }
}
