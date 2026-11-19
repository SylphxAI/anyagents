//! Durable per-agent sandbox binding storage (`public.agent_sandbox_bindings`).
//!
//! Replaces pod-local `config/sandbox-sylphx.json` / `config/sandbox.json` /
//! `.sylphx-bundle-hashes.json` in PostgreSQL multi-replica mode while keeping
//! local disk persistence intact for SQLite mode.

use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sqlx::PgPool;

use super::crypto::MasterKey;
use crate::db::block_on_pg;
use crate::error::Error;
use crate::id::AgentId;
use crate::migrate::now_ms;

/// Default logical workspace inside remote sandboxes (persistent volume subtree).
pub const DEFAULT_REMOTE_SANDBOX_WORKDIR: &str = "/home/user/agent-data/workspace";

const LOCAL_BINDING_FILE: &str = "sandbox-binding.json";

fn transport_token_aad(agent_id: &AgentId) -> Vec<u8> {
    format!(
        "agent_sandbox_bindings:transport_token:{}",
        agent_id.as_str()
    )
    .into_bytes()
}

/// Durable persistent-volume and ephemeral compute-lease binding for one agent.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SandboxBinding {
    pub agent_id: AgentId,
    pub provider: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template: Option<String>,
    #[serde(default = "default_remote_workdir")]
    pub remote_workdir: String,
    #[serde(default = "default_json_object")]
    pub spec_json: String,
    #[serde(default)]
    pub volume_id: String,
    #[serde(default)]
    pub volume_resource: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume_size_gib: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_resource: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_plane_resource: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guest_endpoint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transport_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transport_token: Option<String>,
    #[serde(default = "default_lease_state")]
    pub lease_state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_expires_at_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_since_ms: Option<i64>,
    #[serde(default = "default_idle_ttl_ms")]
    pub idle_ttl_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_heartbeat_ms: Option<i64>,
    #[serde(default = "default_json_object")]
    pub bundle_manifest_json: String,
    #[serde(default = "default_generation")]
    pub generation: i64,
    #[serde(default)]
    pub updated_at_ms: i64,
}

fn default_remote_workdir() -> String {
    DEFAULT_REMOTE_SANDBOX_WORKDIR.to_string()
}
fn default_json_object() -> String {
    "{}".to_string()
}
fn default_lease_state() -> String {
    "detached".to_string()
}
fn default_idle_ttl_ms() -> i64 {
    300_000
}
fn default_generation() -> i64 {
    1
}

impl fmt::Debug for SandboxBinding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SandboxBinding")
            .field("agent_id", &self.agent_id)
            .field("provider", &self.provider)
            .field("image", &self.image)
            .field("template", &self.template)
            .field("remote_workdir", &self.remote_workdir)
            .field("volume_id", &self.volume_id)
            .field("lease_id", &self.lease_id)
            .field("guest_endpoint", &self.guest_endpoint)
            .field("transport_url", &self.transport_url)
            .field(
                "transport_token",
                &self.transport_token.as_ref().map(|_| "[redacted]"),
            )
            .field("lease_state", &self.lease_state)
            .field("idle_since_ms", &self.idle_since_ms)
            .field("last_heartbeat_ms", &self.last_heartbeat_ms)
            .field("generation", &self.generation)
            .finish()
    }
}

impl SandboxBinding {
    pub fn new(agent_id: AgentId, provider: impl Into<String>) -> Self {
        Self {
            agent_id,
            provider: provider.into(),
            image: None,
            template: None,
            remote_workdir: default_remote_workdir(),
            spec_json: default_json_object(),
            volume_id: String::new(),
            volume_resource: String::new(),
            volume_size_gib: None,
            lease_id: None,
            lease_resource: None,
            data_plane_resource: None,
            guest_endpoint: None,
            transport_url: None,
            transport_token: None,
            lease_state: default_lease_state(),
            lease_expires_at_ms: None,
            idle_since_ms: None,
            idle_ttl_ms: default_idle_ttl_ms(),
            last_heartbeat_ms: None,
            bundle_manifest_json: default_json_object(),
            generation: 1,
            updated_at_ms: now_ms(),
        }
    }
}

/// Persistence abstraction for per-agent sandbox bindings.
pub trait SandboxBindingStore: Send + Sync {
    fn get(&self, agent_id: &AgentId) -> Result<Option<SandboxBinding>, Error>;
    fn upsert(&self, binding: &SandboxBinding) -> Result<SandboxBinding, Error>;
    fn update_lease_state(
        &self,
        agent_id: &AgentId,
        lease_state: &str,
        idle_since_ms: Option<i64>,
    ) -> Result<bool, Error>;
    fn clear_lease(&self, agent_id: &AgentId) -> Result<bool, Error>;
    fn touch_heartbeat(&self, agent_id: &AgentId, at_ms: i64) -> Result<bool, Error>;
    fn list_idle_due(&self, now_ms: i64) -> Result<Vec<SandboxBinding>, Error>;
    fn list_active_ready(&self) -> Result<Vec<SandboxBinding>, Error>;
    fn delete(&self, agent_id: &AgentId) -> Result<bool, Error>;
}

const VALID_LEASE_STATES: &[&str] = &[
    "detached",
    "provisioning",
    "ready",
    "releasing",
    "expired",
    "failed",
];

fn validate_lease_state(state: &str) -> Result<&'static str, Error> {
    VALID_LEASE_STATES
        .iter()
        .copied()
        .find(|s| *s == state)
        .ok_or_else(|| Error::Message(format!("invalid sandbox lease_state `{state}`")))
}

/// PostgreSQL implementation backed by `public.agent_sandbox_bindings`.
#[derive(Clone)]
pub struct PgSandboxBindingStore {
    pool: PgPool,
    master_key: Option<MasterKey>,
}

impl PgSandboxBindingStore {
    pub fn new(pool: PgPool, master_key: Option<MasterKey>) -> Self {
        Self { pool, master_key }
    }

    fn key_opt(&self) -> Result<Option<MasterKey>, Error> {
        if let Some(ref key) = self.master_key {
            return Ok(Some(key.clone()));
        }
        MasterKey::from_env()
    }

    fn require_key(&self) -> Result<MasterKey, Error> {
        self.key_opt()?.ok_or_else(|| {
            Error::Message(
                "encryption key required for sandbox transport_token: set ANYAGENTS_MASTER_KEY"
                    .into(),
            )
        })
    }

    fn decode_row(&self, row: sqlx::postgres::PgRow) -> Result<SandboxBinding, Error> {
        use sqlx::Row as _;
        let aid_str: String = row.try_get("agent_id").map_err(Error::from)?;
        let agent_id = AgentId::parse(&aid_str)?;
        let enc: Option<Vec<u8>> = row.try_get("transport_token_enc").map_err(Error::from)?;
        let transport_token = match enc {
            Some(bytes) if !bytes.is_empty() => {
                let key = self.require_key()?;
                let pt = key.decrypt(&transport_token_aad(&agent_id), &bytes)?;
                Some(String::from_utf8(pt).map_err(|_| {
                    Error::Message("decrypted sandbox transport_token is not utf-8".into())
                })?)
            }
            _ => None,
        };
        Ok(SandboxBinding {
            agent_id,
            provider: row.try_get("provider").map_err(Error::from)?,
            image: row.try_get("image").map_err(Error::from)?,
            template: row.try_get("template").map_err(Error::from)?,
            remote_workdir: row.try_get("remote_workdir").map_err(Error::from)?,
            spec_json: row.try_get("spec_json").map_err(Error::from)?,
            volume_id: row.try_get("volume_id").map_err(Error::from)?,
            volume_resource: row.try_get("volume_resource").map_err(Error::from)?,
            volume_size_gib: row.try_get("volume_size_gib").map_err(Error::from)?,
            lease_id: row.try_get("lease_id").map_err(Error::from)?,
            lease_resource: row.try_get("lease_resource").map_err(Error::from)?,
            data_plane_resource: row.try_get("data_plane_resource").map_err(Error::from)?,
            guest_endpoint: row.try_get("guest_endpoint").map_err(Error::from)?,
            transport_url: row.try_get("transport_url").map_err(Error::from)?,
            transport_token,
            lease_state: row.try_get("lease_state").map_err(Error::from)?,
            lease_expires_at_ms: row.try_get("lease_expires_at_ms").map_err(Error::from)?,
            idle_since_ms: row.try_get("idle_since_ms").map_err(Error::from)?,
            idle_ttl_ms: row.try_get("idle_ttl_ms").map_err(Error::from)?,
            last_heartbeat_ms: row.try_get("last_heartbeat_ms").map_err(Error::from)?,
            bundle_manifest_json: row.try_get("bundle_manifest_json").map_err(Error::from)?,
            generation: row.try_get("generation").map_err(Error::from)?,
            updated_at_ms: row.try_get("updated_at_ms").map_err(Error::from)?,
        })
    }
}

const SELECT_BINDING_COLS: &str = "agent_id, provider, image, template, remote_workdir, spec_json, \
    volume_id, volume_resource, volume_size_gib, lease_id, lease_resource, data_plane_resource, \
    guest_endpoint, transport_url, transport_token_enc, lease_state, lease_expires_at_ms, \
    idle_since_ms, idle_ttl_ms, last_heartbeat_ms, bundle_manifest_json, generation, updated_at_ms";

impl SandboxBindingStore for PgSandboxBindingStore {
    fn get(&self, agent_id: &AgentId) -> Result<Option<SandboxBinding>, Error> {
        let pool = self.pool.clone();
        let aid = agent_id.as_str().to_string();
        let sql = format!(
            "SELECT {SELECT_BINDING_COLS} FROM public.agent_sandbox_bindings WHERE agent_id = $1"
        );
        let row = block_on_pg(async move {
            sqlx::query(&sql)
                .bind(&aid)
                .fetch_optional(&pool)
                .await
                .map_err(Error::from)
        })?;
        row.map(|r| self.decode_row(r)).transpose()
    }

    fn upsert(&self, binding: &SandboxBinding) -> Result<SandboxBinding, Error> {
        let lease_state = validate_lease_state(&binding.lease_state)?;
        let token_enc: Option<Vec<u8>> = match binding.transport_token.as_deref() {
            Some(tok) if !tok.is_empty() => {
                let key = self.require_key()?;
                Some(key.encrypt(&transport_token_aad(&binding.agent_id), tok.as_bytes())?)
            }
            _ => None,
        };
        let now = now_ms();
        let remote_workdir = if binding.remote_workdir.trim().is_empty() {
            DEFAULT_REMOTE_SANDBOX_WORKDIR.to_string()
        } else {
            binding.remote_workdir.clone()
        };
        let pool = self.pool.clone();
        let b = binding.clone();
        let row = block_on_pg(async move {
            let sql = format!(
                "INSERT INTO public.agent_sandbox_bindings (
                    agent_id, provider, image, template, remote_workdir, spec_json,
                    volume_id, volume_resource, volume_size_gib,
                    lease_id, lease_resource, data_plane_resource,
                    guest_endpoint, transport_url, transport_token_enc,
                    lease_state, lease_expires_at_ms, idle_since_ms, idle_ttl_ms,
                    last_heartbeat_ms, bundle_manifest_json, generation, updated_at_ms
                 ) VALUES (
                    $1, $2, $3, $4, $5, $6,
                    $7, $8, $9,
                    $10, $11, $12,
                    $13, $14, $15,
                    $16, $17, $18, $19,
                    $20, $21, 1, $22
                 )
                 ON CONFLICT (agent_id) DO UPDATE
                 SET provider             = EXCLUDED.provider,
                     image                = EXCLUDED.image,
                     template             = EXCLUDED.template,
                     remote_workdir       = EXCLUDED.remote_workdir,
                     spec_json            = EXCLUDED.spec_json,
                     volume_id            = EXCLUDED.volume_id,
                     volume_resource      = EXCLUDED.volume_resource,
                     volume_size_gib      = EXCLUDED.volume_size_gib,
                     lease_id             = EXCLUDED.lease_id,
                     lease_resource       = EXCLUDED.lease_resource,
                     data_plane_resource  = EXCLUDED.data_plane_resource,
                     guest_endpoint       = EXCLUDED.guest_endpoint,
                     transport_url        = EXCLUDED.transport_url,
                     transport_token_enc  = EXCLUDED.transport_token_enc,
                     lease_state          = EXCLUDED.lease_state,
                     lease_expires_at_ms  = EXCLUDED.lease_expires_at_ms,
                     idle_since_ms        = EXCLUDED.idle_since_ms,
                     idle_ttl_ms          = EXCLUDED.idle_ttl_ms,
                     last_heartbeat_ms    = EXCLUDED.last_heartbeat_ms,
                     bundle_manifest_json = EXCLUDED.bundle_manifest_json,
                     generation           = public.agent_sandbox_bindings.generation + 1,
                     updated_at_ms        = EXCLUDED.updated_at_ms
                 RETURNING {SELECT_BINDING_COLS}"
            );
            sqlx::query(&sql)
                .bind(b.agent_id.as_str())
                .bind(&b.provider)
                .bind(&b.image)
                .bind(&b.template)
                .bind(&remote_workdir)
                .bind(&b.spec_json)
                .bind(&b.volume_id)
                .bind(&b.volume_resource)
                .bind(b.volume_size_gib)
                .bind(&b.lease_id)
                .bind(&b.lease_resource)
                .bind(&b.data_plane_resource)
                .bind(&b.guest_endpoint)
                .bind(&b.transport_url)
                .bind(&token_enc)
                .bind(lease_state)
                .bind(b.lease_expires_at_ms)
                .bind(b.idle_since_ms)
                .bind(b.idle_ttl_ms)
                .bind(b.last_heartbeat_ms)
                .bind(&b.bundle_manifest_json)
                .bind(now)
                .fetch_one(&pool)
                .await
                .map_err(Error::from)
        })?;
        self.decode_row(row)
    }

    fn update_lease_state(
        &self,
        agent_id: &AgentId,
        lease_state: &str,
        idle_since_ms: Option<i64>,
    ) -> Result<bool, Error> {
        let state = validate_lease_state(lease_state)?;
        let pool = self.pool.clone();
        let aid = agent_id.as_str().to_string();
        let now = now_ms();
        let affected = block_on_pg(async move {
            sqlx::query(
                "UPDATE public.agent_sandbox_bindings
                 SET lease_state = $2,
                     idle_since_ms = $3,
                     updated_at_ms = $4
                 WHERE agent_id = $1",
            )
            .bind(&aid)
            .bind(state)
            .bind(idle_since_ms)
            .bind(now)
            .execute(&pool)
            .await
            .map(|r| r.rows_affected())
            .map_err(Error::from)
        })?;
        Ok(affected > 0)
    }

    fn clear_lease(&self, agent_id: &AgentId) -> Result<bool, Error> {
        let pool = self.pool.clone();
        let aid = agent_id.as_str().to_string();
        let now = now_ms();
        let affected = block_on_pg(async move {
            sqlx::query(
                "UPDATE public.agent_sandbox_bindings
                 SET lease_id = NULL,
                     lease_resource = NULL,
                     data_plane_resource = NULL,
                     guest_endpoint = NULL,
                     transport_url = NULL,
                     transport_token_enc = NULL,
                     lease_state = 'detached',
                     lease_expires_at_ms = NULL,
                     idle_since_ms = NULL,
                     generation = generation + 1,
                     updated_at_ms = $2
                 WHERE agent_id = $1",
            )
            .bind(&aid)
            .bind(now)
            .execute(&pool)
            .await
            .map(|r| r.rows_affected())
            .map_err(Error::from)
        })?;
        Ok(affected > 0)
    }

    fn touch_heartbeat(&self, agent_id: &AgentId, at_ms: i64) -> Result<bool, Error> {
        let pool = self.pool.clone();
        let aid = agent_id.as_str().to_string();
        let affected = block_on_pg(async move {
            sqlx::query(
                "UPDATE public.agent_sandbox_bindings
                 SET last_heartbeat_ms = $2,
                     updated_at_ms = GREATEST(updated_at_ms, $2)
                 WHERE agent_id = $1",
            )
            .bind(&aid)
            .bind(at_ms)
            .execute(&pool)
            .await
            .map(|r| r.rows_affected())
            .map_err(Error::from)
        })?;
        Ok(affected > 0)
    }

    fn list_idle_due(&self, now: i64) -> Result<Vec<SandboxBinding>, Error> {
        let pool = self.pool.clone();
        let sql = format!(
            "SELECT {SELECT_BINDING_COLS}
             FROM public.agent_sandbox_bindings
             WHERE lease_state = 'ready'
               AND idle_since_ms IS NOT NULL
               AND (idle_since_ms + idle_ttl_ms) <= $1
             ORDER BY idle_since_ms ASC"
        );
        let rows = block_on_pg(async move {
            sqlx::query(&sql)
                .bind(now)
                .fetch_all(&pool)
                .await
                .map_err(Error::from)
        })?;
        rows.into_iter().map(|r| self.decode_row(r)).collect()
    }

    fn list_active_ready(&self) -> Result<Vec<SandboxBinding>, Error> {
        let pool = self.pool.clone();
        let sql = format!(
            "SELECT {SELECT_BINDING_COLS}
             FROM public.agent_sandbox_bindings
             WHERE lease_state = 'ready'
               AND lease_id IS NOT NULL
             ORDER BY updated_at_ms ASC"
        );
        let rows = block_on_pg(async move {
            sqlx::query(&sql)
                .fetch_all(&pool)
                .await
                .map_err(Error::from)
        })?;
        rows.into_iter().map(|r| self.decode_row(r)).collect()
    }

    fn delete(&self, agent_id: &AgentId) -> Result<bool, Error> {
        let pool = self.pool.clone();
        let aid = agent_id.as_str().to_string();
        let affected = block_on_pg(async move {
            sqlx::query("DELETE FROM public.agent_sandbox_bindings WHERE agent_id = $1")
                .bind(&aid)
                .execute(&pool)
                .await
                .map(|r| r.rows_affected())
                .map_err(Error::from)
        })?;
        Ok(affected > 0)
    }
}

/// Local filesystem fallback for zero-config SQLite mode.
#[derive(Clone, Debug)]
pub struct FsSandboxBindingStore {
    data_dir: PathBuf,
}

impl FsSandboxBindingStore {
    pub fn new(data_dir: impl AsRef<Path>) -> Self {
        Self {
            data_dir: data_dir.as_ref().to_path_buf(),
        }
    }

    fn path(&self, agent_id: &AgentId) -> PathBuf {
        self.data_dir
            .join("agents")
            .join(agent_id.as_str())
            .join("config")
            .join(LOCAL_BINDING_FILE)
    }
}

impl SandboxBindingStore for FsSandboxBindingStore {
    fn get(&self, agent_id: &AgentId) -> Result<Option<SandboxBinding>, Error> {
        let path = self.path(agent_id);
        let raw = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let binding: SandboxBinding = serde_json::from_slice(&raw)
            .map_err(|e| Error::Message(format!("invalid sandbox binding json: {e}")))?;
        Ok(Some(binding))
    }

    fn upsert(&self, binding: &SandboxBinding) -> Result<SandboxBinding, Error> {
        validate_lease_state(&binding.lease_state)?;
        let mut stored = binding.clone();
        if let Some(prev) = self.get(&binding.agent_id)? {
            stored.generation = prev.generation + 1;
        }
        stored.updated_at_ms = now_ms();
        let bytes = serde_json::to_vec_pretty(&stored)
            .map_err(|e| Error::Message(format!("serialize sandbox binding failed: {e}")))?;
        anyagents_harness::atomic_write_file(&self.path(&binding.agent_id), &bytes, 0o600)?;
        Ok(stored)
    }

    fn update_lease_state(
        &self,
        agent_id: &AgentId,
        lease_state: &str,
        idle_since_ms: Option<i64>,
    ) -> Result<bool, Error> {
        let state = validate_lease_state(lease_state)?;
        let Some(mut binding) = self.get(agent_id)? else {
            return Ok(false);
        };
        binding.lease_state = state.to_string();
        binding.idle_since_ms = idle_since_ms;
        self.upsert(&binding)?;
        Ok(true)
    }

    fn clear_lease(&self, agent_id: &AgentId) -> Result<bool, Error> {
        let Some(mut binding) = self.get(agent_id)? else {
            return Ok(false);
        };
        binding.lease_id = None;
        binding.lease_resource = None;
        binding.data_plane_resource = None;
        binding.guest_endpoint = None;
        binding.transport_url = None;
        binding.transport_token = None;
        binding.lease_state = "detached".to_string();
        binding.lease_expires_at_ms = None;
        binding.idle_since_ms = None;
        self.upsert(&binding)?;
        Ok(true)
    }

    fn touch_heartbeat(&self, agent_id: &AgentId, at_ms: i64) -> Result<bool, Error> {
        let Some(mut binding) = self.get(agent_id)? else {
            return Ok(false);
        };
        binding.last_heartbeat_ms = Some(at_ms);
        self.upsert(&binding)?;
        Ok(true)
    }

    fn list_idle_due(&self, now: i64) -> Result<Vec<SandboxBinding>, Error> {
        let mut due = Vec::new();
        let agents_dir = self.data_dir.join("agents");
        let Ok(entries) = fs::read_dir(&agents_dir) else {
            return Ok(due);
        };
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(id_str) = name.to_str() else {
                continue;
            };
            let Ok(agent_id) = AgentId::parse(id_str) else {
                continue;
            };
            if let Ok(Some(b)) = self.get(&agent_id)
                && b.lease_state == "ready"
                && let Some(idle_since) = b.idle_since_ms
                && idle_since.saturating_add(b.idle_ttl_ms) <= now
            {
                due.push(b);
            }
        }
        due.sort_by_key(|b| b.idle_since_ms.unwrap_or(i64::MAX));
        Ok(due)
    }

    fn list_active_ready(&self) -> Result<Vec<SandboxBinding>, Error> {
        let mut ready = Vec::new();
        let agents_dir = self.data_dir.join("agents");
        let Ok(entries) = fs::read_dir(&agents_dir) else {
            return Ok(ready);
        };
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(id_str) = name.to_str() else {
                continue;
            };
            let Ok(agent_id) = AgentId::parse(id_str) else {
                continue;
            };
            if let Ok(Some(b)) = self.get(&agent_id)
                && b.lease_state == "ready"
                && b.lease_id.is_some()
            {
                ready.push(b);
            }
        }
        ready.sort_by_key(|b| b.updated_at_ms);
        Ok(ready)
    }

    fn delete(&self, agent_id: &AgentId) -> Result<bool, Error> {
        match fs::remove_file(self.path(agent_id)) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e.into()),
        }
    }
}
