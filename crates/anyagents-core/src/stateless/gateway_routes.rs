//! PostgreSQL cross-replica protocol gateway route table (`public.gateway_routes`).
//!
//! Stores per-agent provider routing metadata with AEAD-encrypted API keys and
//! bearer tokens keyed by `SHA-256(bearer_token)` so any coordinator replica
//! resolves gateway requests in O(1) without pod-local `provider.json` or
//! `gateway-token.txt` files.

use std::fmt;

use sqlx::PgPool;

use super::crypto::MasterKey;
use crate::db::block_on_pg;
use crate::error::Error;
use crate::id::AgentId;
use crate::learned_memory::sha256;
use crate::migrate::now_ms;

type GatewayRouteRow = (
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    Vec<u8>,
    Vec<u8>,
    i64,
);

/// Compute the indexed SHA-256 lookup key for a gateway bearer token.
pub fn hash_gateway_token(token: &str) -> String {
    sha256(token.as_bytes())
}

fn api_key_aad(agent_id: &AgentId, token_hash: &str) -> Vec<u8> {
    format!("gateway_routes:api_key:{}:{token_hash}", agent_id.as_str()).into_bytes()
}

fn token_aad(agent_id: &AgentId, token_hash: &str) -> Vec<u8> {
    format!("gateway_routes:token:{}:{token_hash}", agent_id.as_str()).into_bytes()
}

/// Input parameters for upserting one agent's gateway route.
#[derive(Clone)]
pub struct UpsertGatewayRoute {
    pub agent_id: AgentId,
    pub agent_name: String,
    pub harness: String,
    pub protocol: String,
    pub model: String,
    pub base_url: String,
    pub api_key: String,
    pub token: String,
}

impl fmt::Debug for UpsertGatewayRoute {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UpsertGatewayRoute")
            .field("agent_id", &self.agent_id)
            .field("agent_name", &self.agent_name)
            .field("harness", &self.harness)
            .field("protocol", &self.protocol)
            .field("model", &self.model)
            .field("base_url", &self.base_url)
            .field("api_key", &"[redacted]")
            .field("token", &"[redacted]")
            .finish()
    }
}

/// Decrypted gateway route loaded from `public.gateway_routes`.
#[derive(Clone, PartialEq, Eq)]
pub struct StoredGatewayRoute {
    pub token_hash: String,
    pub agent_id: AgentId,
    pub agent_name: String,
    pub harness: String,
    pub protocol: String,
    pub model: String,
    pub base_url: String,
    pub api_key: String,
    pub token: String,
    pub updated_at_ms: i64,
}

impl fmt::Debug for StoredGatewayRoute {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StoredGatewayRoute")
            .field("token_hash", &self.token_hash)
            .field("agent_id", &self.agent_id)
            .field("agent_name", &self.agent_name)
            .field("harness", &self.harness)
            .field("protocol", &self.protocol)
            .field("model", &self.model)
            .field("base_url", &self.base_url)
            .field("api_key", &"[redacted]")
            .field("token", &"[redacted]")
            .field("updated_at_ms", &self.updated_at_ms)
            .finish()
    }
}

/// PostgreSQL gateway route persistence & O(1) token resolution.
#[derive(Clone)]
pub struct PgGatewayRouteStore {
    pool: PgPool,
    master_key: Option<MasterKey>,
}

impl PgGatewayRouteStore {
    pub fn new(pool: PgPool, master_key: Option<MasterKey>) -> Self {
        Self { pool, master_key }
    }

    fn require_key(&self) -> Result<MasterKey, Error> {
        if let Some(ref key) = self.master_key {
            return Ok(key.clone());
        }
        MasterKey::require_from_env()
    }

    /// Upsert one agent's protocol gateway route (encrypting `api_key` and `token`).
    pub fn upsert(&self, route: &UpsertGatewayRoute) -> Result<StoredGatewayRoute, Error> {
        let key = self.require_key()?;
        let token_hash = hash_gateway_token(&route.token);
        let api_key_enc = key.encrypt(
            &api_key_aad(&route.agent_id, &token_hash),
            route.api_key.as_bytes(),
        )?;
        let token_enc = key.encrypt(
            &token_aad(&route.agent_id, &token_hash),
            route.token.as_bytes(),
        )?;
        let now = now_ms();
        let pool = self.pool.clone();
        let aid = route.agent_id.as_str().to_string();
        let aname = route.agent_name.clone();
        let harness = route.harness.clone();
        let protocol = route.protocol.clone();
        let model = route.model.clone();
        let base_url = route.base_url.trim_end_matches('/').to_string();
        let hash_c = token_hash.clone();

        block_on_pg(async move {
            sqlx::query(
                "INSERT INTO public.gateway_routes
                    (token_hash, agent_id, agent_name, harness, protocol, model, base_url, api_key_enc, token_enc, updated_at_ms)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                 ON CONFLICT (agent_id) DO UPDATE
                 SET token_hash    = EXCLUDED.token_hash,
                     agent_name    = EXCLUDED.agent_name,
                     harness       = EXCLUDED.harness,
                     protocol      = EXCLUDED.protocol,
                     model         = EXCLUDED.model,
                     base_url      = EXCLUDED.base_url,
                     api_key_enc   = EXCLUDED.api_key_enc,
                     token_enc     = EXCLUDED.token_enc,
                     updated_at_ms = EXCLUDED.updated_at_ms",
            )
            .bind(&hash_c)
            .bind(&aid)
            .bind(&aname)
            .bind(&harness)
            .bind(&protocol)
            .bind(&model)
            .bind(&base_url)
            .bind(&api_key_enc)
            .bind(&token_enc)
            .bind(now)
            .execute(&pool)
            .await
            .map_err(Error::from)
        })?;

        Ok(StoredGatewayRoute {
            token_hash,
            agent_id: route.agent_id.clone(),
            agent_name: route.agent_name.clone(),
            harness: route.harness.clone(),
            protocol: route.protocol.clone(),
            model: route.model.clone(),
            base_url: route.base_url.trim_end_matches('/').to_string(),
            api_key: route.api_key.clone(),
            token: route.token.clone(),
            updated_at_ms: now,
        })
    }

    /// Resolve a bearer token in O(1) across any coordinator replica.
    pub fn resolve_by_token(&self, token: &str) -> Result<Option<StoredGatewayRoute>, Error> {
        let token_hash = hash_gateway_token(token);
        let pool = self.pool.clone();
        let row: Option<GatewayRouteRow> = block_on_pg(async move {
            sqlx::query_as(
                "SELECT r.token_hash, r.agent_id, COALESCE(NULLIF(a.name, ''), r.agent_name),
                        r.harness, r.protocol, r.model, r.base_url,
                        r.api_key_enc, r.token_enc, r.updated_at_ms
                 FROM public.gateway_routes r
                 JOIN public.agents a ON a.id = r.agent_id
                 WHERE r.token_hash = $1 AND a.state != 'archived'",
            )
            .bind(&token_hash)
            .fetch_optional(&pool)
            .await
            .map_err(Error::from)
        })?;
        let Some(row) = row else {
            return Ok(None);
        };
        self.decode_row(row).map(Some)
    }

    /// Fetch the stored gateway route for an agent by `agent_id`.
    pub fn get_for_agent(&self, agent_id: &AgentId) -> Result<Option<StoredGatewayRoute>, Error> {
        let pool = self.pool.clone();
        let aid = agent_id.as_str().to_string();
        let row: Option<GatewayRouteRow> = block_on_pg(async move {
            sqlx::query_as(
                "SELECT r.token_hash, r.agent_id, COALESCE(NULLIF(a.name, ''), r.agent_name),
                        r.harness, r.protocol, r.model, r.base_url,
                        r.api_key_enc, r.token_enc, r.updated_at_ms
                 FROM public.gateway_routes r
                 JOIN public.agents a ON a.id = r.agent_id
                 WHERE r.agent_id = $1",
            )
            .bind(&aid)
            .fetch_optional(&pool)
            .await
            .map_err(Error::from)
        })?;
        let Some(row) = row else {
            return Ok(None);
        };
        self.decode_row(row).map(Some)
    }

    /// List all stored gateway routes for live (non-archived) agents.
    pub fn list_live(&self) -> Result<Vec<StoredGatewayRoute>, Error> {
        let pool = self.pool.clone();
        let rows: Vec<GatewayRouteRow> = block_on_pg(async move {
            sqlx::query_as(
                "SELECT r.token_hash, r.agent_id, COALESCE(NULLIF(a.name, ''), r.agent_name),
                        r.harness, r.protocol, r.model, r.base_url,
                        r.api_key_enc, r.token_enc, r.updated_at_ms
                 FROM public.gateway_routes r
                 JOIN public.agents a ON a.id = r.agent_id
                 WHERE a.state != 'archived'
                 ORDER BY r.agent_id ASC",
            )
            .fetch_all(&pool)
            .await
            .map_err(Error::from)
        })?;
        rows.into_iter().map(|r| self.decode_row(r)).collect()
    }

    /// Delete an agent's gateway route.
    pub fn delete_for_agent(&self, agent_id: &AgentId) -> Result<bool, Error> {
        let pool = self.pool.clone();
        let aid = agent_id.as_str().to_string();
        let affected = block_on_pg(async move {
            sqlx::query("DELETE FROM public.gateway_routes WHERE agent_id = $1")
                .bind(&aid)
                .execute(&pool)
                .await
                .map(|r| r.rows_affected())
                .map_err(Error::from)
        })?;
        Ok(affected > 0)
    }

    #[allow(clippy::type_complexity)]
    fn decode_row(
        &self,
        (
            token_hash,
            agent_id_str,
            agent_name,
            harness,
            protocol,
            model,
            base_url,
            api_key_enc,
            token_enc,
            updated_at_ms,
        ): (
            String,
            String,
            String,
            String,
            String,
            String,
            String,
            Vec<u8>,
            Vec<u8>,
            i64,
        ),
    ) -> Result<StoredGatewayRoute, Error> {
        let key = self.require_key()?;
        let agent_id = AgentId::parse(&agent_id_str)?;
        let api_key_bytes = key.decrypt(&api_key_aad(&agent_id, &token_hash), &api_key_enc)?;
        let token_bytes = key.decrypt(&token_aad(&agent_id, &token_hash), &token_enc)?;
        let api_key = String::from_utf8(api_key_bytes)
            .map_err(|_| Error::Message("decrypted gateway api_key is not utf-8".into()))?;
        let token = String::from_utf8(token_bytes)
            .map_err(|_| Error::Message("decrypted gateway token is not utf-8".into()))?;
        Ok(StoredGatewayRoute {
            token_hash,
            agent_id,
            agent_name,
            harness,
            protocol,
            model,
            base_url,
            api_key,
            token,
            updated_at_ms,
        })
    }
}
