//! Durable cross-replica outbox/retry obligations (`public.durable_obligations`).
//!
//! Replaces pod-local `outbound-history/`, `telegram-relay-pending/`,
//! `reap-pending/`, and `.anyagents-spawn-finish.json` journals in PostgreSQL
//! mode while keeping a local filesystem implementation for SQLite mode.

use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sqlx::PgPool;

use crate::db::block_on_pg;
use crate::error::Error;
use crate::id::AgentId;
use crate::migrate::now_ms;

const VALID_KINDS: &[&str] = &[
    "outbound_history",
    "telegram_relay",
    "reap_archived",
    "spawn_finish",
];

const VALID_STATES: &[&str] = &["pending", "uncertain", "done", "discarded"];

fn validate_kind(kind: &str) -> Result<&'static str, Error> {
    VALID_KINDS
        .iter()
        .copied()
        .find(|k| *k == kind)
        .ok_or_else(|| Error::Message(format!("invalid durable obligation kind `{kind}`")))
}

fn validate_state(state: &str) -> Result<&'static str, Error> {
    VALID_STATES
        .iter()
        .copied()
        .find(|s| *s == state)
        .ok_or_else(|| Error::Message(format!("invalid durable obligation state `{state}`")))
}

/// One durable obligation row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableObligation {
    pub kind: String,
    pub key: String,
    pub agent_id: Option<AgentId>,
    pub state: String,
    pub payload_json: String,
    pub attempts: i32,
    pub next_at_ms: i64,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

/// Cross-replica durable outbox / retry queue abstraction.
pub trait DurableObligationStore: Send + Sync {
    fn upsert(&self, obligation: &DurableObligation) -> Result<DurableObligation, Error>;
    fn get(&self, kind: &str, key: &str) -> Result<Option<DurableObligation>, Error>;
    fn list_due(&self, kind: &str, now: i64, limit: i64) -> Result<Vec<DurableObligation>, Error>;
    fn set_state(
        &self,
        kind: &str,
        key: &str,
        state: &str,
        attempts: i32,
        next_at_ms: i64,
    ) -> Result<bool, Error>;
    fn delete(&self, kind: &str, key: &str) -> Result<bool, Error>;
}

/// PostgreSQL implementation backed by `public.durable_obligations`.
#[derive(Clone)]
pub struct PgDurableObligationStore {
    pool: PgPool,
}

impl PgDurableObligationStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    fn decode(
        (
            kind,
            key,
            agent_id,
            state,
            payload_json,
            attempts,
            next_at_ms,
            created_at_ms,
            updated_at_ms,
        ): (
            String,
            String,
            Option<String>,
            String,
            String,
            i32,
            i64,
            i64,
            i64,
        ),
    ) -> Result<DurableObligation, Error> {
        let agent_id = agent_id.map(|s| AgentId::parse(&s)).transpose()?;
        Ok(DurableObligation {
            kind,
            key,
            agent_id,
            state,
            payload_json,
            attempts,
            next_at_ms,
            created_at_ms,
            updated_at_ms,
        })
    }
}

impl DurableObligationStore for PgDurableObligationStore {
    fn upsert(&self, obligation: &DurableObligation) -> Result<DurableObligation, Error> {
        let kind = validate_kind(&obligation.kind)?;
        let state = validate_state(&obligation.state)?;
        let now = now_ms();
        let created = if obligation.created_at_ms > 0 {
            obligation.created_at_ms
        } else {
            now
        };
        let pool = self.pool.clone();
        let key = obligation.key.clone();
        let aid = obligation.agent_id.as_ref().map(|a| a.as_str().to_string());
        let payload = obligation.payload_json.clone();
        let attempts = obligation.attempts;
        let next_at = obligation.next_at_ms;
        let row = block_on_pg(async move {
            sqlx::query_as(
                "INSERT INTO public.durable_obligations
                    (kind, key, agent_id, state, payload_json, attempts, next_at_ms, created_at_ms, updated_at_ms)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                 ON CONFLICT (kind, key) DO UPDATE
                 SET agent_id      = EXCLUDED.agent_id,
                     state         = EXCLUDED.state,
                     payload_json  = EXCLUDED.payload_json,
                     attempts      = EXCLUDED.attempts,
                     next_at_ms    = EXCLUDED.next_at_ms,
                     updated_at_ms = EXCLUDED.updated_at_ms
                 RETURNING kind, key, agent_id, state, payload_json, attempts, next_at_ms, created_at_ms, updated_at_ms",
            )
            .bind(kind)
            .bind(&key)
            .bind(&aid)
            .bind(state)
            .bind(&payload)
            .bind(attempts)
            .bind(next_at)
            .bind(created)
            .bind(now)
            .fetch_one(&pool)
            .await
            .map_err(Error::from)
        })?;
        Self::decode(row)
    }

    fn get(&self, kind: &str, key: &str) -> Result<Option<DurableObligation>, Error> {
        let kind = validate_kind(kind)?;
        let pool = self.pool.clone();
        let key = key.to_string();
        let row = block_on_pg(async move {
            sqlx::query_as(
                "SELECT kind, key, agent_id, state, payload_json, attempts, next_at_ms, created_at_ms, updated_at_ms
                 FROM public.durable_obligations
                 WHERE kind = $1 AND key = $2",
            )
            .bind(kind)
            .bind(&key)
            .fetch_optional(&pool)
            .await
            .map_err(Error::from)
        })?;
        row.map(Self::decode).transpose()
    }

    fn list_due(&self, kind: &str, now: i64, limit: i64) -> Result<Vec<DurableObligation>, Error> {
        let kind = validate_kind(kind)?;
        let pool = self.pool.clone();
        let limit = limit.clamp(1, 1000);
        let rows = block_on_pg(async move {
            sqlx::query_as(
                "SELECT kind, key, agent_id, state, payload_json, attempts, next_at_ms, created_at_ms, updated_at_ms
                 FROM public.durable_obligations
                 WHERE kind = $1 AND state = 'pending' AND next_at_ms <= $2
                 ORDER BY next_at_ms ASC, created_at_ms ASC
                 LIMIT $3",
            )
            .bind(kind)
            .bind(now)
            .bind(limit)
            .fetch_all(&pool)
            .await
            .map_err(Error::from)
        })?;
        rows.into_iter().map(Self::decode).collect()
    }

    fn set_state(
        &self,
        kind: &str,
        key: &str,
        state: &str,
        attempts: i32,
        next_at_ms: i64,
    ) -> Result<bool, Error> {
        let kind = validate_kind(kind)?;
        let state = validate_state(state)?;
        let pool = self.pool.clone();
        let key = key.to_string();
        let now = now_ms();
        let affected = block_on_pg(async move {
            sqlx::query(
                "UPDATE public.durable_obligations
                 SET state = $3, attempts = $4, next_at_ms = $5, updated_at_ms = $6
                 WHERE kind = $1 AND key = $2",
            )
            .bind(kind)
            .bind(&key)
            .bind(state)
            .bind(attempts)
            .bind(next_at_ms)
            .bind(now)
            .execute(&pool)
            .await
            .map(|r| r.rows_affected())
            .map_err(Error::from)
        })?;
        Ok(affected > 0)
    }

    fn delete(&self, kind: &str, key: &str) -> Result<bool, Error> {
        let kind = validate_kind(kind)?;
        let pool = self.pool.clone();
        let key = key.to_string();
        let affected = block_on_pg(async move {
            sqlx::query("DELETE FROM public.durable_obligations WHERE kind = $1 AND key = $2")
                .bind(kind)
                .bind(&key)
                .execute(&pool)
                .await
                .map(|r| r.rows_affected())
                .map_err(Error::from)
        })?;
        Ok(affected > 0)
    }
}

/// Local filesystem fallback for SQLite mode under `<data_dir>/obligations/<kind>/<key>.json`.
#[derive(Clone, Debug)]
pub struct FsDurableObligationStore {
    root: PathBuf,
}

impl FsDurableObligationStore {
    pub fn new(data_dir: impl AsRef<Path>) -> Self {
        Self {
            root: data_dir.as_ref().join("obligations"),
        }
    }

    fn file_path(&self, kind: &str, key: &str) -> Result<PathBuf, Error> {
        let kind = validate_kind(kind)?;
        let safe_key: String = key
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        if safe_key.is_empty() {
            return Err(Error::Message(
                "durable obligation key cannot be empty".into(),
            ));
        }
        Ok(self.root.join(kind).join(format!("{safe_key}.json")))
    }
}

impl DurableObligationStore for FsDurableObligationStore {
    fn upsert(&self, obligation: &DurableObligation) -> Result<DurableObligation, Error> {
        validate_kind(&obligation.kind)?;
        validate_state(&obligation.state)?;
        let mut stored = obligation.clone();
        let now = now_ms();
        if stored.created_at_ms <= 0 {
            stored.created_at_ms = now;
        }
        stored.updated_at_ms = now;
        let bytes = serde_json::to_vec_pretty(&stored)
            .map_err(|e| Error::Message(format!("serialize obligation failed: {e}")))?;
        let path = self.file_path(&stored.kind, &stored.key)?;
        anyagents_harness::atomic_write_file(&path, &bytes, 0o600)?;
        Ok(stored)
    }

    fn get(&self, kind: &str, key: &str) -> Result<Option<DurableObligation>, Error> {
        let path = self.file_path(kind, key)?;
        let raw = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let item: DurableObligation = serde_json::from_slice(&raw)
            .map_err(|e| Error::Message(format!("invalid obligation json: {e}")))?;
        Ok(Some(item))
    }

    fn list_due(&self, kind: &str, now: i64, limit: i64) -> Result<Vec<DurableObligation>, Error> {
        let kind = validate_kind(kind)?;
        let dir = self.root.join(kind);
        let mut out = Vec::new();
        let Ok(entries) = fs::read_dir(&dir) else {
            return Ok(out);
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            if let Ok(raw) = fs::read(&path)
                && let Ok(item) = serde_json::from_slice::<DurableObligation>(&raw)
                && item.state == "pending"
                && item.next_at_ms <= now
            {
                out.push(item);
            }
        }
        out.sort_by_key(|o| (o.next_at_ms, o.created_at_ms));
        out.truncate(limit.clamp(1, 1000) as usize);
        Ok(out)
    }

    fn set_state(
        &self,
        kind: &str,
        key: &str,
        state: &str,
        attempts: i32,
        next_at_ms: i64,
    ) -> Result<bool, Error> {
        let state = validate_state(state)?;
        let Some(mut item) = self.get(kind, key)? else {
            return Ok(false);
        };
        item.state = state.to_string();
        item.attempts = attempts;
        item.next_at_ms = next_at_ms;
        self.upsert(&item)?;
        Ok(true)
    }

    fn delete(&self, kind: &str, key: &str) -> Result<bool, Error> {
        let path = self.file_path(kind, key)?;
        match fs::remove_file(path) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e.into()),
        }
    }
}
