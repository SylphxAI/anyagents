//! Durable serverless remote turn ledger (`public.pending_remote_turns`).
//!
//! Decouples turn dispatch from turn completion so a coordinator can scale to
//! zero replicas or restart mid-turn while detached sandbox turns (`anyagents-guest turn`)
//! and asynchronous cloud harness turns (Spiron, Generic HTTP Cloud) continue
//! executing and settle idempotently via HTTP callbacks or `/v1/internal/tick`.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::PgPool;

use crate::db::block_on_pg;
use crate::error::Error;
use crate::id::AgentId;
use crate::migrate::now_ms;

const LOCAL_REMOTE_TURNS_DIR: &str = "remote-turns";

/// Allowed `pending_remote_turns.status` values.
pub const VALID_REMOTE_TURN_STATUSES: &[&str] =
    &["dispatched", "running", "completed", "failed", "cancelled"];

/// Allowed terminal settlement statuses.
pub const TERMINAL_REMOTE_TURN_STATUSES: &[&str] = &["completed", "failed", "cancelled"];

pub fn is_terminal_remote_turn_status(status: &str) -> bool {
    TERMINAL_REMOTE_TURN_STATUSES.contains(&status)
}

pub(super) fn validate_status(status: &str) -> Result<&'static str, Error> {
    VALID_REMOTE_TURN_STATUSES
        .iter()
        .copied()
        .find(|s| *s == status)
        .ok_or_else(|| Error::Message(format!("invalid remote turn status `{status}`")))
}

pub(super) fn validate_terminal_status(status: &str) -> Result<&'static str, Error> {
    TERMINAL_REMOTE_TURN_STATUSES
        .iter()
        .copied()
        .find(|s| *s == status)
        .ok_or_else(|| {
            Error::Message(format!(
                "invalid terminal remote turn status `{status}` (expected completed, failed, or cancelled)"
            ))
        })
}

pub(super) fn validate_turn_id(turn_id: &str) -> Result<&str, Error> {
    let trimmed = turn_id.trim();
    if trimmed.is_empty()
        || trimmed.len() > 128
        || trimmed.contains('/')
        || trimmed.contains('\\')
        || trimmed.contains("..")
        || trimmed.bytes().any(|b| b.is_ascii_control())
    {
        return Err(Error::Message(format!("invalid turn_id `{turn_id}`")));
    }
    Ok(trimmed)
}

/// Sanitize NUL bytes (`\u{0000}`) which PostgreSQL `TEXT` and `JSONB` reject.
pub fn sanitize_pg_text(raw: &str) -> String {
    if raw.contains('\0') {
        raw.replace('\0', "\u{FFFD}")
    } else {
        raw.to_string()
    }
}

/// Recursively sanitize `\u{0000}` from strings inside a JSON value before PostgreSQL `JSONB` storage.
pub fn sanitize_pg_json(value: &Value) -> Value {
    match value {
        Value::String(s) => Value::String(sanitize_pg_text(s)),
        Value::Array(items) => Value::Array(items.iter().map(sanitize_pg_json).collect()),
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(k, v)| (sanitize_pg_text(k), sanitize_pg_json(v)))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// Constant-time byte slice comparison (returns `false` on length mismatch).
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Generate a time-ordered unique remote turn identifier (`turn_<uuidv7>`).
pub fn generate_turn_id() -> String {
    format!("turn_{}", uuid::Uuid::now_v7().simple())
}

/// Generate a high-entropy per-turn callback bearer token (`rtk_<64-hex>`).
pub fn generate_callback_token() -> String {
    let mut h = Sha256::new();
    h.update(b"anyagents-remote-turn-callback-token-v1");
    h.update(uuid::Uuid::new_v4().as_bytes());
    h.update(uuid::Uuid::new_v4().as_bytes());
    h.update(uuid::Uuid::now_v7().as_bytes());
    let digest = h.finalize();
    let mut hex = String::with_capacity(4 + 64);
    hex.push_str("rtk_");
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut hex, "{byte:02x}");
    }
    hex
}

/// Compute lowercase hex SHA-256 digest of a callback bearer token.
pub fn hash_callback_token(token: &str) -> String {
    let digest = Sha256::digest(token.as_bytes());
    let mut hex = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut hex, "{byte:02x}");
    }
    hex
}

/// Compute lowercase hex HMAC-SHA256 over `payload` keyed by `secret`.
pub fn hmac_sha256_hex(secret: &[u8], payload: &[u8]) -> String {
    let mut key_block = [0u8; 64];
    if secret.len() > 64 {
        let hashed = Sha256::digest(secret);
        key_block[..32].copy_from_slice(&hashed);
    } else {
        key_block[..secret.len()].copy_from_slice(secret);
    }
    let mut ipad = [0x36u8; 64];
    let mut opad = [0x5cu8; 64];
    for i in 0..64 {
        ipad[i] ^= key_block[i];
        opad[i] ^= key_block[i];
    }
    let mut inner = Sha256::new();
    inner.update(ipad);
    inner.update(payload);
    let inner_digest = inner.finalize();

    let mut outer = Sha256::new();
    outer.update(opad);
    outer.update(inner_digest);
    let digest = outer.finalize();

    let mut hex = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut hex, "{byte:02x}");
    }
    hex
}

/// Constant-time SHA-256 verification of a presented bearer token against a stored hash.
pub fn verify_callback_token_hash(expected_hash: &str, presented_token: &str) -> bool {
    let token = presented_token.trim();
    let expected = expected_hash.trim();
    if token.is_empty() || expected.len() != 64 {
        return false;
    }
    let actual = hash_callback_token(token);
    constant_time_eq(expected.as_bytes(), actual.as_bytes())
}

/// Constant-time HMAC-SHA256 verification for webhook signature headers (`sha256=<hex>` or `<hex>`).
pub fn verify_hmac_sha256_signature(secret: &[u8], body: &[u8], signature_header: &str) -> bool {
    if secret.is_empty() {
        return false;
    }
    let raw = signature_header.trim();
    let provided_hex = raw
        .strip_prefix("sha256=")
        .or_else(|| raw.strip_prefix("SHA256="))
        .unwrap_or(raw)
        .trim()
        .to_ascii_lowercase();
    if provided_hex.len() != 64 || !provided_hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return false;
    }
    let expected = hmac_sha256_hex(secret, body);
    constant_time_eq(expected.as_bytes(), provided_hex.as_bytes())
}

/// Durable remote turn record stored in `public.pending_remote_turns`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PendingRemoteTurn {
    pub turn_id: String,
    pub agent_id: AgentId,
    pub harness_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_run_id: Option<String>,
    pub callback_token_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage_json: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_json: Option<Value>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub last_event_at_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_poll_at_ms: Option<i64>,
}

impl PendingRemoteTurn {
    pub fn is_terminal(&self) -> bool {
        is_terminal_remote_turn_status(&self.status)
    }
}

/// Input parameters for creating a new `pending_remote_turns` row.
#[derive(Clone, Debug)]
pub struct NewRemoteTurn {
    pub turn_id: String,
    pub agent_id: AgentId,
    pub harness_kind: String,
    pub provider_kind: Option<String>,
    pub remote_run_id: Option<String>,
    pub callback_token_hash: String,
    pub cursor: Option<String>,
    pub status: String,
    pub metadata_json: Option<Value>,
    pub next_poll_at_ms: Option<i64>,
}

/// Non-terminal incremental progress update (heartbeat, delta, or poll cursor advance).
#[derive(Clone, Debug, Default)]
pub struct RemoteTurnEventUpdate {
    pub status: Option<String>,
    pub cursor: Option<String>,
    pub remote_run_id: Option<String>,
    pub last_event_at_ms: i64,
    pub next_poll_at_ms: Option<Option<i64>>,
}

/// Terminal settlement payload for `settle_turn`.
#[derive(Clone, Debug)]
pub struct SettleRemoteTurn {
    pub status: String,
    pub result_text: Option<String>,
    pub error_text: Option<String>,
    pub usage_json: Option<Value>,
    pub cursor: Option<String>,
    pub settled_at_ms: i64,
}

/// Outcome of an atomic, idempotent `settle_turn` attempt.
#[derive(Clone, Debug, PartialEq)]
pub struct SettleOutcome {
    pub turn: PendingRemoteTurn,
    /// `true` iff this call transitioned the turn from non-terminal (`dispatched`/`running`)
    /// into a terminal state. `false` indicates an idempotent replay against an already settled turn.
    pub transitioned: bool,
}

/// Persistence abstraction for serverless remote turns.
pub trait RemoteTurnStore: Send + Sync {
    fn create_turn(&self, input: &NewRemoteTurn) -> Result<PendingRemoteTurn, Error>;
    fn get_turn(&self, turn_id: &str) -> Result<Option<PendingRemoteTurn>, Error>;
    fn find_by_remote_run_id(
        &self,
        harness_kind: Option<&str>,
        remote_run_id: &str,
    ) -> Result<Option<PendingRemoteTurn>, Error>;
    /// Constant-time SHA-256 verification of `bearer_token` for `turn_id`.
    /// Returns `Ok(Some(turn))` iff `turn_id` exists and the token matches; `Ok(None)` otherwise.
    fn verify_callback_token(
        &self,
        turn_id: &str,
        bearer_token: &str,
    ) -> Result<Option<PendingRemoteTurn>, Error>;
    fn update_remote_run(
        &self,
        turn_id: &str,
        remote_run_id: Option<&str>,
        status: Option<&str>,
        next_poll_at_ms: Option<Option<i64>>,
        metadata_json: Option<&Value>,
    ) -> Result<Option<PendingRemoteTurn>, Error>;
    fn record_event(
        &self,
        turn_id: &str,
        update: &RemoteTurnEventUpdate,
    ) -> Result<Option<PendingRemoteTurn>, Error>;
    fn settle_turn(
        &self,
        turn_id: &str,
        settlement: &SettleRemoteTurn,
    ) -> Result<Option<SettleOutcome>, Error>;
    fn list_due_polls(&self, now_ms: i64, limit: usize) -> Result<Vec<PendingRemoteTurn>, Error>;
    fn list_active_for_agent(&self, agent_id: &AgentId) -> Result<Vec<PendingRemoteTurn>, Error>;
    fn list_stale_active(
        &self,
        harness_kind: Option<&str>,
        stale_before_ms: i64,
        limit: usize,
    ) -> Result<Vec<PendingRemoteTurn>, Error>;
    fn schedule_next_poll(
        &self,
        turn_id: &str,
        next_poll_at_ms: Option<i64>,
        cursor: Option<&str>,
        now_ms: i64,
    ) -> Result<bool, Error>;
    fn delete_turn(&self, turn_id: &str) -> Result<bool, Error>;
    fn list_active_fresh_by_agent(
        &self,
        fresh_since_ms: i64,
    ) -> Result<HashMap<String, i64>, Error>;
    fn latest_failed_for_agent(
        &self,
        agent_id: &AgentId,
    ) -> Result<Option<super::remote_turns_ext::LatestRemoteTurnError>, Error>;
    fn list_latest_failed_by_agent(
        &self,
    ) -> Result<HashMap<String, super::remote_turns_ext::LatestRemoteTurnError>, Error>;
    fn record_agent_inbox_read_seq(
        &self,
        agent_id: &AgentId,
        max_read_seq: i64,
        now_ms: i64,
    ) -> Result<usize, Error>;
}

const SELECT_TURN_COLS: &str = "turn_id, agent_id, harness_kind, provider_kind, remote_run_id, \
    callback_token_hash, cursor, status, result_text, error_text, \
    usage_json::text AS usage_json_text, metadata_json::text AS metadata_json_text, \
    created_at_ms, updated_at_ms, last_event_at_ms, next_poll_at_ms";

/// PostgreSQL implementation backed by `public.pending_remote_turns`.
#[derive(Clone)]
pub struct PgRemoteTurnStore {
    pool: PgPool,
}

impl PgRemoteTurnStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    fn decode_row(row: sqlx::postgres::PgRow) -> Result<PendingRemoteTurn, Error> {
        use sqlx::Row as _;
        let aid_str: String = row.try_get("agent_id").map_err(Error::from)?;
        let agent_id = AgentId::parse(&aid_str)?;
        let usage_text: Option<String> = row.try_get("usage_json_text").map_err(Error::from)?;
        let usage_json = match usage_text {
            Some(s) if !s.trim().is_empty() => Some(
                serde_json::from_str::<Value>(&s)
                    .map_err(|e| Error::Message(format!("invalid usage_json in pg: {e}")))?,
            ),
            _ => None,
        };
        let meta_text: Option<String> = row.try_get("metadata_json_text").map_err(Error::from)?;
        let metadata_json = match meta_text {
            Some(s) if !s.trim().is_empty() => Some(
                serde_json::from_str::<Value>(&s)
                    .map_err(|e| Error::Message(format!("invalid metadata_json in pg: {e}")))?,
            ),
            _ => None,
        };
        Ok(PendingRemoteTurn {
            turn_id: row.try_get("turn_id").map_err(Error::from)?,
            agent_id,
            harness_kind: row.try_get("harness_kind").map_err(Error::from)?,
            provider_kind: row.try_get("provider_kind").map_err(Error::from)?,
            remote_run_id: row.try_get("remote_run_id").map_err(Error::from)?,
            callback_token_hash: row.try_get("callback_token_hash").map_err(Error::from)?,
            cursor: row.try_get("cursor").map_err(Error::from)?,
            status: row.try_get("status").map_err(Error::from)?,
            result_text: row.try_get("result_text").map_err(Error::from)?,
            error_text: row.try_get("error_text").map_err(Error::from)?,
            usage_json,
            metadata_json,
            created_at_ms: row.try_get("created_at_ms").map_err(Error::from)?,
            updated_at_ms: row.try_get("updated_at_ms").map_err(Error::from)?,
            last_event_at_ms: row.try_get("last_event_at_ms").map_err(Error::from)?,
            next_poll_at_ms: row.try_get("next_poll_at_ms").map_err(Error::from)?,
        })
    }
}

fn encode_opt_json(value: Option<&Value>) -> Result<Option<String>, Error> {
    match value {
        Some(v) => {
            let clean = sanitize_pg_json(v);
            serde_json::to_string(&clean)
                .map(Some)
                .map_err(|e| Error::Message(format!("serialize remote turn json: {e}")))
        }
        None => Ok(None),
    }
}

impl RemoteTurnStore for PgRemoteTurnStore {
    fn create_turn(&self, input: &NewRemoteTurn) -> Result<PendingRemoteTurn, Error> {
        let turn_id = validate_turn_id(&input.turn_id)?.to_string();
        let status = validate_status(&input.status)?;
        let now = now_ms();
        let meta_str = encode_opt_json(input.metadata_json.as_ref())?;
        let pool = self.pool.clone();
        let aid = input.agent_id.as_str().to_string();
        let harness_kind = sanitize_pg_text(&input.harness_kind);
        let provider_kind = input.provider_kind.as_deref().map(sanitize_pg_text);
        let remote_run_id = input.remote_run_id.as_deref().map(sanitize_pg_text);
        let callback_token_hash = input.callback_token_hash.clone();
        let cursor = input.cursor.as_deref().map(sanitize_pg_text);
        let next_poll_at_ms = input.next_poll_at_ms;
        let sql = format!(
            "INSERT INTO public.pending_remote_turns (
                turn_id, agent_id, harness_kind, provider_kind, remote_run_id,
                callback_token_hash, cursor, status, metadata_json,
                created_at_ms, updated_at_ms, last_event_at_ms, next_poll_at_ms
             ) VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8, $9::jsonb,
                $10, $10, $10, $11
             )
             RETURNING {SELECT_TURN_COLS}"
        );
        let row = block_on_pg(async move {
            sqlx::query(&sql)
                .bind(&turn_id)
                .bind(&aid)
                .bind(&harness_kind)
                .bind(&provider_kind)
                .bind(&remote_run_id)
                .bind(&callback_token_hash)
                .bind(&cursor)
                .bind(status)
                .bind(&meta_str)
                .bind(now)
                .bind(next_poll_at_ms)
                .fetch_one(&pool)
                .await
                .map_err(|err| {
                    let mapped = Error::from(err);
                    if mapped.is_active_turn_conflict() {
                        Error::TurnAlreadyActive(aid.clone())
                    } else {
                        mapped
                    }
                })
        })?;
        Self::decode_row(row)
    }

    fn get_turn(&self, turn_id: &str) -> Result<Option<PendingRemoteTurn>, Error> {
        let Ok(clean_id) = validate_turn_id(turn_id) else {
            return Ok(None);
        };
        let pool = self.pool.clone();
        let id = clean_id.to_string();
        let sql = format!(
            "SELECT {SELECT_TURN_COLS} FROM public.pending_remote_turns WHERE turn_id = $1"
        );
        let row = block_on_pg(async move {
            sqlx::query(&sql)
                .bind(&id)
                .fetch_optional(&pool)
                .await
                .map_err(Error::from)
        })?;
        row.map(Self::decode_row).transpose()
    }

    fn find_by_remote_run_id(
        &self,
        harness_kind: Option<&str>,
        remote_run_id: &str,
    ) -> Result<Option<PendingRemoteTurn>, Error> {
        let trimmed = remote_run_id.trim();
        if trimmed.is_empty() {
            return Ok(None);
        }
        let pool = self.pool.clone();
        let run_id = trimmed.to_string();
        let kind = harness_kind
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        let row = block_on_pg(async move {
            if let Some(hk) = kind {
                let sql = format!(
                    "SELECT {SELECT_TURN_COLS} FROM public.pending_remote_turns
                     WHERE harness_kind = $1 AND remote_run_id = $2
                     ORDER BY created_at_ms DESC LIMIT 1"
                );
                sqlx::query(&sql)
                    .bind(&hk)
                    .bind(&run_id)
                    .fetch_optional(&pool)
                    .await
                    .map_err(Error::from)
            } else {
                let sql = format!(
                    "SELECT {SELECT_TURN_COLS} FROM public.pending_remote_turns
                     WHERE remote_run_id = $1
                     ORDER BY created_at_ms DESC LIMIT 1"
                );
                sqlx::query(&sql)
                    .bind(&run_id)
                    .fetch_optional(&pool)
                    .await
                    .map_err(Error::from)
            }
        })?;
        row.map(Self::decode_row).transpose()
    }

    fn verify_callback_token(
        &self,
        turn_id: &str,
        bearer_token: &str,
    ) -> Result<Option<PendingRemoteTurn>, Error> {
        let Some(turn) = self.get_turn(turn_id)? else {
            return Ok(None);
        };
        if verify_callback_token_hash(&turn.callback_token_hash, bearer_token) {
            Ok(Some(turn))
        } else {
            Ok(None)
        }
    }

    fn update_remote_run(
        &self,
        turn_id: &str,
        remote_run_id: Option<&str>,
        status: Option<&str>,
        next_poll_at_ms: Option<Option<i64>>,
        metadata_json: Option<&Value>,
    ) -> Result<Option<PendingRemoteTurn>, Error> {
        let id = validate_turn_id(turn_id)?.to_string();
        let validated_status = match status {
            Some(s) => Some(validate_status(s)?.to_string()),
            None => None,
        };
        let meta_str = encode_opt_json(metadata_json)?;
        let now = now_ms();
        let pool = self.pool.clone();
        let run_id = remote_run_id.map(sanitize_pg_text);
        let update_next_poll = next_poll_at_ms.is_some();
        let next_poll_val = next_poll_at_ms.flatten();
        let sql = format!(
            "UPDATE public.pending_remote_turns
             SET remote_run_id   = COALESCE($2, remote_run_id),
                 status          = CASE
                                     WHEN status IN ('completed', 'failed', 'cancelled') THEN status
                                     ELSE COALESCE($3, status)
                                   END,
                 next_poll_at_ms = CASE
                                     WHEN status IN ('completed', 'failed', 'cancelled') THEN NULL
                                     WHEN $4 THEN $5
                                     ELSE next_poll_at_ms
                                   END,
                 metadata_json   = COALESCE($6::jsonb, metadata_json),
                 updated_at_ms   = GREATEST(updated_at_ms, $7)
             WHERE turn_id = $1
             RETURNING {SELECT_TURN_COLS}"
        );
        let row = block_on_pg(async move {
            sqlx::query(&sql)
                .bind(&id)
                .bind(&run_id)
                .bind(&validated_status)
                .bind(update_next_poll)
                .bind(next_poll_val)
                .bind(&meta_str)
                .bind(now)
                .fetch_optional(&pool)
                .await
                .map_err(Error::from)
        })?;
        row.map(Self::decode_row).transpose()
    }

    fn record_event(
        &self,
        turn_id: &str,
        update: &RemoteTurnEventUpdate,
    ) -> Result<Option<PendingRemoteTurn>, Error> {
        let id = validate_turn_id(turn_id)?.to_string();
        let next_status = match update.status.as_deref() {
            Some(s) => {
                let valid = validate_status(s)?;
                if is_terminal_remote_turn_status(valid) {
                    return Err(Error::Message(
                        "record_event cannot write terminal status; use settle_turn".into(),
                    ));
                }
                Some(valid.to_string())
            }
            None => Some("running".to_string()),
        };
        let event_ts = if update.last_event_at_ms > 0 {
            update.last_event_at_ms
        } else {
            now_ms()
        };
        let now = now_ms().max(event_ts);
        let cursor = update.cursor.as_deref().map(sanitize_pg_text);
        let remote_run_id = update.remote_run_id.as_deref().map(sanitize_pg_text);
        let update_next_poll = update.next_poll_at_ms.is_some();
        let next_poll_val = update.next_poll_at_ms.flatten();
        let pool = self.pool.clone();
        let sql = format!(
            "UPDATE public.pending_remote_turns
             SET status           = CASE
                                      WHEN status IN ('completed', 'failed', 'cancelled') THEN status
                                      ELSE COALESCE($2, status)
                                    END,
                 cursor           = COALESCE($3, cursor),
                 remote_run_id    = COALESCE($4, remote_run_id),
                 last_event_at_ms = GREATEST(last_event_at_ms, $5),
                 updated_at_ms    = GREATEST(updated_at_ms, $6),
                 next_poll_at_ms  = CASE
                                      WHEN status IN ('completed', 'failed', 'cancelled') THEN NULL
                                      WHEN $7 THEN $8
                                      ELSE next_poll_at_ms
                                    END
             WHERE turn_id = $1
             RETURNING {SELECT_TURN_COLS}"
        );
        let row = block_on_pg(async move {
            sqlx::query(&sql)
                .bind(&id)
                .bind(&next_status)
                .bind(&cursor)
                .bind(&remote_run_id)
                .bind(event_ts)
                .bind(now)
                .bind(update_next_poll)
                .bind(next_poll_val)
                .fetch_optional(&pool)
                .await
                .map_err(Error::from)
        })?;
        row.map(Self::decode_row).transpose()
    }

    fn settle_turn(
        &self,
        turn_id: &str,
        settlement: &SettleRemoteTurn,
    ) -> Result<Option<SettleOutcome>, Error> {
        let id = validate_turn_id(turn_id)?.to_string();
        let terminal = validate_terminal_status(&settlement.status)?;
        let settled_at = if settlement.settled_at_ms > 0 {
            settlement.settled_at_ms
        } else {
            now_ms()
        };
        let now = now_ms().max(settled_at);
        let result_text = settlement.result_text.as_deref().map(sanitize_pg_text);
        let error_text = settlement.error_text.as_deref().map(sanitize_pg_text);
        let usage_str = encode_opt_json(settlement.usage_json.as_ref())?;
        let cursor = settlement.cursor.as_deref().map(sanitize_pg_text);

        let pool = self.pool.clone();
        let id_clone = id.clone();
        let sql = format!(
            "UPDATE public.pending_remote_turns
             SET status           = $2,
                 result_text      = $3,
                 error_text       = $4,
                 usage_json       = $5::jsonb,
                 cursor           = COALESCE($6, cursor),
                 last_event_at_ms = GREATEST(last_event_at_ms, $7),
                 updated_at_ms    = GREATEST(updated_at_ms, $8),
                 next_poll_at_ms  = NULL
             WHERE turn_id = $1
               AND status IN ('dispatched', 'running')
             RETURNING {SELECT_TURN_COLS}"
        );
        let transitioned_row = block_on_pg(async move {
            sqlx::query(&sql)
                .bind(&id_clone)
                .bind(terminal)
                .bind(&result_text)
                .bind(&error_text)
                .bind(&usage_str)
                .bind(&cursor)
                .bind(settled_at)
                .bind(now)
                .fetch_optional(&pool)
                .await
                .map_err(Error::from)
        })?;
        if let Some(row) = transitioned_row {
            return Ok(Some(SettleOutcome {
                turn: Self::decode_row(row)?,
                transitioned: true,
            }));
        }
        // Row was either missing or already terminal; return idempotent replay if present.
        Ok(self.get_turn(&id)?.map(|turn| SettleOutcome {
            turn,
            transitioned: false,
        }))
    }

    fn list_due_polls(&self, now: i64, limit: usize) -> Result<Vec<PendingRemoteTurn>, Error> {
        let limit = i64::try_from(limit.max(1)).unwrap_or(64);
        let pool = self.pool.clone();
        let sql = format!(
            "SELECT {SELECT_TURN_COLS}
             FROM public.pending_remote_turns
             WHERE status IN ('dispatched', 'running')
               AND next_poll_at_ms IS NOT NULL
               AND next_poll_at_ms <= $1
             ORDER BY next_poll_at_ms ASC, created_at_ms ASC
             LIMIT $2"
        );
        let rows = block_on_pg(async move {
            sqlx::query(&sql)
                .bind(now)
                .bind(limit)
                .fetch_all(&pool)
                .await
                .map_err(Error::from)
        })?;
        rows.into_iter().map(Self::decode_row).collect()
    }

    fn list_active_for_agent(&self, agent_id: &AgentId) -> Result<Vec<PendingRemoteTurn>, Error> {
        let pool = self.pool.clone();
        let aid = agent_id.as_str().to_string();
        let sql = format!(
            "SELECT {SELECT_TURN_COLS}
             FROM public.pending_remote_turns
             WHERE agent_id = $1
               AND status IN ('dispatched', 'running')
             ORDER BY created_at_ms ASC"
        );
        let rows = block_on_pg(async move {
            sqlx::query(&sql)
                .bind(&aid)
                .fetch_all(&pool)
                .await
                .map_err(Error::from)
        })?;
        rows.into_iter().map(Self::decode_row).collect()
    }

    fn list_stale_active(
        &self,
        harness_kind: Option<&str>,
        stale_before_ms: i64,
        limit: usize,
    ) -> Result<Vec<PendingRemoteTurn>, Error> {
        let limit = i64::try_from(limit.max(1)).unwrap_or(64);
        let pool = self.pool.clone();
        let kind = harness_kind
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        let rows = block_on_pg(async move {
            if let Some(hk) = kind {
                let sql = format!(
                    "SELECT {SELECT_TURN_COLS}
                     FROM public.pending_remote_turns
                     WHERE status IN ('dispatched', 'running')
                       AND harness_kind = $1
                       AND last_event_at_ms <= $2
                     ORDER BY last_event_at_ms ASC
                     LIMIT $3"
                );
                sqlx::query(&sql)
                    .bind(&hk)
                    .bind(stale_before_ms)
                    .bind(limit)
                    .fetch_all(&pool)
                    .await
                    .map_err(Error::from)
            } else {
                let sql = format!(
                    "SELECT {SELECT_TURN_COLS}
                     FROM public.pending_remote_turns
                     WHERE status IN ('dispatched', 'running')
                       AND last_event_at_ms <= $1
                     ORDER BY last_event_at_ms ASC
                     LIMIT $2"
                );
                sqlx::query(&sql)
                    .bind(stale_before_ms)
                    .bind(limit)
                    .fetch_all(&pool)
                    .await
                    .map_err(Error::from)
            }
        })?;
        rows.into_iter().map(Self::decode_row).collect()
    }

    fn schedule_next_poll(
        &self,
        turn_id: &str,
        next_poll_at_ms: Option<i64>,
        cursor: Option<&str>,
        now: i64,
    ) -> Result<bool, Error> {
        let id = validate_turn_id(turn_id)?.to_string();
        let cursor = cursor.map(sanitize_pg_text);
        let pool = self.pool.clone();
        let affected = block_on_pg(async move {
            sqlx::query(
                "UPDATE public.pending_remote_turns
                 SET next_poll_at_ms  = $2,
                     cursor           = COALESCE($3, cursor),
                     last_event_at_ms = GREATEST(last_event_at_ms, $4),
                     updated_at_ms    = GREATEST(updated_at_ms, $4)
                 WHERE turn_id = $1
                   AND status IN ('dispatched', 'running')",
            )
            .bind(&id)
            .bind(next_poll_at_ms)
            .bind(&cursor)
            .bind(now)
            .execute(&pool)
            .await
            .map(|r| r.rows_affected())
            .map_err(Error::from)
        })?;
        Ok(affected > 0)
    }

    fn delete_turn(&self, turn_id: &str) -> Result<bool, Error> {
        let id = validate_turn_id(turn_id)?.to_string();
        let pool = self.pool.clone();
        let affected = block_on_pg(async move {
            sqlx::query("DELETE FROM public.pending_remote_turns WHERE turn_id = $1")
                .bind(&id)
                .execute(&pool)
                .await
                .map(|r| r.rows_affected())
                .map_err(Error::from)
        })?;
        Ok(affected > 0)
    }

    fn list_active_fresh_by_agent(
        &self,
        fresh_since_ms: i64,
    ) -> Result<HashMap<String, i64>, Error> {
        super::remote_turns_ext::pg_list_active_fresh_by_agent(&self.pool, fresh_since_ms)
    }

    fn latest_failed_for_agent(
        &self,
        agent_id: &AgentId,
    ) -> Result<Option<super::remote_turns_ext::LatestRemoteTurnError>, Error> {
        super::remote_turns_ext::pg_latest_failed_for_agent(&self.pool, agent_id)
    }

    fn list_latest_failed_by_agent(
        &self,
    ) -> Result<HashMap<String, super::remote_turns_ext::LatestRemoteTurnError>, Error> {
        super::remote_turns_ext::pg_list_latest_failed_by_agent(&self.pool)
    }

    fn record_agent_inbox_read_seq(
        &self,
        agent_id: &AgentId,
        max_read_seq: i64,
        now: i64,
    ) -> Result<usize, Error> {
        super::remote_turns_ext::pg_record_agent_inbox_read_seq(
            &self.pool,
            agent_id,
            max_read_seq,
            now,
        )
    }
}

/// Local filesystem fallback (`<data_dir>/remote-turns/<turn_id>.json`) for SQLite mode.
#[derive(Clone, Debug)]
pub struct FsRemoteTurnStore {
    root: PathBuf,
    guard: Arc<Mutex<()>>,
}

impl FsRemoteTurnStore {
    pub fn new(data_dir: impl AsRef<Path>) -> Self {
        Self {
            root: data_dir.as_ref().join(LOCAL_REMOTE_TURNS_DIR),
            guard: Arc::new(Mutex::new(())),
        }
    }

    fn turn_path(&self, turn_id: &str) -> Result<PathBuf, Error> {
        let id = validate_turn_id(turn_id)?;
        Ok(self.root.join(format!("{id}.json")))
    }

    fn read_unlocked(&self, turn_id: &str) -> Result<Option<PendingRemoteTurn>, Error> {
        let path = self.turn_path(turn_id)?;
        let raw = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let turn: PendingRemoteTurn = serde_json::from_slice(&raw)
            .map_err(|e| Error::Message(format!("invalid remote turn json: {e}")))?;
        Ok(Some(turn))
    }

    fn write_unlocked(&self, turn: &PendingRemoteTurn) -> Result<(), Error> {
        let path = self.turn_path(&turn.turn_id)?;
        let bytes = serde_json::to_vec_pretty(turn)
            .map_err(|e| Error::Message(format!("serialize remote turn json: {e}")))?;
        anyagents_harness::atomic_write_file(&path, &bytes, 0o600)?;
        Ok(())
    }

    fn list_all_unlocked(&self) -> Result<Vec<PendingRemoteTurn>, Error> {
        let mut out = Vec::new();
        let Ok(entries) = fs::read_dir(&self.root) else {
            return Ok(out);
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            if let Ok(raw) = fs::read(&path)
                && let Ok(turn) = serde_json::from_slice::<PendingRemoteTurn>(&raw)
            {
                out.push(turn);
            }
        }
        Ok(out)
    }
}

impl RemoteTurnStore for FsRemoteTurnStore {
    fn create_turn(&self, input: &NewRemoteTurn) -> Result<PendingRemoteTurn, Error> {
        let _lock = self.guard.lock().unwrap_or_else(|e| e.into_inner());
        let turn_id = validate_turn_id(&input.turn_id)?.to_string();
        let status = validate_status(&input.status)?.to_string();
        if self.read_unlocked(&turn_id)?.is_some() {
            return Err(Error::Message(format!(
                "remote turn `{turn_id}` already exists"
            )));
        }
        if !is_terminal_remote_turn_status(&status)
            && self
                .list_all_unlocked()?
                .iter()
                .any(|t| t.agent_id == input.agent_id && !t.is_terminal())
        {
            return Err(Error::TurnAlreadyActive(input.agent_id.to_string()));
        }
        let now = now_ms();
        let turn = PendingRemoteTurn {
            turn_id,
            agent_id: input.agent_id.clone(),
            harness_kind: sanitize_pg_text(&input.harness_kind),
            provider_kind: input.provider_kind.as_deref().map(sanitize_pg_text),
            remote_run_id: input.remote_run_id.as_deref().map(sanitize_pg_text),
            callback_token_hash: input.callback_token_hash.clone(),
            cursor: input.cursor.as_deref().map(sanitize_pg_text),
            status,
            result_text: None,
            error_text: None,
            usage_json: None,
            metadata_json: input.metadata_json.as_ref().map(sanitize_pg_json),
            created_at_ms: now,
            updated_at_ms: now,
            last_event_at_ms: now,
            next_poll_at_ms: input.next_poll_at_ms,
        };
        self.write_unlocked(&turn)?;
        Ok(turn)
    }

    fn get_turn(&self, turn_id: &str) -> Result<Option<PendingRemoteTurn>, Error> {
        let _lock = self.guard.lock().unwrap_or_else(|e| e.into_inner());
        if validate_turn_id(turn_id).is_err() {
            return Ok(None);
        }
        self.read_unlocked(turn_id)
    }

    fn find_by_remote_run_id(
        &self,
        harness_kind: Option<&str>,
        remote_run_id: &str,
    ) -> Result<Option<PendingRemoteTurn>, Error> {
        let _lock = self.guard.lock().unwrap_or_else(|e| e.into_inner());
        let trimmed = remote_run_id.trim();
        if trimmed.is_empty() {
            return Ok(None);
        }
        let kind = harness_kind.map(str::trim).filter(|s| !s.is_empty());
        let mut matches: Vec<PendingRemoteTurn> = self
            .list_all_unlocked()?
            .into_iter()
            .filter(|t| {
                t.remote_run_id.as_deref() == Some(trimmed)
                    && kind.is_none_or(|hk| t.harness_kind == hk)
            })
            .collect();
        matches.sort_by_key(|t| std::cmp::Reverse(t.created_at_ms));
        Ok(matches.into_iter().next())
    }

    fn verify_callback_token(
        &self,
        turn_id: &str,
        bearer_token: &str,
    ) -> Result<Option<PendingRemoteTurn>, Error> {
        let Some(turn) = self.get_turn(turn_id)? else {
            return Ok(None);
        };
        if verify_callback_token_hash(&turn.callback_token_hash, bearer_token) {
            Ok(Some(turn))
        } else {
            Ok(None)
        }
    }

    fn update_remote_run(
        &self,
        turn_id: &str,
        remote_run_id: Option<&str>,
        status: Option<&str>,
        next_poll_at_ms: Option<Option<i64>>,
        metadata_json: Option<&Value>,
    ) -> Result<Option<PendingRemoteTurn>, Error> {
        let _lock = self.guard.lock().unwrap_or_else(|e| e.into_inner());
        let Some(mut turn) = self.read_unlocked(turn_id)? else {
            return Ok(None);
        };
        if let Some(run_id) = remote_run_id {
            turn.remote_run_id = Some(sanitize_pg_text(run_id));
        }
        if !turn.is_terminal() {
            if let Some(st) = status {
                turn.status = validate_status(st)?.to_string();
            }
            if let Some(np) = next_poll_at_ms {
                turn.next_poll_at_ms = np;
            }
        }
        if let Some(meta) = metadata_json {
            turn.metadata_json = Some(sanitize_pg_json(meta));
        }
        turn.updated_at_ms = turn.updated_at_ms.max(now_ms());
        self.write_unlocked(&turn)?;
        Ok(Some(turn))
    }

    fn record_event(
        &self,
        turn_id: &str,
        update: &RemoteTurnEventUpdate,
    ) -> Result<Option<PendingRemoteTurn>, Error> {
        let _lock = self.guard.lock().unwrap_or_else(|e| e.into_inner());
        let Some(mut turn) = self.read_unlocked(turn_id)? else {
            return Ok(None);
        };
        let next_status = match update.status.as_deref() {
            Some(s) => {
                let valid = validate_status(s)?;
                if is_terminal_remote_turn_status(valid) {
                    return Err(Error::Message(
                        "record_event cannot write terminal status; use settle_turn".into(),
                    ));
                }
                valid
            }
            None => "running",
        };
        let event_ts = if update.last_event_at_ms > 0 {
            update.last_event_at_ms
        } else {
            now_ms()
        };
        if !turn.is_terminal() {
            turn.status = next_status.to_string();
            if let Some(np) = update.next_poll_at_ms {
                turn.next_poll_at_ms = np;
            }
        }
        if let Some(ref cur) = update.cursor {
            turn.cursor = Some(sanitize_pg_text(cur));
        }
        if let Some(ref rid) = update.remote_run_id {
            turn.remote_run_id = Some(sanitize_pg_text(rid));
        }
        turn.last_event_at_ms = turn.last_event_at_ms.max(event_ts);
        turn.updated_at_ms = turn.updated_at_ms.max(now_ms().max(event_ts));
        self.write_unlocked(&turn)?;
        Ok(Some(turn))
    }

    fn settle_turn(
        &self,
        turn_id: &str,
        settlement: &SettleRemoteTurn,
    ) -> Result<Option<SettleOutcome>, Error> {
        let _lock = self.guard.lock().unwrap_or_else(|e| e.into_inner());
        let terminal = validate_terminal_status(&settlement.status)?;
        let Some(mut turn) = self.read_unlocked(turn_id)? else {
            return Ok(None);
        };
        if turn.is_terminal() {
            return Ok(Some(SettleOutcome {
                turn,
                transitioned: false,
            }));
        }
        let settled_at = if settlement.settled_at_ms > 0 {
            settlement.settled_at_ms
        } else {
            now_ms()
        };
        turn.status = terminal.to_string();
        turn.result_text = settlement.result_text.as_deref().map(sanitize_pg_text);
        turn.error_text = settlement.error_text.as_deref().map(sanitize_pg_text);
        turn.usage_json = settlement.usage_json.as_ref().map(sanitize_pg_json);
        if let Some(ref cur) = settlement.cursor {
            turn.cursor = Some(sanitize_pg_text(cur));
        }
        turn.last_event_at_ms = turn.last_event_at_ms.max(settled_at);
        turn.updated_at_ms = turn.updated_at_ms.max(now_ms().max(settled_at));
        turn.next_poll_at_ms = None;
        self.write_unlocked(&turn)?;
        Ok(Some(SettleOutcome {
            turn,
            transitioned: true,
        }))
    }

    fn list_due_polls(&self, now: i64, limit: usize) -> Result<Vec<PendingRemoteTurn>, Error> {
        let _lock = self.guard.lock().unwrap_or_else(|e| e.into_inner());
        let mut due: Vec<PendingRemoteTurn> = self
            .list_all_unlocked()?
            .into_iter()
            .filter(|t| !t.is_terminal() && t.next_poll_at_ms.is_some_and(|next_at| next_at <= now))
            .collect();
        due.sort_by_key(|t| (t.next_poll_at_ms.unwrap_or(i64::MAX), t.created_at_ms));
        due.truncate(limit.max(1));
        Ok(due)
    }

    fn list_active_for_agent(&self, agent_id: &AgentId) -> Result<Vec<PendingRemoteTurn>, Error> {
        let _lock = self.guard.lock().unwrap_or_else(|e| e.into_inner());
        let mut active: Vec<PendingRemoteTurn> = self
            .list_all_unlocked()?
            .into_iter()
            .filter(|t| &t.agent_id == agent_id && !t.is_terminal())
            .collect();
        active.sort_by_key(|t| t.created_at_ms);
        Ok(active)
    }

    fn list_stale_active(
        &self,
        harness_kind: Option<&str>,
        stale_before_ms: i64,
        limit: usize,
    ) -> Result<Vec<PendingRemoteTurn>, Error> {
        let _lock = self.guard.lock().unwrap_or_else(|e| e.into_inner());
        let kind = harness_kind.map(str::trim).filter(|s| !s.is_empty());
        let mut stale: Vec<PendingRemoteTurn> = self
            .list_all_unlocked()?
            .into_iter()
            .filter(|t| {
                !t.is_terminal()
                    && kind.is_none_or(|hk| t.harness_kind == hk)
                    && t.last_event_at_ms <= stale_before_ms
            })
            .collect();
        stale.sort_by_key(|t| t.last_event_at_ms);
        stale.truncate(limit.max(1));
        Ok(stale)
    }

    fn schedule_next_poll(
        &self,
        turn_id: &str,
        next_poll_at_ms: Option<i64>,
        cursor: Option<&str>,
        now: i64,
    ) -> Result<bool, Error> {
        let _lock = self.guard.lock().unwrap_or_else(|e| e.into_inner());
        let Some(mut turn) = self.read_unlocked(turn_id)? else {
            return Ok(false);
        };
        if turn.is_terminal() {
            return Ok(false);
        }
        turn.next_poll_at_ms = next_poll_at_ms;
        if let Some(cur) = cursor {
            turn.cursor = Some(sanitize_pg_text(cur));
        }
        turn.last_event_at_ms = turn.last_event_at_ms.max(now);
        turn.updated_at_ms = turn.updated_at_ms.max(now);
        self.write_unlocked(&turn)?;
        Ok(true)
    }

    fn delete_turn(&self, turn_id: &str) -> Result<bool, Error> {
        let _lock = self.guard.lock().unwrap_or_else(|e| e.into_inner());
        let path = self.turn_path(turn_id)?;
        match fs::remove_file(path) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    fn list_active_fresh_by_agent(
        &self,
        fresh_since_ms: i64,
    ) -> Result<HashMap<String, i64>, Error> {
        let _lock = self.guard.lock().unwrap_or_else(|e| e.into_inner());
        let all = self.list_all_unlocked()?;
        Ok(super::remote_turns_ext::collect_active_fresh_by_agent(
            all.iter(),
            fresh_since_ms,
        ))
    }

    fn latest_failed_for_agent(
        &self,
        agent_id: &AgentId,
    ) -> Result<Option<super::remote_turns_ext::LatestRemoteTurnError>, Error> {
        let _lock = self.guard.lock().unwrap_or_else(|e| e.into_inner());
        let all = self.list_all_unlocked()?;
        Ok(super::remote_turns_ext::latest_failed_for_agent_from_iter(
            all.iter(),
            agent_id,
        ))
    }

    fn list_latest_failed_by_agent(
        &self,
    ) -> Result<HashMap<String, super::remote_turns_ext::LatestRemoteTurnError>, Error> {
        let _lock = self.guard.lock().unwrap_or_else(|e| e.into_inner());
        let all = self.list_all_unlocked()?;
        Ok(super::remote_turns_ext::collect_latest_failed_by_agent(
            all.iter(),
        ))
    }

    fn record_agent_inbox_read_seq(
        &self,
        agent_id: &AgentId,
        max_read_seq: i64,
        now: i64,
    ) -> Result<usize, Error> {
        let _lock = self.guard.lock().unwrap_or_else(|e| e.into_inner());
        let mut updated = 0usize;
        for mut turn in self.list_all_unlocked()? {
            if &turn.agent_id == agent_id
                && super::remote_turns_ext::apply_inbox_read_seq_to_turn(
                    &mut turn,
                    max_read_seq,
                    now,
                )
            {
                self.write_unlocked(&turn)?;
                updated += 1;
            }
        }
        Ok(updated)
    }
}

pub use super::remote_turns_ext::InMemoryRemoteTurnStore;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn callback_token_hashing_and_hmac_verification_are_constant_time_and_fail_closed() {
        let tok1 = generate_callback_token();
        let tok2 = generate_callback_token();
        assert_ne!(tok1, tok2);
        assert!(tok1.starts_with("rtk_"));
        let hash1 = hash_callback_token(&tok1);
        assert_eq!(hash1.len(), 64);

        assert!(verify_callback_token_hash(&hash1, &tok1));
        assert!(!verify_callback_token_hash(&hash1, &tok2));
        assert!(!verify_callback_token_hash(&hash1, ""));
        assert!(!verify_callback_token_hash("short", &tok1));

        let secret = b"webhook-signing-secret-123";
        let payload = br#"{"event":"turn.completed","id":"run-1"}"#;
        let sig = hmac_sha256_hex(secret, payload);
        assert!(verify_hmac_sha256_signature(
            secret,
            payload,
            &format!("sha256={sig}")
        ));
        assert!(verify_hmac_sha256_signature(secret, payload, &sig));
        assert!(!verify_hmac_sha256_signature(
            secret,
            b"tampered",
            &format!("sha256={sig}")
        ));
        assert!(!verify_hmac_sha256_signature(
            b"",
            payload,
            &format!("sha256={sig}")
        ));
    }

    #[test]
    fn fs_remote_turn_store_enforces_token_auth_idempotent_settle_and_due_polls() {
        let dir = crate::test_tempdir();
        let store = FsRemoteTurnStore::new(dir.path());
        let agent_id = AgentId::generate();
        let tok = generate_callback_token();
        let tok_hash = hash_callback_token(&tok);
        let now = now_ms();

        let created = store
            .create_turn(&NewRemoteTurn {
                turn_id: "turn_fs_1".into(),
                agent_id: agent_id.clone(),
                harness_kind: "spiron".into(),
                provider_kind: Some("spiron".into()),
                remote_run_id: Some("task-123".into()),
                callback_token_hash: tok_hash,
                cursor: None,
                status: "dispatched".into(),
                metadata_json: Some(json!({"remote_agent_id": "agt-remote"})),
                next_poll_at_ms: Some(now - 100),
            })
            .unwrap();
        assert_eq!(created.status, "dispatched");

        // Constant-time token verification succeeds for valid token and fails closed otherwise.
        assert!(
            store
                .verify_callback_token("turn_fs_1", &tok)
                .unwrap()
                .is_some()
        );
        assert!(
            store
                .verify_callback_token("turn_fs_1", "wrong-token")
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .verify_callback_token("unknown_turn", &tok)
                .unwrap()
                .is_none()
        );

        // Due poll query picks up the turn.
        let due = store.list_due_polls(now, 10).unwrap();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].turn_id, "turn_fs_1");

        // Incremental event advances dispatched -> running and sets cursor.
        let updated = store
            .record_event(
                "turn_fs_1",
                &RemoteTurnEventUpdate {
                    status: Some("running".into()),
                    cursor: Some("ev-5".into()),
                    remote_run_id: None,
                    last_event_at_ms: now + 50,
                    next_poll_at_ms: Some(Some(now + 5_000)),
                },
            )
            .unwrap()
            .unwrap();
        assert_eq!(updated.status, "running");
        assert_eq!(updated.cursor.as_deref(), Some("ev-5"));

        // First settlement transitions and sanitizes embedded NUL bytes.
        let first_settle = store
            .settle_turn(
                "turn_fs_1",
                &SettleRemoteTurn {
                    status: "completed".into(),
                    result_text: Some("ok\0clean".into()),
                    error_text: None,
                    usage_json: Some(json!({"input_tokens": 12, "output_tokens": 5})),
                    cursor: Some("ev-9".into()),
                    settled_at_ms: now + 100,
                },
            )
            .unwrap()
            .unwrap();
        assert!(first_settle.transitioned);
        assert_eq!(first_settle.turn.status, "completed");
        assert_eq!(
            first_settle.turn.result_text.as_deref(),
            Some("ok\u{FFFD}clean")
        );
        assert!(first_settle.turn.next_poll_at_ms.is_none());

        // Second settlement (e.g. duplicate webhook or racing tick) is a strict no-op!
        let replay = store
            .settle_turn(
                "turn_fs_1",
                &SettleRemoteTurn {
                    status: "failed".into(),
                    result_text: None,
                    error_text: Some("late timeout".into()),
                    usage_json: None,
                    cursor: None,
                    settled_at_ms: now + 500,
                },
            )
            .unwrap()
            .unwrap();
        assert!(!replay.transitioned);
        assert_eq!(replay.turn.status, "completed");
        assert_eq!(replay.turn.result_text.as_deref(), Some("ok\u{FFFD}clean"));
        assert!(replay.turn.error_text.is_none());

        // Settled turn no longer appears in due polls or active turns.
        assert!(store.list_due_polls(now + 100_000, 10).unwrap().is_empty());
        assert!(store.list_active_for_agent(&agent_id).unwrap().is_empty());
    }

    #[test]
    fn create_turn_fails_closed_on_duplicate_active_agent_turn_and_recovers_after_settle() {
        let dir = crate::test_tempdir();
        let fs_store = Arc::new(FsRemoteTurnStore::new(dir.path()));
        let mem_store = Arc::new(InMemoryRemoteTurnStore::new());
        let mk_input = |turn_id: &str, agent_id: &AgentId, status: &str| NewRemoteTurn {
            turn_id: turn_id.to_string(),
            agent_id: agent_id.clone(),
            harness_kind: "sandbox".to_string(),
            provider_kind: Some("http".to_string()),
            remote_run_id: None,
            callback_token_hash: hash_callback_token(&format!("tok-{turn_id}")),
            cursor: None,
            status: status.to_string(),
            metadata_json: None,
            next_poll_at_ms: None,
        };

        for store in [
            Arc::clone(&fs_store) as Arc<dyn RemoteTurnStore>,
            Arc::clone(&mem_store) as Arc<dyn RemoteTurnStore>,
        ] {
            let aid = AgentId::generate();
            let other = AgentId::generate();

            // Concurrent race across 8 threads: strictly one active turn wins.
            let barrier = Arc::new(std::sync::Barrier::new(8));
            let mut handles = Vec::new();
            for i in 0..8usize {
                let s = Arc::clone(&store);
                let b = Arc::clone(&barrier);
                let a = aid.clone();
                let st = if i.is_multiple_of(2) {
                    "dispatched"
                } else {
                    "running"
                };
                handles.push(std::thread::spawn(move || {
                    b.wait();
                    s.create_turn(&mk_input(&format!("turn_race_{i}"), &a, st))
                }));
            }
            let mut winners = Vec::new();
            let mut conflicts = 0usize;
            for h in handles {
                match h.join().unwrap() {
                    Ok(t) => winners.push(t.turn_id),
                    Err(err) => {
                        assert!(err.is_active_turn_conflict(), "unexpected error: {err:?}");
                        assert!(matches!(err, Error::TurnAlreadyActive(_)));
                        conflicts += 1;
                    }
                }
            }
            assert_eq!(winners.len(), 1);
            assert_eq!(conflicts, 7);
            assert_eq!(store.list_active_for_agent(&aid).unwrap().len(), 1);

            // Different agent can still create an active turn concurrently.
            store
                .create_turn(&mk_input("turn_other_agent", &other, "running"))
                .unwrap();

            // Back-to-back active turn for same agent fails closed while winner is active.
            let dup = store
                .create_turn(&mk_input("turn_dup_after_race", &aid, "running"))
                .unwrap_err();
            assert!(dup.is_active_turn_conflict());

            // Once settled (completed / failed / cancelled), a new active turn succeeds.
            for (idx, terminal) in ["completed", "failed", "cancelled"].iter().enumerate() {
                let current_id = if idx == 0 {
                    winners[0].clone()
                } else {
                    format!("turn_after_{idx}")
                };
                let settled = store
                    .settle_turn(
                        &current_id,
                        &SettleRemoteTurn {
                            status: (*terminal).to_string(),
                            result_text: None,
                            error_text: None,
                            usage_json: None,
                            cursor: None,
                            settled_at_ms: now_ms(),
                        },
                    )
                    .unwrap()
                    .unwrap();
                assert!(settled.transitioned);

                let next_id = format!("turn_after_{}", idx + 1);
                let next = store
                    .create_turn(&mk_input(&next_id, &aid, "dispatched"))
                    .expect("new active turn must succeed once previous is settled");
                assert_eq!(next.turn_id, next_id);
            }
        }
    }
}
