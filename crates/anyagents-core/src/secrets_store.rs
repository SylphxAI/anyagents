//! Named encrypted deployment secrets and per-agent secret grant storage.
//!
//! Secret environment bundles (`deployment_secrets`) are encrypted at rest with
//! [`MasterKey`] (`ANYAGENTS_MASTER_KEY`, domain-separated AEAD bound to the
//! secret bundle name as AAD). Metadata queries (`list_deployment_secrets`,
//! `get_deployment_secret_metadata`, `list_agent_secrets`) never read or decrypt
//! ciphertext columns and never expose plaintext values.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde::{Deserialize, Serialize};

use crate::db::{DbExecExt as _, db_params};
use crate::error::Error;
use crate::id::AgentId;
use crate::migrate::now_ms;
use crate::stateless::crypto::{MASTER_KEY_ENV, MasterKey};
use crate::store::Store;

const CIPHERTEXT_PREFIX: &str = "aaenc1:";
const MAX_SECRET_NAME_LEN: usize = 64;
const MAX_ENV_KEY_LEN: usize = 256;

/// Write-only metadata for a named encrypted deployment secret bundle.
/// Intentionally has no field for secret values.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeploymentSecretMetadata {
    pub name: String,
    pub env_keys: Vec<String>,
    pub updated_at_ms: i64,
    pub granted_agent_ids: Vec<String>,
}

/// Validate a deployment secret bundle name (`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}$`).
pub fn validate_secret_name(name: &str) -> Result<(), Error> {
    let bytes = name.as_bytes();
    if bytes.is_empty() || bytes.len() > MAX_SECRET_NAME_LEN {
        return Err(Error::Message(format!(
            "invalid secret name `{name}`: must be 1..={MAX_SECRET_NAME_LEN} characters matching ^[a-zA-Z0-9][a-zA-Z0-9._-]{{0,63}}$"
        )));
    }
    if !bytes[0].is_ascii_alphanumeric()
        || !bytes[1..]
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
    {
        return Err(Error::Message(format!(
            "invalid secret name `{name}`: must match ^[a-zA-Z0-9][a-zA-Z0-9._-]{{0,63}}$"
        )));
    }
    Ok(())
}

/// Returns `true` for reserved internal coordinator and sandbox runtime env keys
/// that deployment secrets must never define or override.
pub fn is_reserved_secret_env_key(key: &str) -> bool {
    let upper = key.to_ascii_uppercase();
    upper.starts_with("ANYAGENTS_")
        || matches!(
            upper.as_str(),
            "HOME" | "XDG_CONFIG_HOME" | "XDG_DATA_HOME" | "XDG_CACHE_HOME" | "XDG_STATE_HOME"
        )
}

/// Validate an environment variable name inside a deployment secret bundle
/// (`^[A-Za-z_][A-Za-z0-9_]*$`, rejecting reserved coordinator/runtime keys).
pub fn validate_secret_env_key(key: &str) -> Result<(), Error> {
    let bytes = key.as_bytes();
    if bytes.is_empty() || bytes.len() > MAX_ENV_KEY_LEN {
        return Err(Error::Message(format!(
            "invalid secret env var name `{key}`: must match ^[A-Za-z_][A-Za-z0-9_]*$"
        )));
    }
    if !(bytes[0].is_ascii_alphabetic() || bytes[0] == b'_')
        || !bytes[1..]
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || *b == b'_')
    {
        return Err(Error::Message(format!(
            "invalid secret env var name `{key}`: must match ^[A-Za-z_][A-Za-z0-9_]*$"
        )));
    }
    if is_reserved_secret_env_key(key) {
        return Err(Error::Message(format!(
            "env var `{key}` is reserved for internal coordinator or sandbox runtime use"
        )));
    }
    Ok(())
}

/// Load `ANYAGENTS_MASTER_KEY` strictly, failing closed when unset or malformed.
pub fn require_deployment_master_key() -> Result<MasterKey, Error> {
    let raw = std::env::var(MASTER_KEY_ENV).map_err(|_| Error::Precondition {
        machine: "secrets",
        reason: format!("encryption key required: set {MASTER_KEY_ENV} (at least 16 characters)"),
    })?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(Error::Precondition {
            machine: "secrets",
            reason: format!(
                "encryption key required: set {MASTER_KEY_ENV} (at least 16 characters)"
            ),
        });
    }
    MasterKey::from_secret_str(trimmed).map_err(|err| Error::Precondition {
        machine: "secrets",
        reason: err.to_string(),
    })
}

fn secret_aad(name: &str) -> String {
    format!("deployment_secrets:v1:{name}")
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

fn hex_decode(input: &str) -> Result<Vec<u8>, Error> {
    let bytes = input.as_bytes();
    if !bytes.len().is_multiple_of(2) {
        return Err(Error::Message(
            "corrupted secret ciphertext encoding (odd length)".into(),
        ));
    }
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.as_chunks::<2>().0 {
        let hi = hex_nibble(pair[0])?;
        let lo = hex_nibble(pair[1])?;
        out.push((hi << 4) | lo);
    }
    Ok(out)
}

fn hex_nibble(b: u8) -> Result<u8, Error> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => Err(Error::Message(
            "corrupted secret ciphertext encoding (non-hex byte)".into(),
        )),
    }
}

fn seal_env_entries(
    key: &MasterKey,
    name: &str,
    env: &BTreeMap<String, String>,
) -> Result<String, Error> {
    let plaintext = serde_json::to_vec(env)
        .map_err(|err| Error::Message(format!("failed to serialize secret entries: {err}")))?;
    let aad = secret_aad(name);
    let sealed = key.encrypt(aad.as_bytes(), &plaintext)?;
    Ok(format!("{CIPHERTEXT_PREFIX}{}", hex_encode(&sealed)))
}

fn open_env_entries(
    key: &MasterKey,
    name: &str,
    encoded: &str,
) -> Result<BTreeMap<String, String>, Error> {
    let hex_part = encoded.strip_prefix(CIPHERTEXT_PREFIX).ok_or_else(|| {
        Error::Message(format!(
            "secret `{name}` ciphertext has unsupported envelope prefix"
        ))
    })?;
    let blob = hex_decode(hex_part)?;
    let aad = secret_aad(name);
    let plaintext = key.decrypt(aad.as_bytes(), &blob)?;
    serde_json::from_slice(&plaintext).map_err(|err| {
        Error::Message(format!(
            "failed to decode decrypted secret `{name}` payload: {err}"
        ))
    })
}

fn normalize_secret_names(names: &[String]) -> Result<Vec<String>, Error> {
    let mut unique = BTreeSet::new();
    for raw in names {
        let trimmed = raw.trim();
        validate_secret_name(trimmed)?;
        unique.insert(trimmed.to_string());
    }
    Ok(unique.into_iter().collect())
}

impl Store {
    /// Encrypt and upsert a named deployment secret bundle. Returns metadata
    /// ONLY (never plaintext values). Fails closed if `ANYAGENTS_MASTER_KEY` is unset.
    pub fn upsert_deployment_secret(
        &self,
        name: &str,
        env: &BTreeMap<String, String>,
    ) -> Result<DeploymentSecretMetadata, Error> {
        validate_secret_name(name)?;
        if env.is_empty() {
            return Err(Error::Message(
                "secret bundle must contain at least one environment variable".into(),
            ));
        }
        for (k, v) in env {
            validate_secret_env_key(k)?;
            if v.contains('\0') {
                return Err(Error::Message(format!(
                    "secret env var `{k}` value cannot contain NUL bytes"
                )));
            }
        }
        let key = require_deployment_master_key()?;
        let ciphertext = seal_env_entries(&key, name, env)?;
        let env_keys: Vec<String> = env.keys().cloned().collect();
        let key_names_json = serde_json::to_string(&env_keys)
            .map_err(|err| Error::Message(format!("failed to serialize key names: {err}")))?;
        let updated_at_ms = now_ms();

        self.write_tx(|tx| {
            tx.db_execute(
                "INSERT INTO deployment_secrets (name, env_entries_ciphertext, key_names_json, updated_at_ms)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(name) DO UPDATE SET
                   env_entries_ciphertext = excluded.env_entries_ciphertext,
                   key_names_json = excluded.key_names_json,
                   updated_at_ms = excluded.updated_at_ms",
                &db_params![name, ciphertext, key_names_json, updated_at_ms],
            )?;
            let granted_agent_ids = tx.db_query_vec(
                "SELECT agent_id FROM agent_secret_grants WHERE secret_name = ?1 ORDER BY agent_id",
                &db_params![name],
                |row| row.get::<String>(0),
            )?;
            Ok(DeploymentSecretMetadata {
                name: name.to_string(),
                env_keys,
                updated_at_ms,
                granted_agent_ids,
            })
        })
    }

    /// List metadata for every deployment secret bundle, ordered by name.
    /// Never reads ciphertext or exposes plaintext values.
    pub fn list_deployment_secrets(&self) -> Result<Vec<DeploymentSecretMetadata>, Error> {
        let conn = self.read_conn();
        let rows = conn.db_query_vec(
            "SELECT name, key_names_json, updated_at_ms
             FROM deployment_secrets
             ORDER BY name",
            &[],
            |row| {
                let name: String = row.get(0)?;
                let env_keys: Vec<String> = row.json(1)?;
                let updated_at_ms: i64 = row.get(2)?;
                Ok((name, env_keys, updated_at_ms))
            },
        )?;
        let mut grants_by_secret: HashMap<String, Vec<String>> = HashMap::new();
        for (secret_name, agent_id) in conn.db_query_vec(
            "SELECT secret_name, agent_id
             FROM agent_secret_grants
             ORDER BY secret_name, agent_id",
            &[],
            |row| Ok((row.get::<String>(0)?, row.get::<String>(1)?)),
        )? {
            grants_by_secret
                .entry(secret_name)
                .or_default()
                .push(agent_id);
        }
        Ok(rows
            .into_iter()
            .map(|(name, env_keys, updated_at_ms)| {
                let granted_agent_ids = grants_by_secret.remove(&name).unwrap_or_default();
                DeploymentSecretMetadata {
                    name,
                    env_keys,
                    updated_at_ms,
                    granted_agent_ids,
                }
            })
            .collect())
    }

    /// Read metadata ONLY for one named deployment secret bundle.
    pub fn get_deployment_secret_metadata(
        &self,
        name: &str,
    ) -> Result<Option<DeploymentSecretMetadata>, Error> {
        validate_secret_name(name)?;
        let conn = self.read_conn();
        let Some((name, env_keys, updated_at_ms)) = conn.db_query_opt(
            "SELECT name, key_names_json, updated_at_ms
             FROM deployment_secrets
             WHERE name = ?1",
            &db_params![name],
            |row| {
                let name: String = row.get(0)?;
                let env_keys: Vec<String> = row.json(1)?;
                let updated_at_ms: i64 = row.get(2)?;
                Ok((name, env_keys, updated_at_ms))
            },
        )?
        else {
            return Ok(None);
        };
        let granted_agent_ids = conn.db_query_vec(
            "SELECT agent_id FROM agent_secret_grants WHERE secret_name = ?1 ORDER BY agent_id",
            &db_params![name],
            |row| row.get::<String>(0),
        )?;
        Ok(Some(DeploymentSecretMetadata {
            name,
            env_keys,
            updated_at_ms,
            granted_agent_ids,
        }))
    }

    /// Delete a deployment secret and cascade deletion from `agent_secret_grants`.
    /// Returns `true` if the secret existed and was deleted.
    pub fn delete_deployment_secret(&self, name: &str) -> Result<bool, Error> {
        validate_secret_name(name)?;
        self.write_tx(|tx| {
            tx.db_execute(
                "DELETE FROM agent_secret_grants WHERE secret_name = ?1",
                &db_params![name],
            )?;
            let deleted = tx.db_execute(
                "DELETE FROM deployment_secrets WHERE name = ?1",
                &db_params![name],
            )?;
            Ok(deleted > 0)
        })
    }

    /// List sorted secret bundle names granted to `agent_id` (names ONLY, never values).
    pub fn list_agent_secrets(&self, agent_id: &AgentId) -> Result<Vec<String>, Error> {
        let conn = self.read_conn();
        conn.db_query_vec(
            "SELECT secret_name
             FROM agent_secret_grants
             WHERE agent_id = ?1
             ORDER BY secret_name",
            &db_params![agent_id.as_str()],
            |row| row.get::<String>(0),
        )
    }

    /// Load all agent secret grants in a single query (`agent_id -> sorted secret names`).
    pub fn all_agent_secret_grants(&self) -> Result<HashMap<String, Vec<String>>, Error> {
        let conn = self.read_conn();
        let mut map: HashMap<String, Vec<String>> = HashMap::new();
        for (agent_id, secret_name) in conn.db_query_vec(
            "SELECT agent_id, secret_name
             FROM agent_secret_grants
             ORDER BY agent_id, secret_name",
            &[],
            |row| Ok((row.get::<String>(0)?, row.get::<String>(1)?)),
        )? {
            map.entry(agent_id).or_default().push(secret_name);
        }
        Ok(map)
    }

    /// Verify every secret name is valid and exists in `deployment_secrets`.
    pub fn check_deployment_secrets_exist(&self, secret_names: &[String]) -> Result<(), Error> {
        let normalized = normalize_secret_names(secret_names)?;
        if normalized.is_empty() {
            return Ok(());
        }
        let conn = self.read_conn();
        for name in &normalized {
            let exists: Option<i64> = conn.db_query_opt(
                "SELECT 1 FROM deployment_secrets WHERE name = ?1",
                &db_params![name],
                |row| row.get(0),
            )?;
            if exists.is_none() {
                return Err(Error::Message(format!(
                    "unknown deployment secret `{name}`"
                )));
            }
        }
        Ok(())
    }

    /// Atomically replace the set of secret bundles granted to `agent_id`.
    /// Rejects non-existent `deployment_secrets` names. Authorization belongs to the caller.
    pub fn set_agent_secrets(
        &self,
        agent_id: &AgentId,
        secret_names: &[String],
        granted_by: Option<&str>,
    ) -> Result<Vec<String>, Error> {
        let desired = normalize_secret_names(secret_names)?;
        let now = now_ms();
        self.write_tx(|tx| {
            let agent_exists: Option<i64> = tx.db_query_opt(
                "SELECT 1 FROM agents WHERE id = ?1",
                &db_params![agent_id.as_str()],
                |row| row.get(0),
            )?;
            if agent_exists.is_none() {
                return Err(Error::AgentNotFound(agent_id.to_string()));
            }
            for name in &desired {
                let exists: Option<i64> = tx.db_query_opt(
                    "SELECT 1 FROM deployment_secrets WHERE name = ?1",
                    &db_params![name],
                    |row| row.get(0),
                )?;
                if exists.is_none() {
                    return Err(Error::Message(format!(
                        "unknown deployment secret `{name}`"
                    )));
                }
            }
            let existing: BTreeSet<String> = tx
                .db_query_vec(
                    "SELECT secret_name FROM agent_secret_grants WHERE agent_id = ?1",
                    &db_params![agent_id.as_str()],
                    |row| row.get::<String>(0),
                )?
                .into_iter()
                .collect();
            let desired_set: BTreeSet<String> = desired.iter().cloned().collect();
            for removed in existing.difference(&desired_set) {
                tx.db_execute(
                    "DELETE FROM agent_secret_grants WHERE agent_id = ?1 AND secret_name = ?2",
                    &db_params![agent_id.as_str(), removed],
                )?;
            }
            for added in desired_set.difference(&existing) {
                tx.db_execute(
                    "INSERT INTO agent_secret_grants (agent_id, secret_name, granted_by, granted_at_ms)
                     VALUES (?1, ?2, ?3, ?4)
                     ON CONFLICT(agent_id, secret_name) DO NOTHING",
                    &db_params![agent_id.as_str(), added, granted_by, now],
                )?;
            }
            Ok(desired)
        })
    }

    /// Resolve and decrypt all secret bundles granted to `agent_id`, merged in
    /// deterministic ascending `secret_name` order.
    ///
    /// Returns an empty map without requiring `ANYAGENTS_MASTER_KEY` when the
    /// agent has no grants. Fails closed when grants exist and
    /// `ANYAGENTS_MASTER_KEY` is unset, invalid, or ciphertext fails MAC verification.
    pub fn resolve_agent_secret_env(
        &self,
        agent_id: &AgentId,
    ) -> Result<BTreeMap<String, String>, Error> {
        let rows = {
            let conn = self.read_conn();
            conn.db_query_vec(
                "SELECT s.name, s.env_entries_ciphertext
                 FROM agent_secret_grants g
                 JOIN deployment_secrets s ON s.name = g.secret_name
                 WHERE g.agent_id = ?1
                 ORDER BY s.name ASC",
                &db_params![agent_id.as_str()],
                |row| Ok((row.get::<String>(0)?, row.get::<String>(1)?)),
            )?
        };
        if rows.is_empty() {
            return Ok(BTreeMap::new());
        }
        let key = require_deployment_master_key()?;
        let mut merged = BTreeMap::new();
        for (name, ciphertext) in rows {
            let entries = open_env_entries(&key, &name, &ciphertext)?;
            for (k, v) in entries {
                if !is_reserved_secret_env_key(&k) {
                    merged.insert(k, v);
                }
            }
        }
        Ok(merged)
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::sync::Mutex;

    use super::*;
    use crate::NewAgent;

    static MASTER_KEY_LOCK: Mutex<()> = Mutex::new(());

    struct MasterKeyGuard(Option<OsString>);

    impl MasterKeyGuard {
        fn set(val: Option<&str>) -> Self {
            let prev = std::env::var_os(MASTER_KEY_ENV);
            match val {
                Some(v) => unsafe { std::env::set_var(MASTER_KEY_ENV, v) },
                None => unsafe { std::env::remove_var(MASTER_KEY_ENV) },
            }
            Self(prev)
        }
    }

    impl Drop for MasterKeyGuard {
        fn drop(&mut self) {
            match &self.0 {
                Some(v) => unsafe { std::env::set_var(MASTER_KEY_ENV, v) },
                None => unsafe { std::env::remove_var(MASTER_KEY_ENV) },
            }
        }
    }

    #[test]
    fn validates_secret_names_and_rejects_reserved_coordinator_env_keys() {
        for ok in ["github-ci", "prod.api_1", "A", &"a".repeat(64)] {
            assert!(validate_secret_name(ok).is_ok(), "expected valid: {ok}");
        }
        for bad in [
            "",
            "-leading-dash",
            ".leading-dot",
            "_leading_underscore",
            "has space",
            "bad/slash",
            &"a".repeat(65),
        ] {
            assert!(
                validate_secret_name(bad).is_err(),
                "expected invalid: {bad}"
            );
        }

        for ok in ["GH_TOKEN", "OPENAI_API_KEY", "_CUSTOM_123", "a"] {
            assert!(validate_secret_env_key(ok).is_ok(), "expected valid: {ok}");
        }
        for bad in [
            "",
            "1LEADING_DIGIT",
            "BAD-DASH",
            "BAD.DOT",
            "ANYAGENTS_MASTER_KEY",
            "ANYAGENTS_OPERATOR_TOKEN",
            "ANYAGENTS_CAPABILITY_KEY",
            "ANYAGENTS_DATABASE_URL",
            "ANYAGENTS_DIRECT_DATABASE_URL",
            "ANYAGENTS_TICK_TOKEN",
            "ANYAGENTS_ADMIN_SESSION_SECRET",
            "ANYAGENTS_ADMIN_OIDC_CLIENT_SECRET",
            "ANYAGENTS_S3_SECRET_ACCESS_KEY",
            "anyagents_custom_lower",
            "HOME",
        ] {
            assert!(
                validate_secret_env_key(bad).is_err(),
                "expected rejected: {bad}"
            );
        }
    }

    #[test]
    fn fails_closed_when_master_key_unset_and_encrypts_at_rest() {
        let _lock = MASTER_KEY_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = crate::test_tempdir();
        let db_path = dir.path().join("state.db");
        let store = Store::open(&db_path).unwrap();
        let agent = store
            .create_agent(NewAgent::new("builder", "claude-code"))
            .unwrap();

        // Without master key: upsert fails closed; resolving agent with no grants succeeds empty.
        {
            let _unset = MasterKeyGuard::set(None);
            let mut env = BTreeMap::new();
            env.insert("API_TOKEN".into(), "plaintext-canary-99887766".into());
            assert!(store.upsert_deployment_secret("ci-creds", &env).is_err());
            assert!(
                store
                    .resolve_agent_secret_env(&agent.id)
                    .unwrap()
                    .is_empty()
            );
        }

        // With master key: upsert encrypts at rest and metadata never leaks values.
        let _set = MasterKeyGuard::set(Some("test-master-key-material-0123456789abcdef"));
        let mut env_a = BTreeMap::new();
        env_a.insert("SHARED_VAR".into(), "from-a".into());
        env_a.insert("GH_TOKEN".into(), "secret-canary-gh-token-abc123".into());
        let meta = store.upsert_deployment_secret("a-first", &env_a).unwrap();
        assert_eq!(meta.name, "a-first");
        assert_eq!(meta.env_keys, vec!["GH_TOKEN", "SHARED_VAR"]);
        assert!(meta.granted_agent_ids.is_empty());

        // Inspect raw SQLite column directly to verify encryption at rest.
        let raw_conn = rusqlite::Connection::open(&db_path).unwrap();
        let raw_ct: String = raw_conn
            .query_row(
                "SELECT env_entries_ciphertext FROM deployment_secrets WHERE name = 'a-first'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(raw_ct.starts_with(CIPHERTEXT_PREFIX));
        assert!(!raw_ct.contains("secret-canary-gh-token-abc123"));
        assert!(!raw_ct.contains("GH_TOKEN"));

        // Second bundle overrides SHARED_VAR deterministically because "b-second" > "a-first".
        let mut env_b = BTreeMap::new();
        env_b.insert("SHARED_VAR".into(), "from-b".into());
        env_b.insert("STRIPE_KEY".into(), "secret-canary-stripe-xyz987".into());
        store.upsert_deployment_secret("b-second", &env_b).unwrap();

        // Unknown secret name rejected.
        assert!(
            store
                .set_agent_secrets(&agent.id, &["does-not-exist".into()], Some("operator"))
                .is_err()
        );

        let granted = store
            .set_agent_secrets(
                &agent.id,
                &["b-second".into(), "a-first".into(), "a-first".into()],
                Some("operator"),
            )
            .unwrap();
        assert_eq!(granted, vec!["a-first", "b-second"]);
        assert_eq!(
            store.list_agent_secrets(&agent.id).unwrap(),
            vec!["a-first", "b-second"]
        );

        let resolved = store.resolve_agent_secret_env(&agent.id).unwrap();
        assert_eq!(
            resolved.get("GH_TOKEN").map(String::as_str),
            Some("secret-canary-gh-token-abc123")
        );
        assert_eq!(
            resolved.get("STRIPE_KEY").map(String::as_str),
            Some("secret-canary-stripe-xyz987")
        );
        assert_eq!(
            resolved.get("SHARED_VAR").map(String::as_str),
            Some("from-b")
        );

        let listed = store.list_deployment_secrets().unwrap();
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0].granted_agent_ids, vec![agent.id.to_string()]);
        let listed_json = serde_json::to_string(&listed).unwrap();
        assert!(!listed_json.contains("secret-canary-gh-token-abc123"));
        assert!(!listed_json.contains("secret-canary-stripe-xyz987"));

        // Unsetting master key causes decrypt to fail closed while metadata listing still works.
        drop(_set);
        {
            let _unset = MasterKeyGuard::set(None);
            assert!(store.resolve_agent_secret_env(&agent.id).is_err());
            assert_eq!(store.list_deployment_secrets().unwrap().len(), 2);
        }

        // Deleting a secret cascades its grants.
        assert!(store.delete_deployment_secret("a-first").unwrap());
        assert!(!store.delete_deployment_secret("a-first").unwrap());
        assert_eq!(
            store.list_agent_secrets(&agent.id).unwrap(),
            vec!["b-second"]
        );
    }
}
