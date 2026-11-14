//! Remote session tokens: the credential a remote (cloud) session presents
//! when it calls back into this server's MCP endpoint.
//!
//! A remote session token is a separate kind from the local mailbox
//! capability. It names exactly one agent and one remote session, it expires
//! on its own, the server rotates it before it does, and pausing, archiving or
//! relaunching the agent revokes it. Only its SHA-256 digest is stored. The
//! digest is not keyed by the per-process capability key, so a token survives
//! a server restart until its own expiry, and a reader of the table learns
//! nothing it could present.

use std::io::Read as _;

use rusqlite::{OptionalExtension, params};
use sha2::{Digest as _, Sha256};

use super::{AgentId, Error, Store, agent_exists, now_ms};

/// Every remote session token starts with this, so a caller can tell the
/// kinds apart before any lookup and a secret scanner can recognise one.
pub const REMOTE_SESSION_TOKEN_PREFIX: &str = "aars_";
/// Every remote session id starts with this.
const SESSION_ID_PREFIX: &str = "rs_";
/// Random bytes in a token, written in hex after the prefix.
const TOKEN_BYTES: usize = 32;
/// Random bytes in a session id.
const SESSION_BYTES: usize = 16;

/// Whether `token` has the shape of a remote session token. Shape only: a
/// token that has it is authenticated by [`Store::authenticate_remote_session`]
/// and by nothing else.
pub fn is_remote_session_token(token: &str) -> bool {
    token
        .strip_prefix(REMOTE_SESSION_TOKEN_PREFIX)
        .is_some_and(|rest| {
            rest.len() == TOKEN_BYTES * 2 && rest.bytes().all(|b| b.is_ascii_hexdigit())
        })
}

/// A newly issued token. The value is returned once, to be written where the
/// remote session reads it; it is never stored and never printed.
#[derive(Clone, PartialEq, Eq)]
pub struct RemoteSessionToken {
    pub token: String,
    pub session_id: String,
    pub expires_at_ms: i64,
}

impl std::fmt::Debug for RemoteSessionToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteSessionToken")
            .field("token", &"<redacted>")
            .field("session_id", &self.session_id)
            .field("expires_at_ms", &self.expires_at_ms)
            .finish()
    }
}

/// What one presented remote session token authenticates.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteSessionGrant {
    pub agent_id: AgentId,
    pub session_id: String,
    pub expires_at_ms: i64,
}

/// One remote session that still has an unrevoked, unexpired token.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteSession {
    pub agent_id: AgentId,
    pub session_id: String,
    /// The latest expiry among the session's live tokens.
    pub expires_at_ms: i64,
}

fn digest(token: &str) -> String {
    let mut out = String::with_capacity(64);
    for byte in Sha256::digest(token.as_bytes()) {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

fn random_hex(len: usize) -> Result<String, Error> {
    let mut bytes = vec![0u8; len];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn checked_ttl(ttl_ms: i64) -> Result<i64, Error> {
    if ttl_ms <= 0 {
        return Err(Error::Message(
            "a remote session token needs a positive lifetime".into(),
        ));
    }
    Ok(ttl_ms)
}

fn insert_token(
    tx: &rusqlite::Transaction<'_>,
    agent_id: &AgentId,
    session_id: &str,
    now: i64,
    ttl_ms: i64,
) -> Result<RemoteSessionToken, Error> {
    let token = format!("{REMOTE_SESSION_TOKEN_PREFIX}{}", random_hex(TOKEN_BYTES)?);
    let expires_at_ms = now.saturating_add(ttl_ms);
    tx.execute(
        "INSERT INTO remote_session_tokens
            (token_hash, agent_id, session_id, created_at_ms, expires_at_ms, revoked_at_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, NULL)",
        params![
            digest(&token),
            agent_id.as_str(),
            session_id,
            now,
            expires_at_ms
        ],
    )?;
    Ok(RemoteSessionToken {
        token,
        session_id: session_id.to_string(),
        expires_at_ms,
    })
}

impl Store {
    /// Start a new remote session for `agent_id` and issue its first token,
    /// valid for `ttl_ms`. Every earlier session of the agent is revoked in
    /// the same commit: one agent has at most one remote session at a time.
    pub fn begin_remote_session(
        &self,
        agent_id: &AgentId,
        ttl_ms: i64,
    ) -> Result<RemoteSessionToken, Error> {
        let ttl_ms = checked_ttl(ttl_ms)?;
        let session_id = format!("{SESSION_ID_PREFIX}{}", random_hex(SESSION_BYTES)?);
        let now = now_ms();
        self.write_tx(|tx| {
            agent_exists(tx, agent_id)?;
            tx.execute(
                "UPDATE remote_session_tokens SET revoked_at_ms = ?1
                 WHERE agent_id = ?2 AND revoked_at_ms IS NULL",
                params![now, agent_id.as_str()],
            )?;
            insert_token(tx, agent_id, &session_id, now, ttl_ms)
        })
    }

    /// Issue a fresh token for a session that is still live, valid for
    /// `ttl_ms`. Earlier tokens of the session keep their own expiry, so a
    /// call the remote side made with the last token still completes. A
    /// revoked or fully expired session is never extended.
    pub fn rotate_remote_session(
        &self,
        agent_id: &AgentId,
        session_id: &str,
        ttl_ms: i64,
    ) -> Result<RemoteSessionToken, Error> {
        let ttl_ms = checked_ttl(ttl_ms)?;
        let now = now_ms();
        self.write_tx(|tx| {
            let live: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM remote_session_tokens
                  WHERE agent_id = ?1 AND session_id = ?2
                    AND revoked_at_ms IS NULL AND expires_at_ms > ?3)",
                params![agent_id.as_str(), session_id, now],
                |row| row.get(0),
            )?;
            if !live {
                return Err(Error::Message(format!(
                    "remote session {session_id} is not live"
                )));
            }
            insert_token(tx, agent_id, session_id, now, ttl_ms)
        })
    }

    /// The agent and session a presented remote session token authenticates
    /// now. A token that is malformed, unknown, revoked or expired, or whose
    /// agent is paused, archived or gone, authenticates nothing.
    pub fn authenticate_remote_session(&self, token: &str) -> Result<RemoteSessionGrant, Error> {
        self.authenticate_remote_session_at(token, now_ms())
    }

    /// [`Self::authenticate_remote_session`] at a given time.
    pub fn authenticate_remote_session_at(
        &self,
        token: &str,
        now: i64,
    ) -> Result<RemoteSessionGrant, Error> {
        let rejected = || Error::Message("the remote session is not authenticated".into());
        if !is_remote_session_token(token) {
            return Err(rejected());
        }
        let conn = self.read_conn();
        let found = conn
            .prepare_cached(
                "SELECT t.agent_id, t.session_id, t.expires_at_ms, a.state
                   FROM remote_session_tokens t
                   JOIN agents a ON a.id = t.agent_id
                  WHERE t.token_hash = ?1 AND t.revoked_at_ms IS NULL AND t.expires_at_ms > ?2",
            )?
            .query_row(params![digest(token), now], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })
            .optional()?;
        match found {
            Some((agent_id, session_id, expires_at_ms, state))
                if !matches!(state.as_str(), "paused" | "archived") =>
            {
                Ok(RemoteSessionGrant {
                    agent_id: AgentId::parse(&agent_id)?,
                    session_id,
                    expires_at_ms,
                })
            }
            _ => Err(rejected()),
        }
    }

    /// Revoke the agent's remote sessions now: one session when `session_id`
    /// names it, else all of them. Returns how many tokens were revoked.
    pub fn revoke_remote_sessions(
        &self,
        agent_id: &AgentId,
        session_id: Option<&str>,
    ) -> Result<usize, Error> {
        let now = now_ms();
        self.write_tx(|tx| {
            agent_exists(tx, agent_id)?;
            Ok(tx.execute(
                "UPDATE remote_session_tokens SET revoked_at_ms = ?1
                 WHERE agent_id = ?2 AND revoked_at_ms IS NULL
                   AND (?3 IS NULL OR session_id = ?3)",
                params![now, agent_id.as_str(), session_id],
            )?)
        })
    }

    /// Revoke one token, for example one that was issued but never reached
    /// the file its session reads. Returns whether it was live.
    pub fn revoke_remote_session_token(&self, token: &str) -> Result<bool, Error> {
        let now = now_ms();
        let digest = digest(token);
        self.write_tx(|tx| {
            Ok(tx.execute(
                "UPDATE remote_session_tokens SET revoked_at_ms = ?1
                 WHERE token_hash = ?2 AND revoked_at_ms IS NULL",
                params![now, digest],
            )? > 0)
        })
    }

    /// Every remote session with a live token at `now`, with the latest expiry
    /// among its live tokens, oldest expiry first.
    pub fn live_remote_sessions(&self, now: i64) -> Result<Vec<RemoteSession>, Error> {
        let conn = self.read_conn();
        let mut stmt = conn.prepare_cached(
            "SELECT agent_id, session_id, MAX(expires_at_ms) AS expires
               FROM remote_session_tokens
              WHERE revoked_at_ms IS NULL AND expires_at_ms > ?1
              GROUP BY agent_id, session_id
              ORDER BY expires, agent_id, session_id",
        )?;
        let rows = stmt.query_map([now], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })?;
        let mut sessions = Vec::new();
        for row in rows {
            let (agent_id, session_id, expires_at_ms) = row?;
            sessions.push(RemoteSession {
                agent_id: AgentId::parse(&agent_id)?,
                session_id,
                expires_at_ms,
            });
        }
        Ok(sessions)
    }

    /// Forget tokens that expired or were revoked before `before_ms`. They
    /// authenticate nothing; this only bounds the table.
    pub fn prune_remote_session_tokens(&self, before_ms: i64) -> Result<usize, Error> {
        let stale: bool = self.read_conn().query_row(
            "SELECT EXISTS(SELECT 1 FROM remote_session_tokens
              WHERE expires_at_ms < ?1 OR revoked_at_ms < ?1)",
            [before_ms],
            |row| row.get(0),
        )?;
        if !stale {
            return Ok(0);
        }
        self.write_tx(|tx| {
            Ok(tx.execute(
                "DELETE FROM remote_session_tokens
                 WHERE expires_at_ms < ?1 OR revoked_at_ms < ?1",
                [before_ms],
            )?)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NewAgent;

    fn store() -> (tempfile::TempDir, Store, AgentId) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("cloud", "claude-cloud"))
            .unwrap();
        (dir, store, agent.id)
    }

    const HOUR_MS: i64 = 3_600_000;

    #[test]
    fn a_token_is_bound_to_its_agent_and_session_and_stored_only_as_a_digest() {
        let (_dir, store, agent) = store();
        let issued = store.begin_remote_session(&agent, HOUR_MS).unwrap();
        assert!(is_remote_session_token(&issued.token));
        assert!(issued.session_id.starts_with(SESSION_ID_PREFIX));
        assert!(!format!("{issued:?}").contains(&issued.token));

        let grant = store.authenticate_remote_session(&issued.token).unwrap();
        assert_eq!(grant.agent_id, agent);
        assert_eq!(grant.session_id, issued.session_id);
        assert_eq!(grant.expires_at_ms, issued.expires_at_ms);

        let stored: Vec<String> = {
            let conn = store.read_conn();
            let mut stmt = conn
                .prepare("SELECT token_hash FROM remote_session_tokens")
                .unwrap();
            stmt.query_map([], |row| row.get(0))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap()
        };
        assert_eq!(stored, vec![digest(&issued.token)]);
        assert!(!stored[0].contains(&issued.token[REMOTE_SESSION_TOKEN_PREFIX.len()..]));
    }

    #[test]
    fn a_token_stops_authenticating_at_its_expiry() {
        let (_dir, store, agent) = store();
        let issued = store.begin_remote_session(&agent, HOUR_MS).unwrap();
        let before = issued.expires_at_ms - 1;
        assert!(
            store
                .authenticate_remote_session_at(&issued.token, before)
                .is_ok()
        );
        assert!(
            store
                .authenticate_remote_session_at(&issued.token, issued.expires_at_ms)
                .is_err()
        );
        // An expired session cannot be extended by rotation either.
        assert!(
            store
                .live_remote_sessions(issued.expires_at_ms)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn rotation_issues_a_new_token_while_the_last_one_keeps_its_own_expiry() {
        let (_dir, store, agent) = store();
        let first = store.begin_remote_session(&agent, HOUR_MS).unwrap();
        let second = store
            .rotate_remote_session(&agent, &first.session_id, 2 * HOUR_MS)
            .unwrap();
        assert_ne!(first.token, second.token);
        assert_eq!(first.session_id, second.session_id);
        assert!(second.expires_at_ms > first.expires_at_ms);
        assert!(store.authenticate_remote_session(&first.token).is_ok());
        assert!(store.authenticate_remote_session(&second.token).is_ok());
        let live = store.live_remote_sessions(now_ms()).unwrap();
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].expires_at_ms, second.expires_at_ms);
        // Past the first token's expiry only the rotated one authenticates.
        assert!(
            store
                .authenticate_remote_session_at(&first.token, first.expires_at_ms)
                .is_err()
        );
        assert!(
            store
                .authenticate_remote_session_at(&second.token, first.expires_at_ms)
                .is_ok()
        );
        // A token that never reached its file is withdrawn alone.
        assert!(store.revoke_remote_session_token(&second.token).unwrap());
        assert!(!store.revoke_remote_session_token(&second.token).unwrap());
        assert!(store.authenticate_remote_session(&second.token).is_err());
        assert!(store.authenticate_remote_session(&first.token).is_ok());
    }

    #[test]
    fn revocation_on_demand_relaunch_pause_and_archive_ends_the_session() {
        let (_dir, store, agent) = store();

        // On demand, one session.
        let first = store.begin_remote_session(&agent, HOUR_MS).unwrap();
        assert_eq!(
            store
                .revoke_remote_sessions(&agent, Some("rs_other"))
                .unwrap(),
            0
        );
        assert_eq!(
            store
                .revoke_remote_sessions(&agent, Some(&first.session_id))
                .unwrap(),
            1
        );
        assert!(store.authenticate_remote_session(&first.token).is_err());
        assert!(
            store
                .rotate_remote_session(&agent, &first.session_id, HOUR_MS)
                .is_err(),
            "a revoked session is never extended"
        );

        // A relaunch supersedes the previous session.
        let second = store.begin_remote_session(&agent, HOUR_MS).unwrap();
        let third = store.begin_remote_session(&agent, HOUR_MS).unwrap();
        assert!(store.authenticate_remote_session(&second.token).is_err());
        assert!(store.authenticate_remote_session(&third.token).is_ok());

        // Pausing revokes in the same commit, and resuming does not revive it.
        store.set_agent_state(&agent, "paused").unwrap();
        assert!(store.authenticate_remote_session(&third.token).is_err());
        store.set_agent_state(&agent, "running").unwrap();
        assert!(store.authenticate_remote_session(&third.token).is_err());

        // Archiving revokes too.
        let fourth = store.begin_remote_session(&agent, HOUR_MS).unwrap();
        assert!(store.authenticate_remote_session(&fourth.token).is_ok());
        store.set_agent_state(&agent, "archived").unwrap();
        assert!(store.authenticate_remote_session(&fourth.token).is_err());
        assert!(store.live_remote_sessions(now_ms()).unwrap().is_empty());
    }

    #[test]
    fn an_idle_agent_keeps_its_session_until_relaunch() {
        // Scale-to-zero can race a new message back to `running` on the same
        // process, so `idle` alone never revokes; the next launch does.
        let (_dir, store, agent) = store();
        let issued = store.begin_remote_session(&agent, HOUR_MS).unwrap();
        store.set_agent_state(&agent, "idle").unwrap();
        assert!(store.authenticate_remote_session(&issued.token).is_ok());
    }

    #[test]
    fn other_token_kinds_and_shapes_never_authenticate() {
        let (_dir, store, agent) = store();
        store.hold_capability_key(&"k".repeat(64)).unwrap();
        let local = store.issue_capability(&agent).unwrap();
        assert!(!is_remote_session_token(&local));
        assert!(store.authenticate_remote_session(&local).is_err());
        let issued = store.begin_remote_session(&agent, HOUR_MS).unwrap();
        // A remote token is never a local capability.
        assert!(store.authenticate_capability(&issued.token).is_err());
        assert!(store.capability_holder(&issued.token).unwrap().is_none());
        for bad in [
            "",
            REMOTE_SESSION_TOKEN_PREFIX,
            &format!("{REMOTE_SESSION_TOKEN_PREFIX}{}", "z".repeat(64)),
            &issued.token[..issued.token.len() - 1],
            &format!("{}0", issued.token),
        ] {
            assert!(store.authenticate_remote_session(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn deleting_the_agent_removes_its_tokens_and_pruning_bounds_the_table() {
        let (_dir, store, agent) = store();
        let issued = store.begin_remote_session(&agent, HOUR_MS).unwrap();
        store.revoke_remote_sessions(&agent, None).unwrap();
        let other = store
            .create_agent(NewAgent::new("cloud-2", "claude-cloud"))
            .unwrap();
        let kept = store.begin_remote_session(&other.id, HOUR_MS).unwrap();
        assert_eq!(store.prune_remote_session_tokens(now_ms() + 1).unwrap(), 1);
        assert!(store.authenticate_remote_session(&kept.token).is_ok());
        assert!(store.authenticate_remote_session(&issued.token).is_err());

        store.delete_agent(&other.id).unwrap();
        let remaining: i64 = store
            .read_conn()
            .query_row("SELECT COUNT(*) FROM remote_session_tokens", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(remaining, 0);
    }
}
