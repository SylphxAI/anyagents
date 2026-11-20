//! Agent mailbox capabilities, authentication, and send receipts.

use std::io::Read;

use super::helpers::{agent_exists, hash, hex};
use super::origin::message_by_source;
use super::types::{Agent, Message};
use super::{Store, now_ms};
use crate::db::DbExecExt;
use crate::db_params;
use crate::error::Error;
use crate::id::AgentId;

/// A capability is 32 random bytes, written in hex.
const CAPABILITY_BYTES: usize = 32;

impl Store {
    /// Hold this process's mailbox capability key, so it can mint and check
    /// capabilities. The server sets it at start; the key never leaves memory.
    /// A process naming its own key makes every capability it minted useless to
    /// any other process, which is why only the one server should.
    pub fn hold_capability_key(&self, key: &str) -> Result<(), Error> {
        if key.len() < 32 {
            return Err(Error::Message("the capability key is too short".into()));
        }
        *self
            .capabilities
            .lock()
            .unwrap_or_else(|err| err.into_inner()) = Some(key.to_string());
        Ok(())
    }

    /// Whether this process can decide capability validity authoritatively.
    /// Helpers without the key may ask the daemon; the daemon must never ask
    /// its own socket again after rejecting a capability.
    pub fn holds_capability_key(&self) -> bool {
        self.capabilities
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .is_some()
    }

    /// A mailbox capability for one agent instance: an opaque value, stored as
    /// a hash, bound to the agent's immutable id and to the generation of its
    /// current session. Only the server that minted it reads it back.
    pub fn issue_capability(&self, agent_id: &AgentId) -> Result<String, Error> {
        let key = self.capability_key()?;
        let mut bytes = [0u8; CAPABILITY_BYTES];
        std::fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
        let token = hex(&bytes);
        let now = now_ms();
        self.write_tx(|tx| {
            agent_exists(tx, agent_id)?;
            // A new instance replaces the old one: the generation moves, and
            // every capability minted before it stops authenticating.
            let generation: i64 = tx.db_query_one(
                "SELECT COALESCE(MAX(generation), 0) + 1 FROM agent_capabilities WHERE agent_id = ?1",
                &db_params![agent_id.as_str()],
                |row| row.get(0),
            )?;
            tx.db_execute(
                "INSERT INTO agent_capabilities (token_hash, agent_id, generation, created_at_ms, revoked_at_ms)
                 VALUES (?1, ?2, ?3, ?4, NULL)",
                &db_params![hash(&key, &token), agent_id.as_str(), generation, now],
            )?;
            tx.db_execute(
                "UPDATE agent_capabilities SET revoked_at_ms = ?1
                 WHERE agent_id = ?2 AND generation < ?3 AND revoked_at_ms IS NULL",
                &db_params![now, agent_id.as_str(), generation],
            )?;
            Ok(())
        })?;
        Ok(token)
    }

    /// The agent a presented capability authenticates, when the server minted
    /// it and it was not replaced. The comparison is on the stored hash.
    pub fn authenticate_capability(&self, token: &str) -> Result<AgentId, Error> {
        let key = self.capability_key()?;
        let digest = hash(&key, token);
        let conn = self.read_conn();
        let found = conn.db_query_opt(
            "SELECT agent_id FROM agent_capabilities
             WHERE token_hash = ?1 AND revoked_at_ms IS NULL",
            &db_params![digest],
            |row| row.get::<String>(0),
        )?;
        match found {
            Some(id) => AgentId::parse(&id),
            None => Err(Error::Message("the session is not authenticated".into())),
        }
    }

    /// Resolves the full [`Agent`] record authenticated by a presented capability token.
    pub fn authenticate_agent(&self, token: &str) -> Result<Agent, Error> {
        let agent_id = self.authenticate_capability(token)?;
        self.agent(&agent_id)
    }

    /// A receipt one named send already produced, or `None`.
    pub fn send_receipt(&self, id: &str) -> Result<Option<String>, Error> {
        let conn = self.read_conn();
        conn.db_query_opt(
            "SELECT receipt FROM sends WHERE id = ?1",
            &db_params![id],
            |row| row.get(0),
        )
    }

    /// A committed mailbox receipt for this source, including when the server
    /// stopped before recording its whole-send receipt. Call under the lifecycle
    /// lock before preparing an archived recipient for an idempotent retry.
    pub fn message_by_source_key(
        &self,
        agent_id: &AgentId,
        key: &str,
    ) -> Result<Option<Message>, Error> {
        message_by_source(&self.read_conn(), agent_id, key)
    }

    /// Keep one named send's receipt after it was delivered.
    pub fn record_send_receipt(&self, id: &str, receipt: &str) -> Result<(), Error> {
        self.write_tx(|tx| {
            tx.db_execute(
                "INSERT INTO sends (id, receipt, created_at_ms) VALUES (?1, ?2, ?3)
                 ON CONFLICT (id) DO NOTHING",
                &db_params![id, receipt, now_ms()],
            )?;
            Ok(())
        })
    }

    /// This process's capability key. A command that starts before the server
    /// reads the key from `ANYAGENTS_CAPABILITY_KEY` (the test and repair path),
    /// else from the stored key the server wrote, else its own.
    pub(super) fn capability_key(&self) -> Result<String, Error> {
        if let Some(key) = self
            .capabilities
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .clone()
        {
            return Ok(key);
        }
        Err(Error::Message(
            "this process holds no mailbox capability key".into(),
        ))
    }

    /// Whether a capability was ever minted on this deployment. Once one has
    /// been, the deployment has an identity boundary.
    pub fn ever_authenticated(&self) -> bool {
        self.read_conn()
            .db_query_one(
                "SELECT EXISTS(SELECT 1 FROM agent_capabilities)",
                &[],
                |row| row.get(0),
            )
            .unwrap_or(false)
    }

    /// Whether this agent already has an unrevoked capability row in the store.
    pub fn has_active_capability(&self, agent_id: &AgentId) -> Result<bool, Error> {
        let count: i64 = self.read_conn().db_query_one(
            "SELECT COUNT(*) FROM agent_capabilities
             WHERE agent_id = ?1 AND revoked_at_ms IS NULL",
            &db_params![agent_id.as_str()],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    /// Replace an agent's instance: its generation moves, so every capability
    /// minted for the last instance stops authenticating at once.
    pub fn retire_capabilities(&self, agent_id: &AgentId) -> Result<(), Error> {
        self.write_tx(|tx| {
            agent_exists(tx, agent_id)?;
            tx.db_execute(
                "UPDATE agent_capabilities SET revoked_at_ms = ?1
                 WHERE agent_id = ?2 AND revoked_at_ms IS NULL",
                &db_params![now_ms(), agent_id.as_str()],
            )?;
            Ok(())
        })
    }
}
