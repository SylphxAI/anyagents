//! Channel CRUD, credential bindings, state CAS, and agent attachments.

use super::helpers::{agent_by_id, agent_exists};
use super::types::{Agent, Channel, NewChannel};
use super::{Store, now_ms};
use crate::db::{DbExec, DbExecExt, DbRow};
use crate::db_params;
use crate::error::Error;
use crate::id::{AgentId, ChannelId};

impl Store {
    pub fn create_channel(&self, new: NewChannel) -> Result<Channel, Error> {
        if new.kind.is_empty() {
            return Err(Error::Message("channel kind is empty".into()));
        }
        let channel = Channel {
            id: ChannelId::generate(),
            kind: new.kind,
            identity: new.identity,
            config: if new.config.is_empty() {
                "{}".to_string()
            } else {
                new.config
            },
            credential_ref: new.credential_ref,
            state: "{}".to_string(),
            created_at_ms: now_ms(),
            agents: Vec::new(),
        };
        self.write_tx(|tx| {
            tx.db_execute(
                "INSERT INTO channels (id, kind, identity, config, credential_ref, state, created_at_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                &db_params![
                    channel.id.as_str(),
                    channel.kind,
                    channel.identity,
                    channel.config,
                    channel.credential_ref,
                    channel.state,
                    channel.created_at_ms,
                ],
            )?;
            Ok(())
        })?;
        Ok(channel)
    }

    /// Every channel, with the agents attached to it. No secret is in the rows.
    pub fn list_channels(&self) -> Result<Vec<Channel>, Error> {
        let conn = self.read_conn();
        let mut channels = conn.db_query_vec(
            "SELECT id, kind, identity, config, credential_ref, created_at_ms, state FROM channels
             ORDER BY created_at_ms",
            &[],
            channel_from_row_db,
        )?;
        for channel in &mut channels {
            channel.agents = agents_of(&conn, &channel.id)?;
        }
        Ok(channels)
    }

    /// Point a channel at a secret that is already stored. The value stays out.
    pub fn set_credential_ref(
        &self,
        channel_id: &ChannelId,
        credential_ref: &str,
    ) -> Result<(), Error> {
        if credential_ref.is_empty() {
            return Err(Error::Message("credential ref is empty".into()));
        }
        self.write_tx(|tx| {
            let changed = tx.db_execute(
                "UPDATE channels SET credential_ref = ?2 WHERE id = ?1",
                &db_params![channel_id.as_str(), credential_ref],
            )?;
            if changed == 0 {
                return Err(Error::ChannelNotFound(channel_id.to_string()));
            }
            Ok(())
        })
    }

    /// Replace a channel's adapter memory.
    pub fn set_channel_state(&self, channel_id: &ChannelId, state: &str) -> Result<(), Error> {
        serde_json::from_str::<serde_json::Value>(state)
            .map_err(|_| Error::Message("channel state is not json".into()))?;
        self.write_tx(|tx| {
            let changed = tx.db_execute(
                "UPDATE channels SET state = ?2 WHERE id = ?1",
                &db_params![channel_id.as_str(), state],
            )?;
            if changed == 0 {
                return Err(Error::ChannelNotFound(channel_id.to_string()));
            }
            Ok(())
        })
    }

    /// Commit adapter discoveries only if channel and owner still match their
    /// pre-I/O snapshots. A stale snapshot is a normal no-op, not an overwrite.
    pub fn compare_set_channel_state(
        &self,
        expected: &Channel,
        owner: &Agent,
        state: &str,
    ) -> Result<bool, Error> {
        serde_json::from_str::<serde_json::Value>(state)
            .map_err(|_| Error::Message("channel state is not json".into()))?;
        self.write_tx(|tx| {
            let mut current = channel_with(tx, &expected.id)?;
            current.agents = agents_of(tx, &current.id)?;
            let agent = agent_by_id(tx, &owner.id)?;
            if current.kind != expected.kind
                || current.config != expected.config
                || current.identity != expected.identity
                || current.state != expected.state
                || current.credential_ref != expected.credential_ref
                || current.agents != expected.agents
                || !current.agents.contains(&owner.id)
                || agent.state != owner.state
                || agent.launch_env != owner.launch_env
            {
                return Ok(false);
            }
            tx.db_execute(
                "UPDATE channels SET state = ?2 WHERE id = ?1",
                &db_params![expected.id.as_str(), state],
            )?;
            Ok(true)
        })
    }

    pub fn attach_channel(&self, channel_id: &ChannelId, agent_id: &AgentId) -> Result<(), Error> {
        self.write_tx(|tx| {
            channel_with(tx, channel_id)?;
            agent_exists(tx, agent_id)?;
            tx.db_execute(
                "INSERT INTO channel_agents (channel_id, agent_id) VALUES (?1, ?2) ON CONFLICT DO NOTHING",
                &db_params![channel_id.as_str(), agent_id.as_str()],
            )?;
            Ok(())
        })
    }

    /// Update a channel's external identity (e.g. Telegram `@username` or `id:username`).
    pub fn set_channel_identity(
        &self,
        channel_id: &ChannelId,
        identity: Option<&str>,
    ) -> Result<(), Error> {
        let clean = identity.map(str::trim).filter(|s| !s.is_empty());
        self.write_tx(|tx| {
            let changed = tx.db_execute(
                "UPDATE channels SET identity = ?2 WHERE id = ?1",
                &db_params![channel_id.as_str(), clean],
            )?;
            if changed == 0 {
                return Err(Error::ChannelNotFound(channel_id.to_string()));
            }
            Ok(())
        })
    }

    /// Detach all agents currently bound to `channel_id` (used when re-assigning a channel).
    pub fn detach_channel_agents(&self, channel_id: &ChannelId) -> Result<(), Error> {
        self.write_tx(|tx| {
            channel_with(tx, channel_id)?;
            tx.db_execute(
                "DELETE FROM channel_agents WHERE channel_id = ?1",
                &db_params![channel_id.as_str()],
            )?;
            Ok(())
        })
    }

    /// Detach one agent from `channel_id`.
    pub fn detach_channel_agent(
        &self,
        channel_id: &ChannelId,
        agent_id: &AgentId,
    ) -> Result<(), Error> {
        self.write_tx(|tx| {
            tx.db_execute(
                "DELETE FROM channel_agents WHERE channel_id = ?1 AND agent_id = ?2",
                &db_params![channel_id.as_str(), agent_id.as_str()],
            )?;
            Ok(())
        })
    }
}

pub(super) fn channel_with(conn: &dyn DbExec, id: &ChannelId) -> Result<Channel, Error> {
    let found = conn.db_query_opt(
        "SELECT id, kind, identity, config, credential_ref, created_at_ms, state FROM channels WHERE id = ?1",
        &db_params![id.as_str()],
        channel_from_row_db,
    )?;
    found.ok_or_else(|| Error::ChannelNotFound(id.to_string()))
}

/// The (agent, chat) links a Telegram bot saved in its channel state, read
/// from the `chats` array without depending on the channel crate. Anything
/// unreadable is no link, so a corrupt state closes a room rather than opening
/// it to every agent.
pub(super) fn telegram_chat_links(state: &str) -> Vec<(String, i64)> {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(state) else {
        return Vec::new();
    };
    value
        .get("chats")
        .and_then(serde_json::Value::as_array)
        .map(|links| {
            links
                .iter()
                .filter_map(|link| {
                    let agent = link.get("agent")?.as_str()?.to_string();
                    let chat = link.get("chat_id")?.as_i64()?;
                    Some((agent, chat))
                })
                .collect()
        })
        .unwrap_or_default()
}

pub(super) fn channel_from_row_db(row: &DbRow<'_>) -> Result<Channel, Error> {
    Ok(Channel {
        id: row.parse_str(0)?,
        kind: row.get(1)?,
        identity: row.get(2)?,
        config: row.get(3)?,
        credential_ref: row.get(4)?,
        created_at_ms: row.get(5)?,
        state: row.get(6)?,
        agents: Vec::new(),
    })
}

pub(super) fn agents_of(conn: &dyn DbExec, channel: &ChannelId) -> Result<Vec<AgentId>, Error> {
    let raw_ids = conn.db_query_vec(
        "SELECT agent_id FROM channel_agents WHERE channel_id = ?1 ORDER BY agent_id",
        &db_params![channel.as_str()],
        |row| row.get::<String>(0),
    )?;
    raw_ids.iter().map(|id| AgentId::parse(id)).collect()
}
