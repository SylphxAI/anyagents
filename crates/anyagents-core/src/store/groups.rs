//! Named groups, room fan-out, chat membership, and inter-agent reachability.

use super::channels::telegram_chat_links;
use super::helpers::agent_exists;
use super::types::{Agent, Incoming, ReceiveOutcome};
use super::{Store, now_ms};
use crate::db::{DbExec, DbExecExt};
use crate::db_params;
use crate::error::Error;
use crate::id::AgentId;

impl Store {
    /// Append one message to every agent in a room: all of them except the
    /// sender, because a room message is not echoed back to its author.
    ///
    /// Each agent gets its own row with its own sequence, so a room message is
    /// one message per mailbox and every guarantee stays per mailbox. A
    /// `source_key` is namespaced per agent, so replaying the same room line is
    /// idempotent per recipient.
    ///
    /// A room named as a Telegram chat (`<chat id>` or `telegram:chat:<id>`)
    /// reaches only the agents whose own bot is attached to that chat, so one
    /// chat's posts never arrive in another chat's mailboxes. Membership comes
    /// from the channel state each bot saved, never from a hand-kept list.
    pub fn receive_room(
        &self,
        room: &str,
        incoming: Incoming,
    ) -> Result<Vec<(Agent, ReceiveOutcome)>, Error> {
        self.receive_room_with(room, incoming, |agent, one| self.receive(&agent.id, one))
    }

    /// Deliver an unread recipient's incoming message after membership and
    /// replay checks. The callback owns preparation and persistence, so it can
    /// release temporary resources if persistence fails. Replays skip it.
    pub fn receive_room_with(
        &self,
        room: &str,
        incoming: Incoming,
        mut deliver: impl FnMut(&Agent, Incoming) -> Result<ReceiveOutcome, Error>,
    ) -> Result<Vec<(Agent, ReceiveOutcome)>, Error> {
        let sender = incoming.sender.clone();
        // Immutable IDs never fall back to a same-spelled display name. Legacy
        // names (with or without agent:) retain their existing alias lookup.
        let author_id = sender.as_deref().and_then(|value| {
            let reference = value.strip_prefix("agent:").unwrap_or(value);
            match AgentId::parse(reference) {
                Ok(id) => Some(id),
                Err(_) => self.agent_by_name(reference).ok().map(|agent| agent.id),
            }
        });
        let members = self.room_members(room)?;
        let mut delivered = Vec::new();
        for agent in self.list_agents()? {
            if agent.state == "archived" || agent.state == "paused" {
                continue;
            }
            if author_id.as_ref() == Some(&agent.id) {
                continue;
            }
            if let Some(members) = &members
                && !members.iter().any(|name| name == &agent.name)
            {
                continue;
            }
            let default_room_reply = incoming.reply_to.clone().or_else(|| {
                let clean = room
                    .strip_prefix("group:")
                    .or_else(|| room.strip_prefix("telegram:chat:"))
                    .or_else(|| room.strip_prefix("chat:"))
                    .unwrap_or(room);
                if clean == "all"
                    || clean == "peers"
                    || clean
                        .split(':')
                        .next()
                        .unwrap_or(clean)
                        .parse::<i64>()
                        .is_ok()
                {
                    None
                } else {
                    Some(format!("group:{clean}"))
                }
            });
            let one = Incoming {
                text: incoming.text.clone(),
                sender: sender.clone(),
                source_time_ms: incoming.source_time_ms,
                source_key: incoming
                    .source_key
                    .as_ref()
                    .map(|key| format!("room:{room}:{key}")),
                reply_to: default_room_reply,
                quote: incoming.quote.clone(),
                origin: incoming.origin,
                received_at_ms: incoming.received_at_ms,
            };
            if let Some(key) = one.source_key.as_deref()
                && let Some(message) = self.message_by_source_key(&agent.id, key)?
            {
                delivered.push((agent, ReceiveOutcome::Duplicate(message)));
                continue;
            }
            let outcome = deliver(&agent, one)?;
            delivered.push((agent, outcome));
        }
        Ok(delivered)
    }

    /// Who a room reaches, when the room names a chat. `None` is a room that is
    /// not a chat: every agent, as a group by name always was.
    /// Who a room reaches.
    ///
    /// A chat (`<id>`, `chat:<id>`, `telegram:chat:<id>[:thread]`) reaches the
    /// agents whose own bot is attached to that chat. A named group reaches its
    /// own members, and nobody else. A room that is neither is empty rather
    /// than everyone: a name we do not know must never fan out to the world.
    pub fn room_members(&self, room: &str) -> Result<Option<Vec<String>>, Error> {
        let named = room
            .strip_prefix("telegram:chat:")
            .or_else(|| room.strip_prefix("chat:"))
            .unwrap_or(room);
        // A forum topic follows the chat id; the chat is the same either way.
        if let Ok(chat_id) = named.split(':').next().unwrap_or(named).parse::<i64>() {
            return Ok(Some(self.chat_members(chat_id)?));
        }
        let name = room.strip_prefix("group:").unwrap_or(room);
        if name == "all" || name == "peers" {
            let names = self
                .list_agents()?
                .into_iter()
                .filter(|agent| {
                    agent.parent_id.is_none()
                        && agent.state != "archived"
                        && agent.state != "paused"
                })
                .map(|agent| agent.name)
                .collect();
            return Ok(Some(names));
        }
        let members = self.group_members(name)?;
        Ok(Some(members))
    }

    /// The agents whose own bot is attached to one chat.
    pub fn chat_members(&self, chat_id: i64) -> Result<Vec<String>, Error> {
        let mut members = Vec::new();
        for channel in self.list_channels()? {
            for agent_id in &channel.agents {
                // The bot's own saved links say which chats it answers in.
                let linked = telegram_chat_links(&channel.state)
                    .iter()
                    .any(|(_, linked)| *linked == chat_id);
                if !linked {
                    continue;
                }
                if let Ok(agent) = self.agent(agent_id)
                    && !members.contains(&agent.name)
                {
                    members.push(agent.name);
                }
            }
        }
        Ok(members)
    }

    /// Put one agent in a named group. A group exists once it has a member.
    pub fn join_group(&self, name: &str, agent_id: &AgentId) -> Result<(), Error> {
        if name.is_empty() || name.contains(':') || name.chars().any(char::is_whitespace) {
            return Err(Error::Message(format!("invalid group name: {name}")));
        }
        self.write_tx(|tx| {
            agent_exists(tx, agent_id)?;
            tx.db_execute(
                "INSERT INTO groups (name, created_at_ms) VALUES (?1, ?2) ON CONFLICT DO NOTHING",
                &db_params![name, now_ms()],
            )?;
            tx.db_execute(
                "INSERT INTO group_agents (group_name, agent_id) VALUES (?1, ?2) ON CONFLICT DO NOTHING",
                &db_params![name, agent_id.as_str()],
            )?;
            Ok(())
        })
    }

    /// Take one agent out of a named group.
    pub fn leave_group(&self, name: &str, agent_id: &AgentId) -> Result<(), Error> {
        self.write_tx(|tx| {
            tx.db_execute(
                "DELETE FROM group_agents WHERE group_name = ?1 AND agent_id = ?2",
                &db_params![name, agent_id.as_str()],
            )?;
            Ok(())
        })
    }

    /// Remove every group membership for an agent. Group names have no lifecycle privilege.
    pub fn leave_all_groups(&self, agent_id: &AgentId) -> Result<(), Error> {
        self.write_tx(|tx| {
            tx.db_execute(
                "DELETE FROM group_agents WHERE agent_id = ?1",
                &db_params![agent_id.as_str()],
            )?;
            Ok(())
        })
    }

    /// Set whether a named virtual group is `public` (`true` = open to any public agent;
    /// `false` = private/invite-only; participation requires membership or an invitation).
    pub fn set_group_public(&self, name: &str, public: bool) -> Result<(), Error> {
        if name.is_empty() || name.contains(':') || name.chars().any(char::is_whitespace) {
            return Err(Error::Message(format!("invalid group name: {name}")));
        }
        self.write_tx(|tx| {
            tx.db_execute(
                "INSERT INTO groups (name, created_at_ms, public) VALUES (?1, ?2, ?3)
                 ON CONFLICT(name) DO UPDATE SET public = excluded.public",
                &db_params![name, now_ms(), i64::from(public)],
            )?;
            Ok(())
        })
    }

    /// Whether a named virtual group is `public` (`true`) or `private` (`false`, default).
    pub fn is_group_public(&self, name: &str) -> bool {
        let conn = self.read_conn();
        conn.db_query_one(
            "SELECT public FROM groups WHERE name = ?1",
            &db_params![name],
            |r| Ok(r.get::<i64>(0)? != 0),
        )
        .unwrap_or(false)
    }

    /// The agents in a named group, by name. An unknown group is empty.
    pub fn group_members(&self, name: &str) -> Result<Vec<String>, Error> {
        group_members_on(&self.read_conn(), name)
    }

    /// Every named group and its current members, ordered by group name.
    pub fn list_groups(&self) -> Result<Vec<(String, Vec<String>)>, Error> {
        let conn = self.read_conn();
        let names: Vec<String> =
            conn.db_query_vec("SELECT name FROM groups ORDER BY name", &[], |row| {
                row.get(0)
            })?;
        let mut groups = Vec::with_capacity(names.len());
        for name in names {
            let members = group_members_on(&conn, &name)?;
            groups.push((name, members));
        }
        Ok(groups)
    }

    /// Returns `true` when `agent_a` and `agent_b` are members of at least one shared
    /// internal virtual group (`group_agents`) or attached to the same Telegram chat.
    pub fn share_any_group_or_chat(&self, agent_a: &AgentId, agent_b: &AgentId) -> bool {
        let shares_group = {
            let conn = self.read_conn();
            conn.db_query_one(
                "SELECT EXISTS(
                     SELECT 1 FROM group_agents g1
                     JOIN group_agents g2 ON g1.group_name = g2.group_name
                     WHERE g1.agent_id = ?1 AND g2.agent_id = ?2
                 )",
                &db_params![agent_a.as_str(), agent_b.as_str()],
                |r| r.get::<bool>(0),
            )
            .unwrap_or(false)
        };
        if shares_group {
            return true;
        }
        let Ok(channels) = self.list_channels() else {
            return false;
        };
        let mut chats_a = std::collections::HashSet::new();
        let mut chats_b = std::collections::HashSet::new();
        for ch in channels {
            let has_a = ch.agents.iter().any(|id| id == agent_a);
            let has_b = ch.agents.iter().any(|id| id == agent_b);
            if !has_a && !has_b {
                continue;
            }
            for (_, chat_id) in telegram_chat_links(&ch.state) {
                if has_a {
                    chats_a.insert(chat_id);
                }
                if has_b {
                    chats_b.insert(chat_id);
                }
            }
        }
        chats_a.iter().any(|cid| chats_b.contains(cid))
    }

    /// Stateful reply capability: returns `true` if `from_agent` has previously sent a direct
    /// message to `to_agent`, allowing `to_agent` to reply back even if `from_agent` is `public: false`.
    pub fn has_messaged(&self, from_agent: &Agent, to_agent: &Agent) -> bool {
        let dest_name = format!("agent:{}", to_agent.name);
        self.read_conn()
            .db_query_one(
                "SELECT EXISTS(
                     SELECT 1 FROM outbound_messages
                     WHERE agent_id = ?1 AND (destination = ?2 OR destination = ?3)
                 )",
                &db_params![from_agent.id.as_str(), dest_name, to_agent.id.as_str()],
                |r| r.get::<bool>(0),
            )
            .unwrap_or(false)
    }
}

fn group_members_on(conn: &dyn DbExec, name: &str) -> Result<Vec<String>, Error> {
    conn.db_query_vec(
        "SELECT a.name FROM group_agents AS g JOIN agents AS a ON a.id = g.agent_id
         WHERE g.group_name = ?1 ORDER BY a.name",
        &db_params![name],
        |row| row.get(0),
    )
}
