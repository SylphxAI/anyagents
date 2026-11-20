//! Named groups, room fan-out, chat membership, and inter-agent reachability.

use super::channels::telegram_chat_links;
use super::helpers::{agent_exists, decode_budget_column, decode_env, decode_runtime_column};
use super::types::{Agent, Incoming, ReceiveOutcome};
use super::{Store, now_ms};
use crate::db::{DbExec, DbExecExt};
use crate::db_params;
use crate::error::Error;
use crate::id::AgentId;

/// Batched group metadata and full member agent rows loaded in 2 queries.
#[derive(Clone, Debug)]
pub struct GroupSummary {
    pub name: String,
    pub public: bool,
    pub owner_agent_id: Option<AgentId>,
    pub members: Vec<Agent>,
}

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
        self.try_receive_room_with(room, incoming, |agent, one| deliver(agent, one).map(Some))
    }

    /// Like [Self::receive_room_with], but the callback can skip a recipient
    /// whose eligibility changed after the room's membership was read.
    pub fn try_receive_room_with(
        &self,
        room: &str,
        incoming: Incoming,
        mut deliver: impl FnMut(&Agent, Incoming) -> Result<Option<ReceiveOutcome>, Error>,
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
            if let Some(outcome) = deliver(&agent, one)? {
                delivered.push((agent, outcome));
            }
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
        let summaries = self.list_groups_with_roster()?;
        Ok(summaries
            .into_iter()
            .map(|g| (g.name, g.members.into_iter().map(|a| a.name).collect()))
            .collect())
    }

    /// Batched group listing with metadata (`public`, `owner_agent_id`) and full
    /// member `Agent` rows loaded in 2 SQL queries (zero N+1 queries).
    pub fn list_groups_with_roster(&self) -> Result<Vec<GroupSummary>, Error> {
        let conn = self.read_conn();
        let meta_rows: Vec<(String, bool, Option<String>)> = conn.db_query_vec(
            "SELECT name, public, owner_agent_id FROM groups ORDER BY name",
            &[],
            |row| Ok((row.get(0)?, row.get::<i64>(1)? != 0, row.get(2)?)),
        )?;
        let member_rows: Vec<(String, Agent)> = conn.db_query_vec(
            "SELECT g.group_name, a.id, a.name, a.harness, a.model, a.workdir, a.instructions, \
             a.state, a.created_at_ms, a.launch_env, a.parent_id, a.role, \
             a.description, a.runtime, a.remote_budget_micros \
             FROM group_agents AS g \
             JOIN agents AS a ON a.id = g.agent_id \
             ORDER BY g.group_name, a.name",
            &[],
            |row| {
                let group_name: String = row.get(0)?;
                let agent_id = row.parse_str(1)?;
                let harness: String = row.get(3)?;
                let launch_env = row
                    .get::<Option<String>>(9)?
                    .map(|json| decode_env(&json))
                    .unwrap_or_default();
                let parent_id = match row.get::<Option<String>>(10)? {
                    Some(raw) if !raw.is_empty() => Some(AgentId::parse(&raw)?),
                    _ => None,
                };
                let runtime_raw: Option<String> = row.get(13)?;
                let budget_raw: Option<i64> = row.get(14)?;
                let runtime = decode_runtime_column(runtime_raw.as_deref(), &harness, &launch_env);
                let remote_budget_micros = decode_budget_column(budget_raw, &launch_env);
                let agent = Agent {
                    id: agent_id,
                    name: row.get(2)?,
                    harness,
                    runtime,
                    remote_budget_micros,
                    model: row.get(4)?,
                    workdir: row.get(5)?,
                    instructions: row.get(6)?,
                    state: row.get(7)?,
                    created_at_ms: row.get(8)?,
                    launch_env,
                    parent_id,
                    role: row.get(11)?,
                    description: row.get(12)?,
                };
                Ok((group_name, agent))
            },
        )?;
        let mut by_group: std::collections::HashMap<String, Vec<Agent>> =
            std::collections::HashMap::with_capacity(meta_rows.len());
        for (group_name, agent) in member_rows {
            by_group.entry(group_name).or_default().push(agent);
        }
        let mut out = Vec::with_capacity(meta_rows.len());
        for (name, public, owner_raw) in meta_rows {
            let members = by_group.remove(&name).unwrap_or_default();
            out.push(GroupSummary {
                name,
                public,
                owner_agent_id: owner_raw.as_deref().and_then(|s| AgentId::parse(s).ok()),
                members,
            });
        }
        Ok(out)
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
