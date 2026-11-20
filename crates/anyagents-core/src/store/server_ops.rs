//! Narrow reads for the server's request and maintenance paths: one indexed
//! query where they would otherwise decode every agent row or ask once per
//! listed agent.

use std::collections::{HashMap, HashSet};

use super::{AgentId, AgentSummary, Error, Store, telegram_chat_links};
use crate::db::{DbBackend, DbExec as _, DbExecExt as _, db_params};
use crate::stateless::{PendingRemoteTurn, SandboxBinding};

#[derive(Default)]
pub struct AgentOverviewMetadata {
    pub unread_count: i64,
    pub sandbox_lease: Option<OverviewSandboxLease>,
    pub active_remote_turns: Vec<OverviewRemoteTurn>,
}

#[derive(serde::Serialize)]
pub struct OverviewSandboxLease {
    provider: String,
    lease_state: String,
    lease_id: Option<String>,
    volume_id: String,
    lease_expires_at_ms: Option<i64>,
    idle_since_ms: Option<i64>,
    last_heartbeat_ms: Option<i64>,
}

impl From<SandboxBinding> for OverviewSandboxLease {
    fn from(binding: SandboxBinding) -> Self {
        Self {
            provider: binding.provider,
            lease_state: binding.lease_state,
            lease_id: binding.lease_id,
            volume_id: binding.volume_id,
            lease_expires_at_ms: binding.lease_expires_at_ms,
            idle_since_ms: binding.idle_since_ms,
            last_heartbeat_ms: binding.last_heartbeat_ms,
        }
    }
}

pub struct OverviewRemoteTurn {
    pub turn_id: String,
    pub harness_kind: String,
    pub provider_kind: Option<String>,
    pub status: String,
    pub created_at_ms: i64,
    pub last_event_at_ms: i64,
    pub next_poll_at_ms: Option<i64>,
}

impl From<PendingRemoteTurn> for OverviewRemoteTurn {
    fn from(turn: PendingRemoteTurn) -> Self {
        Self {
            turn_id: turn.turn_id,
            harness_kind: turn.harness_kind,
            provider_kind: turn.provider_kind,
            status: turn.status,
            created_at_ms: turn.created_at_ms,
            last_event_at_ms: turn.last_event_at_ms,
            next_poll_at_ms: turn.next_poll_at_ms,
        }
    }
}

// Count only the prefix the old mailbox listing returned. The range on
// (agent_id, seq) stops after 100 rows and never fetches message payloads.
// A missing cursor, like a failed cursor lookup, contributes zero.
macro_rules! overview_unread_count {
    () => {
        "CASE WHEN a.state = 'archived' OR c.agent_id IS NULL THEN 0 ELSE (
             SELECT COUNT(*) FROM (
                 SELECT 1 FROM messages m
                 WHERE m.agent_id = a.id AND m.seq > c.delivered_seq
                 ORDER BY m.seq LIMIT 100
             ) AS pending
         ) END"
    };
}

impl Store {
    /// One batched read for the overview's mailbox counts and PostgreSQL
    /// metadata. On SQLite, the leases and turns live on disk, not in SQL.
    pub fn agent_overview_metadata(
        &self,
        agents: &[AgentSummary],
    ) -> (HashMap<AgentId, AgentOverviewMetadata>, usize) {
        let conn = self.read_conn();
        if conn.backend() == DbBackend::Postgres {
            let rows = conn
                .db_query_vec(
                    concat!(
                        "SELECT a.id, ",
                        overview_unread_count!(),
                        ", b.provider, b.lease_state, b.lease_id, b.volume_id,
                           b.lease_expires_at_ms, b.idle_since_ms, b.last_heartbeat_ms,
                           t.turn_id, t.harness_kind, t.provider_kind, t.status,
                           t.created_at_ms, t.last_event_at_ms, t.next_poll_at_ms
                         FROM public.agents a
                         LEFT JOIN public.mailbox_cursors c ON c.agent_id = a.id
                         LEFT JOIN public.agent_sandbox_bindings b ON b.agent_id = a.id
                         LEFT JOIN public.pending_remote_turns t ON t.agent_id = a.id
                            AND a.state <> 'archived'
                            AND t.status IN ('dispatched', 'running')"
                    ),
                    &[],
                    |r| {
                        let sandbox_lease = if let Some(provider) = r.get::<Option<String>>(2)? {
                            Some(OverviewSandboxLease {
                                provider,
                                lease_state: r.get(3)?,
                                lease_id: r.get(4)?,
                                volume_id: r.get(5)?,
                                lease_expires_at_ms: r.get(6)?,
                                idle_since_ms: r.get(7)?,
                                last_heartbeat_ms: r.get(8)?,
                            })
                        } else {
                            None
                        };
                        // The partial unique index allows at most one active turn per agent.
                        let active_remote_turns =
                            if let Some(turn_id) = r.get::<Option<String>>(9)? {
                                vec![OverviewRemoteTurn {
                                    turn_id,
                                    harness_kind: r.get(10)?,
                                    provider_kind: r.get(11)?,
                                    status: r.get(12)?,
                                    created_at_ms: r.get(13)?,
                                    last_event_at_ms: r.get(14)?,
                                    next_poll_at_ms: r.get(15)?,
                                }]
                            } else {
                                Vec::new()
                            };
                        Ok((
                            r.get(0)?,
                            AgentOverviewMetadata {
                                unread_count: r.get(1)?,
                                sandbox_lease,
                                active_remote_turns,
                            },
                        ))
                    },
                )
                .unwrap_or_default();
            let active_sandboxes_count = rows
                .iter()
                .filter_map(|(_, meta)| meta.sandbox_lease.as_ref())
                .filter(|lease| lease.lease_state == "ready" && lease.lease_id.is_some())
                .count();
            return (rows.into_iter().collect(), active_sandboxes_count);
        }

        let mut unread_counts: HashMap<AgentId, i64> = conn
            .db_query_vec(
                concat!(
                    "SELECT a.id, ",
                    overview_unread_count!(),
                    " FROM agents a LEFT JOIN mailbox_cursors c ON c.agent_id = a.id"
                ),
                &[],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap_or_default()
            .into_iter()
            .collect();
        drop(conn);

        let bindings = self.sandbox_binding_store();
        let turns = self.remote_turn_store();
        let metadata = agents
            .iter()
            .map(|agent| {
                let sandbox_lease = bindings.get(&agent.id).ok().flatten().map(Into::into);
                let active_remote_turns = if agent.state == "archived" {
                    Vec::new()
                } else {
                    turns
                        .list_active_for_agent(&agent.id)
                        .unwrap_or_default()
                        .into_iter()
                        .map(Into::into)
                        .collect()
                };
                (
                    agent.id.clone(),
                    AgentOverviewMetadata {
                        unread_count: unread_counts.remove(&agent.id).unwrap_or_default(),
                        sandbox_lease,
                        active_remote_turns,
                    },
                )
            })
            .collect();
        let active_sandboxes_count = bindings.list_active_ready().map(|v| v.len()).unwrap_or(0);
        (metadata, active_sandboxes_count)
    }

    /// Every agent that shares an internal group or a Telegram chat with
    /// `agent`: exactly the `other` for which
    /// [`Store::share_any_group_or_chat`]`(agent, other)` holds, read with one
    /// query and one channel listing. A read error shares nothing, as there.
    pub fn agents_sharing_group_or_chat(&self, agent: &AgentId) -> HashSet<AgentId> {
        let mut shared = HashSet::new();
        {
            let conn = self.read_conn();
            if let Ok(ids) = conn.db_query_vec(
                "SELECT DISTINCT g2.agent_id FROM group_agents g1
                 JOIN group_agents g2 ON g1.group_name = g2.group_name
                 WHERE g1.agent_id = ?1",
                &db_params![agent],
                |r| r.get::<String>(0),
            ) {
                shared.extend(ids.into_iter().filter_map(|id| AgentId::parse(&id).ok()));
            }
        }
        let Ok(channels) = self.list_channels() else {
            return shared;
        };
        let chats: Vec<HashSet<i64>> = channels
            .iter()
            .map(|channel| {
                telegram_chat_links(&channel.state)
                    .into_iter()
                    .map(|(_, chat)| chat)
                    .collect()
            })
            .collect();
        let mine: HashSet<i64> = channels
            .iter()
            .zip(&chats)
            .filter(|(channel, _)| channel.agents.contains(agent))
            .flat_map(|(_, chats)| chats.iter().copied())
            .collect();
        for (channel, chats) in channels.iter().zip(&chats) {
            if chats.iter().any(|chat| mine.contains(chat)) {
                shared.extend(channel.agents.iter().cloned());
            }
        }
        shared
    }

    /// Every agent below `root` in the parent tree, in any state, through the
    /// parent index. `root` itself is included only when a parent cycle leads
    /// back to it.
    pub fn descendant_ids(&self, root: &AgentId) -> Result<HashSet<AgentId>, Error> {
        let conn = self.read_conn();
        // UNION (not UNION ALL) deduplicates, so a parent cycle ends.
        let ids = conn.db_query_vec(
            "WITH RECURSIVE below(id) AS (
                 SELECT id FROM agents WHERE parent_id = ?1
                 UNION
                 SELECT a.id FROM agents a JOIN below b ON a.parent_id = b.id
             )
             SELECT id FROM below",
            &db_params![root],
            |r| r.get::<AgentId>(0),
        )?;
        Ok(ids.into_iter().collect())
    }

    /// The id of every agent, archived ones included, without decoding rows.
    pub fn agent_ids(&self) -> Result<Vec<AgentId>, Error> {
        let conn = self.read_conn();
        conn.db_query_vec("SELECT id FROM agents ORDER BY name", &[], |r| r.get(0))
    }

    /// Every archived agent that has a parent, through the state index.
    pub fn archived_subagent_ids(&self) -> Result<Vec<AgentId>, Error> {
        let conn = self.read_conn();
        conn.db_query_vec(
            "SELECT id FROM agents WHERE state = 'archived' AND parent_id IS NOT NULL",
            &[],
            |r| r.get(0),
        )
    }

    /// `(id, workdir)` of every agent that is not archived and names a workdir,
    /// without decoding rows. The two ranges let the state index skip the
    /// archive.
    pub fn unarchived_workdirs(&self) -> Result<Vec<(AgentId, String)>, Error> {
        let conn = self.read_conn();
        conn.db_query_vec(
            "SELECT id, workdir FROM agents
             WHERE (state < 'archived' OR state > 'archived') AND workdir IS NOT NULL",
            &[],
            |r| Ok((r.get::<AgentId>(0)?, r.get::<String>(1)?)),
        )
    }

    /// Whether this agent's sleep consolidation has taken in every message it
    /// received and sent: the watermarks in its sleep state reach its mailbox.
    pub fn sleep_memory_caught_up(&self, agent_id: &AgentId) -> Result<bool, Error> {
        let agent_root = self.data_dir().join("agents").join(agent_id.as_str());
        let sleep = crate::learned_memory::read_sleep_state(&agent_root);
        let conn = self.read_conn();
        let max_in_seq: i64 = conn.db_query_one(
            "SELECT COALESCE(MAX(seq), 0) FROM messages WHERE agent_id = ?1",
            &db_params![agent_id],
            |r| r.get(0),
        )?;
        let max_out_ms: i64 = conn.db_query_one(
            "SELECT COALESCE(MAX(sent_at_ms), 0) FROM outbound_messages WHERE agent_id = ?1",
            &db_params![agent_id],
            |r| r.get(0),
        )?;
        Ok(max_in_seq <= sleep.last_inbound_seq && max_out_ms <= sleep.last_outbound_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::super::{NewAgent, NewChannel};
    use super::*;

    #[test]
    fn sharing_set_matches_the_pairwise_check() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let names = [
            "caller",
            "peer",
            "chatmate",
            "stranger",
            "loner",
            "other-chat",
        ];
        let agents: Vec<_> = names
            .iter()
            .map(|name| store.create_agent(NewAgent::new(*name, "codex")).unwrap())
            .collect();
        let [caller, peer, chatmate, stranger, loner, other_chat] = &agents[..] else {
            unreachable!()
        };
        store.join_group("team", &caller.id).unwrap();
        store.join_group("team", &peer.id).unwrap();
        store.join_group("solo", &loner.id).unwrap();
        let channel = store
            .create_channel(NewChannel {
                kind: "telegram".into(),
                identity: None,
                config: "{}".into(),
                credential_ref: None,
            })
            .unwrap();
        store.attach_channel(&channel.id, &caller.id).unwrap();
        store.attach_channel(&channel.id, &chatmate.id).unwrap();
        store
            .set_channel_state(
                &channel.id,
                r#"{"chats":[{"agent":"chatmate","chat_id":-1001}]}"#,
            )
            .unwrap();
        let second = store
            .create_channel(NewChannel {
                kind: "telegram".into(),
                identity: Some("second_bot".into()),
                config: "{}".into(),
                credential_ref: None,
            })
            .unwrap();
        store.attach_channel(&second.id, &other_chat.id).unwrap();
        store
            .set_channel_state(
                &second.id,
                r#"{"chats":[{"agent":"other-chat","chat_id":-2002}]}"#,
            )
            .unwrap();
        let unreadable = store
            .create_channel(NewChannel {
                kind: "telegram".into(),
                identity: Some("broken_bot".into()),
                config: "{}".into(),
                credential_ref: None,
            })
            .unwrap();
        store.attach_channel(&unreadable.id, &stranger.id).unwrap();
        store.attach_channel(&unreadable.id, &caller.id).unwrap();
        store.set_channel_state(&unreadable.id, "[]").unwrap();

        for a in &agents {
            let shared = store.agents_sharing_group_or_chat(&a.id);
            for b in &agents {
                assert_eq!(
                    shared.contains(&b.id),
                    store.share_any_group_or_chat(&a.id, &b.id),
                    "{} and {}",
                    a.name,
                    b.name
                );
            }
        }
        let shared = store.agents_sharing_group_or_chat(&caller.id);
        assert!(shared.contains(&peer.id) && shared.contains(&chatmate.id));
        assert!(!shared.contains(&stranger.id) && !shared.contains(&other_chat.id));
    }

    #[test]
    fn descendants_follow_the_parent_index_across_states() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let child_of = |name: &str, parent: Option<&AgentId>| {
            let mut new = NewAgent::new(name, "codex");
            new.parent_id = parent.cloned();
            store.create_agent(new).unwrap()
        };
        let root = child_of("root", None);
        let a = child_of("a", Some(&root.id));
        let b = child_of("b", Some(&a.id));
        let c = child_of("c", Some(&root.id));
        let other = child_of("other", None);
        let stranger = child_of("stranger", Some(&other.id));
        store.set_agent_state(&b.id, "archived").unwrap();

        let below = store.descendant_ids(&root.id).unwrap();
        assert_eq!(
            below,
            HashSet::from([a.id.clone(), b.id.clone(), c.id.clone()])
        );
        assert_eq!(
            store.descendant_ids(&a.id).unwrap(),
            HashSet::from([b.id.clone()])
        );
        assert!(store.descendant_ids(&b.id).unwrap().is_empty());
        assert!(!store.descendant_ids(&other.id).unwrap().contains(&a.id));
        assert!(
            store
                .descendant_ids(&other.id)
                .unwrap()
                .contains(&stranger.id)
        );

        let listed: Vec<_> = store
            .list_agents()
            .unwrap()
            .into_iter()
            .map(|a| a.id)
            .collect();
        assert_eq!(listed.len(), 6);
        assert_eq!(store.agent_ids().unwrap(), listed);
    }

    #[test]
    fn unarchived_workdirs_skip_the_archive_and_rows_without_one() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let with_workdir = |name: &str, workdir: Option<&str>| {
            let mut new = NewAgent::new(name, "codex");
            new.workdir = workdir.map(str::to_string);
            store.create_agent(new).unwrap()
        };
        let live = with_workdir("live", Some("/work/live"));
        let paused = with_workdir("paused", Some("/work/paused"));
        let archived = with_workdir("archived", Some("/work/archived"));
        with_workdir("none", None);
        store.set_agent_state(&paused.id, "paused").unwrap();
        store.set_agent_state(&archived.id, "archived").unwrap();
        let mut workdirs = store.unarchived_workdirs().unwrap();
        workdirs.sort_by(|a, b| a.1.cmp(&b.1));
        assert_eq!(
            workdirs,
            [
                (live.id, "/work/live".to_string()),
                (paused.id, "/work/paused".to_string())
            ]
        );
    }

    #[test]
    fn archived_subagents_skip_live_children_and_archived_roots() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let root = store.create_agent(NewAgent::new("root", "codex")).unwrap();
        let child = |name: &str| {
            let mut new = NewAgent::new(name, "codex");
            new.parent_id = Some(root.id.clone());
            store.create_agent(new).unwrap()
        };
        let archived = child("archived");
        child("live");
        store.set_agent_state(&archived.id, "archived").unwrap();
        store.set_agent_state(&root.id, "archived").unwrap();
        assert_eq!(store.archived_subagent_ids().unwrap(), [archived.id]);
    }
}
