//! Narrow reads for the server's request and maintenance paths: one indexed
//! query where they would otherwise decode every agent row or ask once per
//! listed agent.

use std::collections::HashSet;

use super::{AgentId, Error, Store, telegram_chat_links};

impl Store {
    /// Every agent that shares an internal group or a Telegram chat with
    /// `agent`: exactly the `other` for which
    /// [`Store::share_any_group_or_chat`]`(agent, other)` holds, read with one
    /// query and one channel listing. A read error shares nothing, as there.
    pub fn agents_sharing_group_or_chat(&self, agent: &AgentId) -> HashSet<AgentId> {
        let mut shared = HashSet::new();
        {
            let conn = self.read_conn();
            if let Ok(mut stmt) = conn.prepare_cached(
                "SELECT DISTINCT g2.agent_id FROM group_agents g1
                 JOIN group_agents g2 ON g1.group_name = g2.group_name
                 WHERE g1.agent_id = ?1",
            ) && let Ok(rows) = stmt.query_map([agent.as_str()], |r| r.get::<_, String>(0))
            {
                shared.extend(rows.flatten().filter_map(|id| AgentId::parse(&id).ok()));
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
        let mut stmt = conn.prepare_cached(
            "WITH RECURSIVE below(id) AS (
                 SELECT id FROM agents WHERE parent_id = ?1
                 UNION
                 SELECT a.id FROM agents a JOIN below b ON a.parent_id = b.id
             )
             SELECT id FROM below",
        )?;
        let rows = stmt.query_map([root.as_str()], |r| r.get::<_, String>(0))?;
        let mut ids = HashSet::new();
        for id in rows {
            ids.insert(AgentId::parse(&id?)?);
        }
        Ok(ids)
    }

    /// The id of every agent, archived ones included, without decoding rows.
    pub fn agent_ids(&self) -> Result<Vec<AgentId>, Error> {
        let conn = self.read_conn();
        let mut stmt = conn.prepare_cached("SELECT id FROM agents ORDER BY name")?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        let mut ids = Vec::new();
        for id in rows {
            ids.push(AgentId::parse(&id?)?);
        }
        Ok(ids)
    }

    /// Every archived agent that has a parent, through the state index.
    pub fn archived_subagent_ids(&self) -> Result<Vec<AgentId>, Error> {
        let conn = self.read_conn();
        let mut stmt = conn.prepare_cached(
            "SELECT id FROM agents WHERE state = 'archived' AND parent_id IS NOT NULL",
        )?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        let mut ids = Vec::new();
        for id in rows {
            ids.push(AgentId::parse(&id?)?);
        }
        Ok(ids)
    }

    /// `(id, workdir)` of every agent that is not archived and names a workdir,
    /// without decoding rows. The two ranges let the state index skip the
    /// archive.
    pub fn unarchived_workdirs(&self) -> Result<Vec<(AgentId, String)>, Error> {
        let conn = self.read_conn();
        let mut stmt = conn.prepare_cached(
            "SELECT id, workdir FROM agents
             WHERE (state < 'archived' OR state > 'archived') AND workdir IS NOT NULL",
        )?;
        let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
        let mut workdirs = Vec::new();
        for row in rows {
            let (id, workdir) = row?;
            workdirs.push((AgentId::parse(&id)?, workdir));
        }
        Ok(workdirs)
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
