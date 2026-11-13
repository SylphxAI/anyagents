//! Narrow reads for the server's request and maintenance paths: one indexed
//! query where they would otherwise decode every agent row or ask once per
//! listed agent.

use std::collections::HashSet;

use super::{AgentId, Store, telegram_chat_links};

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
}
