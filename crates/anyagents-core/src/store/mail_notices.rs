//! Notices AnyAgents writes into a mailbox itself, so that no message waits or
//! stays unread without someone being told (`docs/delivery.md`, "Mail that
//! cannot be delivered now").
//!
//! - Mail queued to a **paused** agent: its sender, when the sender is an
//!   agent, or else the paused agent's parent, is told once per pause: the key
//!   names the paused agent and its delivery cursor, which a pause holds
//!   still, so the next pause (after the cursor moved) is told again.
//! - Mail stored for an **archived** agent without waking it: its agent
//!   sender is told, once per message, that it will not be read unless the
//!   agent is resumed, and who the agent's parent is.
//! - An agent **parked** after errored turns: its nearest live ancestor is
//!   told, or the operator when it has none.
//!
//! Every notice comes from [`SYSTEM_SENDER`] and is keyed by its source key,
//! so it is written once. A notice never causes another notice.

use super::{Agent, Message, ReceiveOutcome, Store};
use crate::error::Error;
use crate::id::AgentId;
use crate::store::Incoming;

/// The sender of every notice AnyAgents writes itself.
pub const SYSTEM_SENDER: &str = "anyagents";

/// Lifecycle notices a child's state produces on its own (`[Sub-Agent
/// Completed]`, `[Sub-Agent Error]`). Their sender did not write them, so a
/// pause or an archive of their recipient is not announced back to it.
fn is_lifecycle_notice(message: &Message) -> bool {
    message.source_key.as_deref().is_some_and(|key| {
        key.starts_with("subagent-error:") || key.starts_with("subagent_completed:")
    })
}

/// The parent sentence of a notice.
fn parent_clause(parent: Option<&Agent>) -> String {
    match parent {
        Some(parent) => format!("its parent is `{}` (`{}`)", parent.name, parent.id),
        None => "it has no parent; the operator resumes it".to_string(),
    }
}

impl Store {
    /// Write one notice into `to`'s mailbox, once per `key`. `true` when it
    /// is new.
    pub fn post_notice(&self, to: &AgentId, key: String, text: String) -> Result<bool, Error> {
        let mut incoming = Incoming::new(text);
        incoming.sender = Some(SYSTEM_SENDER.to_string());
        incoming.source_key = Some(key);
        Ok(matches!(
            self.receive(to, incoming)?,
            ReceiveOutcome::New(_)
        ))
    }

    /// The agent's nearest ancestor that is not archived.
    pub fn live_ancestor(&self, agent: &Agent) -> Result<Option<Agent>, Error> {
        let mut next = agent.parent_id.clone();
        // A cycle cannot be written, but a bound costs nothing.
        for _ in 0..64 {
            let Some(id) = next else {
                return Ok(None);
            };
            let parent = match self.agent(&id) {
                Ok(parent) => parent,
                Err(Error::AgentNotFound(_)) => return Ok(None),
                Err(err) => return Err(err),
            };
            if parent.state != "archived" {
                return Ok(Some(parent));
            }
            next = parent.parent_id.clone();
        }
        Ok(None)
    }

    /// The agent a message's `agent:` sender names, when it still exists.
    fn sender_agent(&self, message: &Message) -> Option<Agent> {
        let reference = message.sender.as_deref()?.trim().strip_prefix("agent:")?;
        self.agent_by_name(reference).ok()
    }

    /// Announce a message that waits behind a pause or that an archived agent
    /// will not read. A failure is reported and never fails the receive: the
    /// message itself is stored.
    pub(super) fn announce_held_mail(&self, message: &Message, recipient: &Agent) {
        if message.sender.as_deref() == Some(SYSTEM_SENDER) || is_lifecycle_notice(message) {
            return;
        }
        let result = match recipient.state.as_str() {
            "paused" => self.announce_paused_mail(message, recipient),
            "archived" => self.announce_archived_mail(message, recipient),
            _ => Ok(()),
        };
        if let Err(err) = result {
            eprintln!(
                "anyagents: notice for mail held by {} ({}): {err}",
                recipient.name, recipient.id
            );
        }
    }

    fn announce_paused_mail(&self, message: &Message, recipient: &Agent) -> Result<(), Error> {
        let parent = self.live_ancestor(recipient)?;
        let cursor = self.delivered_cursor(&recipient.id)?;
        let key = format!("anyagents:paused-mail:{}:{cursor}", recipient.id);
        let why = match self
            .turn_errors(&recipient.id)?
            .filter(|errors| errors.faulted_at_ms.is_some())
        {
            Some(errors) => format!(
                "It was paused because its turns kept ending in an error: {}.",
                errors.last_error.as_deref().unwrap_or("unknown error")
            ),
            None => "It was paused.".to_string(),
        };
        let sender = self
            .sender_agent(message)
            .filter(|sender| sender.id != recipient.id && sender.state != "archived");
        if let Some(sender) = sender {
            self.post_notice(
                &sender.id,
                key,
                format!(
                    "[Queued] `{}` (`{}`) is paused, so your message anyagents-message:{} waits in its mailbox and is delivered when it is resumed. {why} Further mail to it is not announced again while it stays paused; {}.",
                    recipient.name,
                    recipient.id,
                    message.id,
                    parent_clause(parent.as_ref()),
                ),
            )?;
            return Ok(());
        }
        // Mail from a person, a schedule or the operator: the parent decides.
        if let Some(parent) = parent {
            self.post_notice(
                &parent.id,
                key,
                format!(
                    "[Queued] Your sub-agent `{}` (`{}`) is paused, and mail is waiting for it: anyagents-message:{} from {}. {why} It is delivered when the sub-agent is resumed. Further mail to it is not announced again while it stays paused.",
                    recipient.name,
                    recipient.id,
                    message.id,
                    message.sender.as_deref().unwrap_or("an unnamed sender"),
                ),
            )?;
        }
        Ok(())
    }

    fn announce_archived_mail(&self, message: &Message, recipient: &Agent) -> Result<(), Error> {
        let Some(sender) = self
            .sender_agent(message)
            .filter(|sender| sender.id != recipient.id && sender.state != "archived")
        else {
            return Ok(());
        };
        let parent = self.live_ancestor(recipient)?;
        self.post_notice(
            &sender.id,
            format!("anyagents:archived-mail:{}", message.id),
            format!(
                "[Undeliverable] Your message anyagents-message:{} to `{}` was not delivered: agent `{}` is archived; {}. It stays in that mailbox and is read only if the agent is resumed.",
                message.id,
                recipient.name,
                recipient.name,
                parent_clause(parent.as_ref()),
            ),
        )?;
        Ok(())
    }

    /// Tell whoever answers for a parked agent that it was parked, with the
    /// error: its nearest live ancestor, or the operator (the server log, and
    /// `anyagents status`) when it has none. Returns the ancestor told.
    pub fn notify_parked(
        &self,
        agent_id: &AgentId,
        streak: i64,
        error: &str,
    ) -> Result<Option<AgentId>, Error> {
        let agent = self.agent(agent_id)?;
        let faulted_at = self
            .turn_errors(agent_id)?
            .and_then(|errors| errors.faulted_at_ms)
            .unwrap_or(0);
        let Some(ancestor) = self.live_ancestor(&agent)? else {
            eprintln!(
                "anyagents: operator attention: agent {} ({}) was paused after {streak} turns in a row ended in an error; its mail is held until it is resumed. Last error: {error}",
                agent.name, agent.id
            );
            return Ok(None);
        };
        let relation = if agent.parent_id.as_ref() == Some(&ancestor.id) {
            "Your sub-agent"
        } else {
            "A descendant agent"
        };
        self.post_notice(
            &ancestor.id,
            format!("anyagents:faulted:{}:{faulted_at}", agent.id),
            format!(
                "[Sub-Agent Faulted] {relation} `{}` (`{}`) was paused: {streak} turns in a row ended in an error. Its mail is held until it is resumed. Last error: {}\n\nFix the cause (or decide the work is done), then resume it (`anyagents agents resume {}`) or archive it.",
                agent.name,
                agent.id,
                error.trim(),
                agent.name,
            ),
        )?;
        Ok(Some(ancestor.id))
    }
}

#[cfg(test)]
mod tests {
    use super::SYSTEM_SENDER;
    use crate::{Agent, Incoming, Message, NewAgent, ReceiveOutcome, Store};

    fn store() -> (tempfile::TempDir, Store) {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        (dir, store)
    }

    fn child(store: &Store, name: &str, parent: &Agent) -> Agent {
        store
            .create_agent(NewAgent {
                parent_id: Some(parent.id.clone()),
                ..NewAgent::new(name, "spiron")
            })
            .unwrap()
    }

    fn send(store: &Store, to: &Agent, from: &Agent, text: &str) -> Message {
        let mut incoming = Incoming::new(text);
        incoming.sender = Some(format!("agent:{}", from.id));
        match store.receive(&to.id, incoming).unwrap() {
            ReceiveOutcome::New(message) => message,
            ReceiveOutcome::Duplicate(_) => panic!("duplicate"),
        }
    }

    fn notices(store: &Store, agent: &Agent) -> Vec<Message> {
        store
            .inbox(&agent.id, None)
            .unwrap()
            .into_iter()
            .filter(|m| m.sender.as_deref() == Some(SYSTEM_SENDER))
            .collect()
    }

    #[test]
    fn mail_to_a_paused_agent_tells_each_agent_sender_once_per_pause() {
        let (_dir, store) = store();
        let lead = store.create_agent(NewAgent::new("lead", "spiron")).unwrap();
        let worker = child(&store, "worker", &lead);
        let peer = store.create_agent(NewAgent::new("peer", "spiron")).unwrap();
        store.set_agent_state(&worker.id, "paused").unwrap();

        let first = send(&store, &worker, &peer, "one");
        send(&store, &worker, &peer, "two");
        let told = notices(&store, &peer);
        assert_eq!(told.len(), 1, "{told:?}");
        assert!(
            told[0].text.starts_with("[Queued] `worker`"),
            "{}",
            told[0].text
        );
        assert!(told[0].text.contains(first.id.as_str()));
        assert!(told[0].text.contains("its parent is `lead`"));
        assert!(
            notices(&store, &lead).is_empty(),
            "an agent sender is told, not the parent"
        );
        assert_eq!(
            store.agent(&worker.id).unwrap().state,
            "paused",
            "the mail is held, not delivered"
        );

        // Resumed and delivered past, then paused again: a new pause is told.
        store.set_agent_state(&worker.id, "running").unwrap();
        for message in store.inbox(&worker.id, None).unwrap() {
            store.note_delivered(&worker.id, message.seq).unwrap();
        }
        store.set_agent_state(&worker.id, "paused").unwrap();
        send(&store, &worker, &peer, "three");
        assert_eq!(notices(&store, &peer).len(), 2);
    }

    #[test]
    fn mail_from_a_person_to_a_paused_agent_tells_its_parent_once() {
        let (_dir, store) = store();
        let lead = store.create_agent(NewAgent::new("lead", "spiron")).unwrap();
        let worker = child(&store, "worker", &lead);
        store.set_agent_state(&worker.id, "paused").unwrap();
        for text in ["a", "b"] {
            let mut incoming = Incoming::new(text);
            incoming.sender = Some("telegram:user:42".into());
            store.receive(&worker.id, incoming).unwrap();
        }
        let told = notices(&store, &lead);
        assert_eq!(told.len(), 1);
        assert!(told[0].text.contains("Your sub-agent `worker`"));
        assert!(told[0].text.contains("telegram:user:42"));
    }

    #[test]
    fn mail_stored_for_an_archived_agent_bounces_to_its_agent_sender() {
        let (_dir, store) = store();
        let lead = store.create_agent(NewAgent::new("lead", "spiron")).unwrap();
        let worker = child(&store, "worker", &lead);
        store.set_agent_state(&worker.id, "archived").unwrap();
        let reply = send(&store, &worker, &lead, "status?");
        let told = notices(&store, &lead);
        assert_eq!(told.len(), 1);
        assert!(
            told[0]
                .text
                .contains("agent `worker` is archived; it has no parent")
                || told[0]
                    .text
                    .contains("agent `worker` is archived; its parent is"),
            "{}",
            told[0].text
        );
        assert!(told[0].text.contains(reply.id.as_str()));
        // Stored, so a resume still delivers it.
        assert_eq!(
            store.next_undelivered(&worker.id).unwrap().unwrap().id,
            reply.id
        );
    }

    #[test]
    fn automatic_and_system_notices_are_never_announced() {
        let (_dir, store) = store();
        let root = store.create_agent(NewAgent::new("root", "spiron")).unwrap();
        let lead = child(&store, "lead", &root);
        let worker = child(&store, "worker", &lead);
        store.set_agent_state(&lead.id, "paused").unwrap();
        let mut lifecycle = Incoming::new("[Sub-Agent Completed] worker");
        lifecycle.sender = Some(format!("agent:{}", worker.id));
        lifecycle.source_key = Some(format!("subagent_completed:{}:1", worker.id));
        store.receive(&lead.id, lifecycle).unwrap();
        store
            .post_notice(&lead.id, "anyagents:test".into(), "system".into())
            .unwrap();
        assert!(notices(&store, &worker).is_empty());
        assert!(notices(&store, &root).is_empty());
    }

    #[test]
    fn a_parked_agent_is_announced_to_its_nearest_live_ancestor() {
        let (_dir, store) = store();
        let root = store.create_agent(NewAgent::new("root", "spiron")).unwrap();
        let lead = child(&store, "lead", &root);
        let worker = child(&store, "worker", &lead);
        store.set_agent_state(&lead.id, "archived").unwrap();
        store.set_agent_state(&worker.id, "running").unwrap();
        store.park_faulted(&worker.id).unwrap();
        let told = store
            .notify_parked(&worker.id, 3, "context_length_exceeded")
            .unwrap();
        assert_eq!(told, Some(root.id.clone()));
        let notice = notices(&store, &root);
        assert_eq!(notice.len(), 1);
        assert!(
            notice[0]
                .text
                .starts_with("[Sub-Agent Faulted] A descendant agent `worker`")
        );
        assert!(notice[0].text.contains("context_length_exceeded"));
        // Once per parking.
        store
            .notify_parked(&worker.id, 3, "context_length_exceeded")
            .unwrap();
        assert_eq!(notices(&store, &root).len(), 1);
        // A root agent has no ancestor: the operator is told instead.
        store.set_agent_state(&root.id, "running").unwrap();
        store.park_faulted(&root.id).unwrap();
        assert_eq!(store.notify_parked(&root.id, 3, "boom").unwrap(), None);
    }
}
