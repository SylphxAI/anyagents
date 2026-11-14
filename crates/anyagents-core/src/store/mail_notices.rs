//! Notices AnyAgents writes into a mailbox itself, so that no message waits or
//! stays unread without someone being told (`docs/delivery.md`, "Mail that
//! cannot be delivered now").
//!
//! - Mail queued to a **paused** agent: its sender, when the sender is an
//!   agent, or else the paused agent's parent, is told once per pause: the key
//!   names the paused agent and its delivery cursor, which a pause holds
//!   still, so the next pause (after the cursor moved) is told again. An
//!   answer to something the paused agent asked is not announced.
//! - Mail stored for an **archived** agent without waking it: its agent
//!   sender is told, once per message, that it will not be read unless the
//!   agent is resumed, and who the agent's parent is.
//! - An agent **parked** after errored turns: its nearest live ancestor is
//!   told, or, when it has none, the operator's inbox.
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

/// Who answers for `agent`, as a notice says it: its parent, or the
/// nearest running or idle agent above it.
fn answers_for(agent: &Agent, ancestor: Option<&Agent>) -> String {
    match ancestor {
        Some(parent) if agent.parent_id.as_ref() == Some(&parent.id) => {
            format!("its parent is `{}` (`{}`)", parent.name, parent.id)
        }
        Some(above) => format!(
            "the nearest running agent above it is `{}` (`{}`)",
            above.name, above.id
        ),
        None => "no running agent is above it; the operator resumes it".to_string(),
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

    /// The agent's nearest ancestor that runs or can wake on mail (`running`
    /// or `idle`). A paused or archived ancestor reads nothing, so a notice
    /// for it goes past it, and the operator's inbox answers when no
    /// ancestor does.
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
            if matches!(parent.state.as_str(), "running" | "idle") {
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
        // An answer to what the paused agent asked waits for it; whoever
        // answered has nothing to act on.
        if message.reply_to.is_some() {
            return Ok(());
        }
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
                    answers_for(recipient, parent.as_ref()),
                ),
            )?;
            return Ok(());
        }
        // Mail from a person, a schedule or the operator: whoever answers
        // for the paused agent decides.
        match parent {
            Some(parent) => {
                let relation = if recipient.parent_id.as_ref() == Some(&parent.id) {
                    "Your sub-agent"
                } else {
                    "An agent below you"
                };
                self.post_notice(
                    &parent.id,
                    key,
                    format!(
                        "[Queued] {relation} `{}` (`{}`) is paused, and mail is waiting for it: anyagents-message:{} from {}. {why} It is delivered when that agent is resumed. Further mail to it is not announced again while it stays paused.",
                        recipient.name,
                        recipient.id,
                        message.id,
                        message.sender.as_deref().unwrap_or("an unnamed sender"),
                    ),
                )?;
            }
            // One body per agent, so the inbox counts repeats on it.
            None => self.tell_operator(
                recipient,
                format!(
                    "[Queued] `{}` (`{}`) is paused, and mail is waiting for it. {why} It is delivered when the agent is resumed: `anyagents agents resume {}`.",
                    recipient.name, recipient.id, recipient.name,
                ),
            )?,
        }
        Ok(())
    }

    /// Post `body` about `agent` to the operator's inbox. Over the inbox's
    /// rate limit nothing is stored; the agent's state still shows in
    /// `anyagents status`, and the server log says so.
    fn tell_operator(&self, agent: &Agent, body: String) -> Result<(), Error> {
        let receipt = self.report_to_operator(super::OperatorReport {
            sender: format!("agent:{}", agent.id),
            sender_name: Some(agent.name.clone()),
            body,
            received_at_ms: None,
        })?;
        if let super::OperatorReceipt::Limited { retry_after_ms } = receipt {
            eprintln!(
                "anyagents: operator inbox is rate limited ({retry_after_ms} ms); notice about {} ({}) not stored, `anyagents status` shows its state",
                agent.name, agent.id
            );
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
                answers_for(recipient, parent.as_ref()),
            ),
        )?;
        Ok(())
    }

    /// Tell whoever answers for a parked agent that it was parked, with the
    /// error: its nearest live ancestor, or, when it has none, the operator's
    /// inbox (`anyagents inbox --operator`; also the server log and
    /// `anyagents status`). Returns the ancestor told.
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
            // No agent answers for it: the operator's inbox does.
            eprintln!(
                "anyagents: operator attention: agent {} ({}) was paused after {streak} turns in a row ended in an error; its mail is held until it is resumed. Last error: {error}",
                agent.name, agent.id
            );
            self.tell_operator(
                &agent,
                format!(
                    "[Agent Faulted] `{}` (`{}`) was paused: {streak} turns in a row ended in an error of its own session. Its mail is held until it is resumed. Last error: {}\n\nFix the cause, then resume it: `anyagents agents resume {}`, or every faulted agent at once: `anyagents resume --faulted`.",
                    agent.name,
                    agent.id,
                    error.trim(),
                    agent.name,
                ),
            )?;
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
                "[Sub-Agent Faulted] {relation} `{}` (`{}`) was paused: {streak} turns in a row ended in an error of its own session. Its mail is held until it is resumed. Last error: {}\n\nFix the cause (or decide the work is done), then resume it (`anyagents agents resume {}`) or archive it.",
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

    /// An agent that runs, as one a notice can reach.
    fn root(store: &Store, name: &str) -> Agent {
        let agent = store.create_agent(NewAgent::new(name, "spiron")).unwrap();
        store.set_agent_state(&agent.id, "running").unwrap()
    }

    fn child(store: &Store, name: &str, parent: &Agent) -> Agent {
        let agent = store
            .create_agent(NewAgent {
                parent_id: Some(parent.id.clone()),
                ..NewAgent::new(name, "spiron")
            })
            .unwrap();
        store.set_agent_state(&agent.id, "running").unwrap()
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
        let lead = root(&store, "lead");
        let worker = child(&store, "worker", &lead);
        let peer = root(&store, "peer");
        store.set_agent_state(&worker.id, "paused").unwrap();

        // An answer to what the paused agent asked tells no one.
        let mut answer = Incoming::new("pong");
        answer.sender = Some(format!("agent:{}", peer.id));
        answer.reply_to = Some("msg_asked_earlier".into());
        store.receive(&worker.id, answer).unwrap();
        assert!(notices(&store, &peer).is_empty());
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
        let lead = root(&store, "lead");
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
        let lead = root(&store, "lead");
        let worker = child(&store, "worker", &lead);
        store.set_agent_state(&worker.id, "archived").unwrap();
        let reply = send(&store, &worker, &lead, "status?");
        let told = notices(&store, &lead);
        assert_eq!(told.len(), 1);
        assert!(
            told[0]
                .text
                .contains("agent `worker` is archived; its parent is `lead`"),
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
        let root = root(&store, "root");
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

    /// A paused ancestor reads nothing: a notice goes past it to the nearest
    /// running one, or to the operator's inbox when none runs.
    #[test]
    fn notices_pass_paused_ancestors_and_end_with_the_operator() {
        let (_dir, store) = store();
        let top = root(&store, "top");
        let lead = child(&store, "lead", &top);
        let worker = child(&store, "worker", &lead);
        store.set_agent_state(&lead.id, "paused").unwrap();
        store.set_agent_state(&worker.id, "paused").unwrap();
        let mut human = Incoming::new("hello");
        human.sender = Some("telegram:user:42".into());
        store.receive(&worker.id, human.clone()).unwrap();
        assert!(
            notices(&store, &lead).is_empty(),
            "a paused parent is passed"
        );
        let told = notices(&store, &top);
        assert_eq!(told.len(), 1, "{told:?}");
        assert!(
            told[0]
                .text
                .starts_with("[Queued] An agent below you `worker`")
        );

        // With no running ancestor, the operator's inbox is told.
        store.set_agent_state(&top.id, "paused").unwrap();
        let loner = child(&store, "loner", &lead);
        store.set_agent_state(&loner.id, "paused").unwrap();
        store.receive(&loner.id, human).unwrap();
        let reports = store.operator_inbox(None, None).unwrap();
        assert_eq!(reports.len(), 1);
        assert!(
            reports[0].body.starts_with("[Queued] `loner`"),
            "{}",
            reports[0].body
        );

        // The same holds for a parking.
        store.set_agent_state(&worker.id, "running").unwrap();
        store.park_faulted(&worker.id).unwrap();
        assert_eq!(
            store
                .notify_parked(&worker.id, 3, "context_length_exceeded")
                .unwrap(),
            None
        );
        let reports = store.operator_inbox(None, None).unwrap();
        assert_eq!(reports.len(), 2);
        assert!(reports[1].body.starts_with("[Agent Faulted] `worker`"));
    }

    #[test]
    fn a_parked_agent_is_announced_to_its_nearest_live_ancestor() {
        let (_dir, store) = store();
        let root = root(&store, "root");
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
        // A root agent has no ancestor: the operator's inbox is told instead.
        store.set_agent_state(&root.id, "running").unwrap();
        store.park_faulted(&root.id).unwrap();
        assert_eq!(store.notify_parked(&root.id, 3, "boom").unwrap(), None);
        let reports = store.operator_inbox(None, None).unwrap();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].sender, format!("agent:{}", root.id));
        assert!(reports[0].body.starts_with("[Agent Faulted] `root`"));
        assert!(reports[0].body.contains("boom"));
    }
}
