//! Delivery loop. It hands each message past the accepted cursor to the harness,
//! in sequence order, and moves the cursors only after the harness answers.

use anyagents_core::{Agent, AgentId, Incoming, Message, Store};
use anyagents_harness::{DeliverOutcome, Harness, HarnessError, OutboundMessage, message_id_in_text};

pub use anyagents_harness::{DeliveryMode, SteerSupport, TurnState};

/// Why one step of the loop stopped.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Store(#[from] anyagents_core::Error),
    #[error("harness process is not running")]
    HarnessDown,
    #[error("harness failed: {0}")]
    HarnessFailed(String),
}

impl Error {
    /// A second loop for this agent was refused.
    pub fn agent_busy(&self) -> bool {
        matches!(self, Self::Store(anyagents_core::Error::AgentBusy))
    }
}

/// What one pass over the mailbox did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RunOutcome {
    Idle,
    Delivered {
        accepted: u64,
        /// Highest sequence marked processed during this pass, or 0 when no turn finished.
        processed_through: i64,
    },
    Deferred {
        seq: i64,
    },
    Quota {
        reset_at_ms: i64,
    },
}

/// Health of one agent: the harness report plus mailbox lag.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentHealth {
    pub process_alive: bool,
    pub turn_state: TurnState,
    pub delivery_mode: DeliveryMode,
    pub steer_support: SteerSupport,
    pub quota_reset_at_ms: Option<i64>,
    pub sign_in_fault: Option<String>,
    pub oldest_unaccepted_age_ms: Option<i64>,
    pub oldest_unprocessed_age_ms: Option<i64>,
}

/// One running agent: the mailbox, the per-agent lock, and its harness.
pub struct DeliveryLoop<H: Harness> {
    store: Store,
    agent_id: AgentId,
    harness: H,
    // Held so the process lock stays until this loop is dropped.
    _lock: anyagents_core::AgentLock,
    quota_reset_at_ms: Option<i64>,
}

/// The vendor echoes the marked prompt as the first line. The marker line is
/// not part of the answer.
fn answer_text(text: &str) -> String {
    let mut lines = text.lines();
    let Some(first) = lines.next() else {
        return String::new();
    };
    if message_id_in_text(first).is_some() {
        lines.collect::<Vec<_>>().join("\n").trim().to_string()
    } else {
        text.trim().to_string()
    }
}

impl<H: Harness> DeliveryLoop<H> {
    /// Take the per-agent lock and refuse a second loop for the same agent.
    pub fn start(store: Store, agent_id: AgentId, harness: H) -> Result<Self, Error> {
        store.agent(&agent_id)?;
        let lock = store.try_lock_agent(&agent_id)?;
        Ok(Self {
            store,
            agent_id,
            harness,
            _lock: lock,
            quota_reset_at_ms: None,
        })
    }

    /// Hand the next unaccepted message to the harness and do not move a cursor.
    /// Dropping the loop after this is the crash between accept and its commit.
    pub fn deliver_next_without_commit(&mut self) -> Result<Option<DeliverOutcome>, Error> {
        let Some(message) = self.store.next_unaccepted(&self.agent_id)? else {
            return Ok(None);
        };
        match self.harness.deliver(&outbound(&message)) {
            Ok(outcome) => Ok(Some(outcome)),
            Err(HarnessError::Dead) => Err(Error::HarnessDown),
            Err(HarnessError::Failed(reason)) => Err(Error::HarnessFailed(reason)),
        }
    }

    /// Deliver every unaccepted message until the mailbox is caught up, the harness
    /// defers, or it reports a quota reset. A quota answer is returned, not retried.
    pub fn run_once(&mut self) -> Result<RunOutcome, Error> {
        let mut accepted = 0u64;
        let mut processed_through = 0i64;
        loop {
            let Some(message) = self.store.next_unaccepted(&self.agent_id)? else {
                return Ok(if accepted == 0 {
                    RunOutcome::Idle
                } else {
                    RunOutcome::Delivered {
                        accepted,
                        processed_through,
                    }
                });
            };
            let seq = message.seq;
            match self.harness.deliver(&outbound(&message)) {
                Ok(DeliverOutcome::Accepted { turn_finished }) => {
                    self.store
                        .note_delivery(&self.agent_id, seq, turn_finished)?;
                    accepted += 1;
                    if turn_finished {
                        processed_through = seq;
                    }
                    self.quota_reset_at_ms = None;
                }
                Ok(DeliverOutcome::Deferred) => return Ok(RunOutcome::Deferred { seq }),
                Ok(DeliverOutcome::Quota { reset_at_ms }) => {
                    self.quota_reset_at_ms = Some(reset_at_ms);
                    return Ok(RunOutcome::Quota { reset_at_ms });
                }
                Err(HarnessError::Dead) => return Err(Error::HarnessDown),
                Err(HarnessError::Failed(reason)) => return Err(Error::HarnessFailed(reason)),
            }
        }
    }

    /// Record the harness's latest answer to the first message past the
    /// processed cursor, as a reply from the agent. A repeated answer is one row.
    pub fn record_answer(&self, agent: &Agent) -> Result<(), Error> {
        let seq = self.store.processed_cursor(&self.agent_id)?;
        let Some(message) = self
            .store
            .inbox(&self.agent_id, Some(seq))?
            .into_iter()
            .find(|message| message.seq > seq && message.reply_to.is_none())
        else {
            return Ok(());
        };
        let Some(text) = self.harness.answer_for(message.id.as_str()) else {
            return Ok(());
        };
        let text = answer_text(&text);
        if text.is_empty() {
            return Ok(());
        }
        self.store.receive(
            &self.agent_id,
            Incoming {
                text,
                sender: Some(format!("agent:{}", agent.name)),
                source_time_ms: None,
                source_key: Some(format!("answer:{}", message.id)),
                reply_to: Some(message.id.to_string()),
                received_at_ms: None,
            },
        )?;
        Ok(())
    }

    pub fn mark_processed_through(&self, seq: i64) -> Result<(), Error> {
        self.store.advance_processed_through(&self.agent_id, seq)?;
        Ok(())
    }

    pub fn health(&self, now_ms: i64) -> Result<AgentHealth, Error> {
        let harness = self.harness.health();
        let lag = self.store.lag(&self.agent_id, now_ms)?;
        Ok(AgentHealth {
            process_alive: harness.process_alive,
            turn_state: harness.turn_state,
            delivery_mode: harness.delivery_mode,
            steer_support: harness.steer_support,
            quota_reset_at_ms: harness.quota_reset_at_ms.or(self.quota_reset_at_ms),
            sign_in_fault: harness.sign_in_fault,
            oldest_unaccepted_age_ms: lag.oldest_unaccepted_age_ms,
            oldest_unprocessed_age_ms: lag.oldest_unprocessed_age_ms,
        })
    }

    /// Drop the store and the lock, keeping the harness so a restarted loop can dedupe.
    pub fn into_harness(self) -> H {
        self.harness
    }

    pub fn harness(&self) -> &H {
        &self.harness
    }

    pub fn harness_mut(&mut self) -> &mut H {
        &mut self.harness
    }

    /// Swap the vendor process without releasing the agent lock.
    pub fn replace_harness(&mut self, harness: H) {
        self.harness = harness;
        self.quota_reset_at_ms = None;
    }

    /// Clear a quota wait on the loop and on the harness.
    pub fn clear_quota(&mut self) {
        self.quota_reset_at_ms = None;
        self.harness.clear_quota();
    }

    pub fn store(&self) -> &Store {
        &self.store
    }
}

fn outbound(message: &Message) -> OutboundMessage {
    OutboundMessage {
        id: message.id.as_str().to_string(),
        seq: message.seq,
        text: message.text.clone(),
        sender: message.sender.clone(),
        reply_to: message.reply_to.clone(),
        source_time_ms: message.source_time_ms,
    }
}

#[cfg(test)]
mod tests {
    use anyagents_core::{Incoming, NewAgent, Store};
    use anyagents_harness::{DeliveryMode, FakeHarness};

    use super::*;

    fn setup(mode: DeliveryMode) -> (tempfile::TempDir, DeliveryLoop<FakeHarness>, AgentId) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("alpha", "claude-code"))
            .unwrap();
        let id = agent.id.clone();
        let loop_ = DeliveryLoop::start(store, agent.id, FakeHarness::new(mode)).unwrap();
        (dir, loop_, id)
    }

    fn recv(store: &Store, agent: &AgentId, text: &str) -> i64 {
        match store.receive(agent, Incoming::new(text)).unwrap() {
            anyagents_core::ReceiveOutcome::New(message) => message.seq,
            anyagents_core::ReceiveOutcome::Duplicate(_) => panic!("unexpected duplicate"),
        }
    }

    #[test]
    fn queue_holds_the_second_message_until_the_turn_ends() {
        let (_dir, mut loop_, id) = setup(DeliveryMode::Queue);
        let first = recv(loop_.store(), &id, "one");
        let second = recv(loop_.store(), &id, "two");
        assert_eq!(
            loop_.run_once().unwrap(),
            RunOutcome::Deferred { seq: second }
        );
        assert_eq!(loop_.harness().turns_started(), 1);
        assert_eq!(loop_.harness().written_ids().len(), 1);
        assert_eq!(loop_.harness_mut().complete_turn(), Some(first));
        loop_.mark_processed_through(first).unwrap();
        loop_.run_once().unwrap();
        assert_eq!(loop_.harness().written_ids().len(), 2);
        assert_eq!(loop_.harness().turns_started(), 2);
        assert_eq!(loop_.store().accepted_cursor(&id).unwrap(), second);
    }

    #[test]
    fn steer_puts_both_messages_in_one_turn() {
        let (_dir, mut loop_, id) = setup(DeliveryMode::Steer);
        recv(loop_.store(), &id, "one");
        let second = recv(loop_.store(), &id, "two");
        let outcome = loop_.run_once().unwrap();
        assert_eq!(
            outcome,
            RunOutcome::Delivered {
                accepted: 2,
                processed_through: 0,
            }
        );
        assert_eq!(loop_.harness().turns_started(), 1);
        assert_eq!(loop_.harness_mut().complete_turn(), Some(second));
    }

    #[test]
    fn followup_accepts_during_the_turn_and_promotes_after() {
        let (_dir, mut loop_, id) = setup(DeliveryMode::Followup);
        let first = recv(loop_.store(), &id, "one");
        let second = recv(loop_.store(), &id, "two");
        loop_.run_once().unwrap();
        assert_eq!(loop_.harness().turns_started(), 1);
        assert_eq!(loop_.harness().written_ids().len(), 2);
        assert_eq!(loop_.store().accepted_cursor(&id).unwrap(), second);
        assert_eq!(loop_.harness_mut().complete_turn(), Some(first));
        assert_eq!(
            loop_.harness().open_ids(),
            vec![loop_.harness().written_ids()[1].clone()]
        );
        assert_eq!(loop_.harness().turns_started(), 2);
    }
}
