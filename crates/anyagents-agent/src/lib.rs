//! Delivery loop. It hands each message past the delivery cursor to the
//! harness, in sequence order, and moves the cursor only after the harness's
//! own input or steer API has taken the message. It never looks at a turn.

use anyagents_core::{AgentId, Message, Store};
use anyagents_harness::{DeliverOutcome, Harness, HarnessError, OutboundMessage, SteerItem};

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
    /// The mailbox has nothing the harness has not taken.
    Idle,
    /// Every new line was handed over; `delivered` is how many.
    Delivered {
        delivered: u64,
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
    pub oldest_undelivered_age_ms: Option<i64>,
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

    /// Hand the next undelivered message to the harness and do not move the
    /// cursor. Dropping the loop after this is the crash between the harness
    /// taking the line and the cursor recording it.
    pub fn deliver_next_without_commit(&mut self) -> Result<Option<DeliverOutcome>, Error> {
        let Some(message) = self.store.next_undelivered(&self.agent_id)? else {
            return Ok(None);
        };
        match self.harness.deliver(&outbound(&message)) {
            Ok(outcome) => Ok(Some(outcome)),
            Err(HarnessError::Dead) => Err(Error::HarnessDown),
            Err(HarnessError::Failed(reason)) => Err(Error::HarnessFailed(reason)),
        }
    }

    /// Hand every undelivered message to the harness in sequence order, moving
    /// the cursor after each one its native input or steer API has taken. A
    /// quota answer is returned, not retried.
    pub fn run_once(&mut self) -> Result<RunOutcome, Error> {
        let mut delivered = 0u64;
        loop {
            let Some(message) = self.store.next_undelivered(&self.agent_id)? else {
                return Ok(if delivered == 0 {
                    RunOutcome::Idle
                } else {
                    RunOutcome::Delivered { delivered }
                });
            };
            let seq = message.seq;
            match self.harness.deliver(&outbound(&message)) {
                Ok(DeliverOutcome::Accepted { .. }) => {
                    self.store.note_delivered(&self.agent_id, seq)?;
                    delivered += 1;
                    self.quota_reset_at_ms = None;
                }
                // A harness that could not take the line leaves it where it is.
                // The pass ends with what it did take, never with the line
                // claimed as delivered.
                Ok(DeliverOutcome::Deferred) => {
                    return Ok(if delivered == 0 {
                        RunOutcome::Idle
                    } else {
                        RunOutcome::Delivered { delivered }
                    });
                }
                Ok(DeliverOutcome::Quota { reset_at_ms }) => {
                    self.quota_reset_at_ms = Some(reset_at_ms);
                    return Ok(RunOutcome::Quota { reset_at_ms });
                }
                Err(HarnessError::Dead) => return Err(Error::HarnessDown),
                Err(HarnessError::Failed(reason)) => return Err(Error::HarnessFailed(reason)),
            }
        }
    }

    /// Put every new mailbox message into the harness through its between-steps
    /// channel, in sequence order, and move the cursor behind each one it takes.
    pub fn steer_next(&mut self) -> Result<Option<SteerItem>, Error> {
        let Some(message) = self.store.next_undelivered(&self.agent_id)? else {
            return Ok(None);
        };
        // An older line the channel pushed before the mailbox stored it is
        // written first; anything else waits for its own turn.
        if let Some(item) = self.harness.poll_steer().map_err(inject_error)?
            && item.message.id != message.id.as_str()
        {
            let id = item.message.id.clone();
            match self.harness.inject(&item.message) {
                Ok(true) => {}
                Ok(false) => self.harness.release_steer(&id),
                Err(HarnessError::Dead) => return Err(Error::HarnessDown),
                Err(HarnessError::Failed(reason)) => return Err(Error::HarnessFailed(reason)),
            }
        }
        let item = SteerItem {
            order: 0,
            message: outbound(&message),
        };
        let taken = match self.harness.inject(&item.message) {
            Ok(taken) => taken,
            Err(HarnessError::Dead) => return Err(Error::HarnessDown),
            Err(HarnessError::Failed(reason)) => return Err(Error::HarnessFailed(reason)),
        };
        if !taken {
            self.harness.release_steer(message.id.as_str());
            return Ok(None);
        }
        self.store.note_delivered(&self.agent_id, message.seq)?;
        Ok(Some(item))
    }

    pub fn delivered_cursor(&self) -> Result<i64, Error> {
        Ok(self.store.delivered_cursor(&self.agent_id)?)
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
            oldest_undelivered_age_ms: lag.oldest_undelivered_age_ms,
        })
    }

    /// Drop the store and the lock, keeping the harness so a restarted loop can dedupe.
    pub fn into_harness(self) -> H {
        self.harness
    }

    pub fn agent_id(&self) -> AgentId {
        self.agent_id.clone()
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

fn inject_error(err: HarnessError) -> Error {
    match err {
        HarnessError::Dead => Error::HarnessDown,
        HarnessError::Failed(reason) => Error::HarnessFailed(reason),
    }
}

pub(crate) fn outbound(message: &Message) -> OutboundMessage {
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
    fn three_lines_sent_while_a_turn_is_open_are_all_delivered_once() {
        // The harness holds its turn open for the whole test. Every line is
        // still handed over as it arrives, and the one cursor moves behind it.
        let (_dir, mut loop_, id) = setup(DeliveryMode::Steer);
        let seqs: Vec<i64> = ["one", "two", "three"]
            .iter()
            .map(|text| recv(loop_.store(), &id, text))
            .collect();
        assert_eq!(
            loop_.run_once().unwrap(),
            RunOutcome::Delivered { delivered: 3 }
        );
        assert_eq!(loop_.harness().turns_started(), 1);
        assert_eq!(loop_.harness().written_ids().len(), 3);
        assert_eq!(loop_.delivered_cursor().unwrap(), *seqs.last().unwrap());
        // A second pass has nothing left to hand over.
        assert_eq!(loop_.run_once().unwrap(), RunOutcome::Idle);
        let written = loop_.harness().written_ids();
        assert_eq!(written.len(), 3, "{written:?}");
    }

    #[test]
    fn a_line_the_harness_did_not_take_leaves_the_cursor_where_it_was() {
        let (_dir, mut loop_, id) = setup(DeliveryMode::Steer);
        loop_.harness_mut().kill();
        recv(loop_.store(), &id, "one");
        assert!(matches!(loop_.run_once(), Err(Error::HarnessDown)));
        assert_eq!(loop_.delivered_cursor().unwrap(), 0);
        // The same line goes to the process that comes back: it was never taken.
        loop_.harness_mut().revive();
        assert_eq!(
            loop_.run_once().unwrap(),
            RunOutcome::Delivered { delivered: 1 }
        );
        let written = loop_.harness().written_ids();
        assert_eq!(written.len(), 1, "{written:?}");
    }

    #[test]
    fn a_restart_mid_stream_loses_nothing_and_writes_nothing_twice() {
        // A crash between the harness taking a line and the cursor recording it
        // replays that line to the restarted process. The line before it is not
        // written again, and the line after it still follows.
        let (_dir, mut loop_, id) = setup(DeliveryMode::Steer);
        recv(loop_.store(), &id, "one");
        recv(loop_.store(), &id, "two");
        let third = recv(loop_.store(), &id, "three");
        loop_.run_once().unwrap();
        assert_eq!(loop_.delivered_cursor().unwrap(), third);
        let path = loop_.store().data_dir().join("state.db");
        let harness = loop_.into_harness();
        let store = Store::open(path).unwrap();
        // Only the first line's cursor was committed; the other two were taken
        // and not recorded when the process died.
        store.rewind_for_test(&id, 1);
        let mut restarted = DeliveryLoop::start(store, id.clone(), harness).unwrap();
        assert_eq!(
            restarted.run_once().unwrap(),
            RunOutcome::Delivered { delivered: 2 }
        );
        assert_eq!(restarted.delivered_cursor().unwrap(), third);
        let written = restarted.harness().written_ids();
        assert_eq!(written.len(), 3, "{written:?}");
    }

    #[test]
    fn a_quota_answer_stops_the_pass_without_moving_the_cursor() {
        let (_dir, mut loop_, id) = setup(DeliveryMode::Steer);
        loop_.harness_mut().set_quota(Some(1_700_000_000_000));
        recv(loop_.store(), &id, "one");
        assert_eq!(
            loop_.run_once().unwrap(),
            RunOutcome::Quota {
                reset_at_ms: 1_700_000_000_000
            }
        );
        assert_eq!(loop_.delivered_cursor().unwrap(), 0);
    }

    #[test]
    fn every_line_the_harness_takes_moves_the_one_cursor() {
        let (_dir, mut loop_, id) = setup(DeliveryMode::Steer);
        recv(loop_.store(), &id, "one");
        recv(loop_.store(), &id, "two");
        assert_eq!(
            loop_.run_once().unwrap(),
            RunOutcome::Delivered { delivered: 2 }
        );
        // The cursor is the last line taken, and the inbox agrees from it on.
        let last = loop_.delivered_cursor().unwrap();
        assert_eq!(last, 2);
        let delivered = loop_.store().inbox(&id, None).unwrap();
        assert_eq!(
            delivered
                .iter()
                .filter(|m| m.ack == anyagents_core::Ack::Delivered)
                .count(),
            2
        );
    }

    #[test]
    fn lag_reports_the_oldest_line_the_harness_has_not_taken() {
        let (_dir, mut loop_, id) = setup(DeliveryMode::Steer);
        let mut incoming = Incoming::new("held");
        incoming.received_at_ms = Some(1_000);
        loop_.store().receive(&id, incoming).unwrap();
        assert_eq!(
            loop_.health(6_000).unwrap().oldest_undelivered_age_ms,
            Some(5_000)
        );
        loop_.run_once().unwrap();
        assert_eq!(loop_.health(6_000).unwrap().oldest_undelivered_age_ms, None);
    }

    #[test]
    fn an_agent_on_another_harness_is_refused_a_second_loop() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("alpha", "opencode"))
            .unwrap();
        let _first = DeliveryLoop::start(
            Store::open(dir.path().join("state.db")).unwrap(),
            agent.id.clone(),
            FakeHarness::new(DeliveryMode::Steer),
        )
        .unwrap();
        match DeliveryLoop::start(store, agent.id, FakeHarness::new(DeliveryMode::Steer)) {
            Err(err) => assert!(err.agent_busy()),
            Ok(_) => panic!("a second loop was accepted"),
        }
    }
}
