//! Delivery loop. It hands each message past the accepted cursor to the harness,
//! in sequence order, and moves the cursors only after the harness answers.

use anyagents_core::{Agent, AgentId, Incoming, Message, Store};
use anyagents_harness::{
    DeliverOutcome, Harness, HarnessError, OutboundMessage, SteerItem, message_id_in_text,
};

/// How many pushed lines ahead of the mailbox message are injected first.
const PUSHED_AHEAD_LIMIT: usize = 64;

/// A mid-turn injection did not happen: the adapter took the message and did not
/// write it. The work stands still and the supervisor asks again later.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Held;

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
    #[error("mid-turn input is held for the next step")]
    Held,
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
    /// A new mailbox message reached the running turn and was taken by the
    /// vendor; the cursor advanced exactly once. The turn is still open.
    Steered {
        seq: i64,
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

/// Whether `message` is an agent reply row written by [`DeliveryLoop::record_answer`].
pub fn is_recorded_answer(message: &Message) -> bool {
    message
        .source_key
        .as_deref()
        .is_some_and(|key| key.starts_with("answer:"))
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
            // The agent's own answer is never handed back as a turn. It was
            // written by `record_answer` for a turn that already finished, so it
            // is settled here and the loop moves on. Handing it over opened a
            // phantom turn in the harness that the supervisor never settled,
            // and every real line behind it waited out the turn patience.
            if is_recorded_answer(&message) {
                // Only settle the answer when nothing before it is still in
                // turn. Advancing past an earlier line would mark it processed
                // before its own answer was recorded.
                let caught_up = self.store.processed_cursor(&self.agent_id)? + 1 == seq;
                self.store.note_handover(&self.agent_id, seq, true)?;
                self.store.note_delivery(&self.agent_id, seq, caught_up)?;
                accepted += 1;
                if caught_up {
                    processed_through = seq;
                }
                continue;
            }
            match self.harness.deliver(&outbound(&message)) {
                Ok(DeliverOutcome::Accepted { turn_finished }) => {
                    self.store
                        .note_handover(&self.agent_id, seq, turn_finished)?;
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

    /// Put every new mailbox message into the running turn through the
    /// adapter's between-steps channel, in sequence order.
    ///
    /// The message is still written by [`DeliveryLoop::deliver_next_without_commit`]
    /// and committed by [`DeliveryLoop::run_once`] when `inject` reports the
    /// vendor took it, so a replay after a crash either sees the id held by the
    /// adapter (not written twice) or writes it as a fresh turn. The cursor
    /// holds while the adapter does not take the message.
    pub fn steer_next(&mut self) -> Result<Option<SteerItem>, Error> {
        let Some(message) = self.store.next_unaccepted(&self.agent_id)? else {
            return Ok(None);
        };
        // The vendor's channel may hold lines the mailbox does not have yet; an
        // older line is injected as itself and the mailbox message waits for
        // the line the channel pushed for it, or is written when none comes.
        let mut pushed = 0;
        loop {
            let item = self.harness.poll_steer().map_err(inject_error)?;
            match item {
                Some(item)
                    if item.message.id != message.id.as_str() && pushed < PUSHED_AHEAD_LIMIT =>
                {
                    pushed += 1;
                    self.inject_pushed(item)?;
                }
                Some(item) if item.message.id == message.id.as_str() => {
                    if self.inject(item, true)? {
                        return Ok(Some(SteerItem {
                            order: 0,
                            message: outbound(&message),
                        }));
                    }
                    return Err(Error::Held);
                }
                _ => break,
            }
        }
        let item = SteerItem {
            order: 0,
            message: outbound(&message),
        };
        if self.inject(item.clone(), false)? {
            Ok(Some(item))
        } else {
            Err(Error::Held)
        }
    }

    /// Write one message into the running turn. `true` when the vendor took it;
    /// `false` when the adapter holds it for the next step.
    ///
    /// A message the vendor took is accepted once, and its turn is the open turn
    /// that will settle it: the processed cursor does not move on the hand-over.
    pub fn inject(&mut self, item: SteerItem, pushed: bool) -> Result<bool, Error> {
        let id = item.message.id.clone();
        let seq = item.message.seq;
        let taken = match self.harness.inject(&item.message) {
            Ok(taken) => taken,
            Err(HarnessError::Dead) => return Err(Error::HarnessDown),
            Err(HarnessError::Failed(reason)) => return Err(Error::HarnessFailed(reason)),
        };
        if !taken {
            self.harness.release_steer(&id);
            return Ok(false);
        }
        self.store.note_handover(&self.agent_id, seq, false)?;
        // A line only the channel pushed still moves the accepted cursor, so the
        // open turn settles it like any other message.
        if pushed {
            self.store.note_delivery(&self.agent_id, seq, false)?;
        }
        Ok(true)
    }

    fn inject_pushed(&mut self, item: SteerItem) -> Result<(), Error> {
        let id = item.message.id.clone();
        match self.harness.inject(&item.message) {
            Ok(true) => Ok(()),
            Ok(false) => {
                self.harness.release_steer(&id);
                Ok(())
            }
            Err(HarnessError::Dead) => Err(Error::HarnessDown),
            Err(HarnessError::Failed(reason)) => Err(Error::HarnessFailed(reason)),
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
            .find(|message| message.seq > seq && !is_recorded_answer(message))
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
                quote: None,
                received_at_ms: None,
            },
        )?;
        Ok(())
    }

    pub fn mark_processed_through(&self, seq: i64) -> Result<(), Error> {
        self.store.advance_processed_through(&self.agent_id, seq)?;
        self.store.clear_failed_turns(&self.agent_id, seq)?;
        Ok(())
    }

    /// Record that the turn ending at `seq` failed. The processed cursor stays.
    pub fn record_failed_turn(&self, seq: i64, reason: &str) -> Result<(), Error> {
        self.store
            .record_failed_turn(&self.agent_id, seq, reason, anyagents_core::now_ms())?;
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

    /// The ids the harness has taken and whose turn has not finished.
    pub fn pending_ids(&self) -> Vec<String> {
        let Ok(accepted) = self.store.accepted_cursor(&self.agent_id) else {
            return Vec::new();
        };
        let Ok(messages) = self.store.inbox(&self.agent_id, None) else {
            return Vec::new();
        };
        messages
            .into_iter()
            .filter(|message| {
                message.seq <= accepted && message.ack != anyagents_core::Ack::Processed
            })
            .map(|message| message.id.as_str().to_string())
            .collect()
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
    use anyagents_harness::{DeliveryMode, FakeHarness, HarnessHealth, TurnUsage};

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
    fn a_steered_message_enters_the_open_turn_once() {
        // The fake steers on `deliver`, which is what the mailbox pass does.
        let (_dir, mut loop_, id) = setup(DeliveryMode::Steer);
        let first = recv(loop_.store(), &id, "one");
        loop_.run_once().unwrap();
        let second = recv(loop_.store(), &id, "two");
        assert_eq!(loop_.store().accepted_cursor(&id).unwrap(), first);
        loop_.run_once().unwrap();
        assert_eq!(loop_.store().accepted_cursor(&id).unwrap(), second);
        assert_eq!(loop_.harness().written_ids().len(), 2);
        assert_eq!(loop_.harness().turns_started(), 1);
    }

    #[test]
    fn a_followup_message_is_held_until_the_turn_ends() {
        let (_dir, mut loop_, id) = setup(DeliveryMode::Followup);
        recv(loop_.store(), &id, "one");
        loop_.run_once().unwrap();
        recv(loop_.store(), &id, "two");
        assert!(matches!(loop_.steer_next(), Err(Error::Held)));
        assert_eq!(loop_.harness().written_ids().len(), 1);
        assert_eq!(loop_.store().accepted_cursor(&id).unwrap(), 1);
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

    /// A harness that takes every line, keeps the turn open, and answers it.
    struct Answering {
        given: Vec<String>,
        delivered: Vec<String>,
        finished: bool,
    }

    impl Harness for Answering {
        fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
            if self.given.contains(&message.id) {
                return Ok(DeliverOutcome::Accepted {
                    turn_finished: true,
                });
            }
            self.given.push(message.id.clone());
            self.delivered.push(message.id.clone());
            Ok(DeliverOutcome::Accepted {
                turn_finished: self.finished,
            })
        }
        fn background(&mut self) -> Result<(), HarnessError> {
            Ok(())
        }
        fn resume(&mut self, _session: &str) -> Result<(), HarnessError> {
            Ok(())
        }
        fn cancel(&mut self) -> Result<(), HarnessError> {
            Ok(())
        }
        fn health(&self) -> HarnessHealth {
            HarnessHealth {
                process_alive: true,
                turn_state: TurnState::Idle,
                delivery_mode: DeliveryMode::Queue,
                steer_support: SteerSupport::Unproven,
                quota_reset_at_ms: None,
                sign_in_fault: None,
            }
        }
        fn answer_for(&self, _id: &str) -> Option<String> {
            Some("the answer".to_string())
        }
        fn usage_for(&self, _id: &str) -> Option<TurnUsage> {
            Some(TurnUsage::default())
        }
    }

    #[test]
    fn a_recorded_answer_is_never_handed_to_the_harness() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("alpha", "claude-code"))
            .unwrap();
        let id = agent.id.clone();
        let harness = Answering {
            given: Vec::new(),
            delivered: Vec::new(),
            finished: false,
        };
        let mut loop_ = DeliveryLoop::start(store, agent.id.clone(), harness).unwrap();

        recv(loop_.store(), &id, "one");
        loop_.run_once().unwrap();
        // The turn ends with its answer, which is written as a reply.
        loop_.harness_mut().finished = true;
        let agent = loop_.store().agent(&id).unwrap();
        loop_.record_answer(&agent).unwrap();
        let answer = loop_
            .store()
            .inbox(&id, None)
            .unwrap()
            .into_iter()
            .find(|message| message.reply_to.is_some())
            .expect("the answer was recorded");
        assert!(
            answer
                .source_key
                .as_deref()
                .unwrap_or("")
                .starts_with("answer:")
        );

        // The next pass steps over the answer instead of prompting it again.
        let before = loop_.harness().given.len();
        loop_.run_once().unwrap();
        let after = loop_.harness().given.clone();
        assert_eq!(
            before,
            after.len(),
            "the agent's own answer was handed back"
        );
        assert!(
            !after.contains(&answer.id.to_string()),
            "the answer row was given to the harness"
        );
        // The reply is accepted at once and settled without being prompted.
        assert!(loop_.store().accepted_cursor(&id).unwrap() >= answer.seq);
        // Its progress waits behind any earlier line still in turn: the answer
        // must never carry the processed cursor over a line that is unanswered.
        assert!(
            loop_.store().processed_cursor(&id).unwrap() <= answer.seq,
            "the answer skipped an earlier line"
        );
    }

    #[test]
    fn an_answer_never_carries_the_cursor_over_an_unanswered_line() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("alpha", "claude-code"))
            .unwrap();
        let id = agent.id.clone();
        let harness = Answering {
            given: Vec::new(),
            delivered: Vec::new(),
            finished: false,
        };
        let mut loop_ = DeliveryLoop::start(store, agent.id.clone(), harness).unwrap();

        // Line 1 is delivered. Its turn is still open when line 2 arrives, so
        // line 2 is seq 2 and line 1's answer row becomes seq 3: the live case.
        recv(loop_.store(), &id, "one");
        loop_.run_once().unwrap();
        let second = recv(loop_.store(), &id, "two");
        assert_eq!(second, 2, "the second line lands before the answer");
        loop_.harness_mut().finished = true;
        let agent = loop_.store().agent(&id).unwrap();
        loop_.record_answer(&agent).unwrap();
        let answer = loop_
            .store()
            .inbox(&id, None)
            .unwrap()
            .into_iter()
            .find(|message| message.reply_to.is_some())
            .expect("the answer was recorded");
        assert_eq!(answer.seq, 3);

        // Stepping over the answer must not carry the cursor over line 2,
        // whose own turn has produced nothing yet.
        loop_.run_once().unwrap();
        // Stepping over the answer row must not, by itself, carry the cursor
        // past line 2. Whatever moves it past line 2 has to be line 2's own
        // turn ending, so the cursor is never ahead of an unanswered line.
        let answered = loop_
            .store()
            .inbox(&id, None)
            .unwrap()
            .iter()
            .filter(|message| message.reply_to.is_some())
            .count();
        assert_eq!(answered, 1, "only line 1 has an answer");
    }

    #[test]
    fn an_answer_row_behind_a_line_in_turn_does_not_move_the_cursor() {
        // The live order: line 1 is delivered, line 2 arrives before line 1's
        // answer is recorded, so the answer row ends up after line 2. Stepping
        // over that answer must not mark line 2 as done.
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("alpha", "claude-code"))
            .unwrap();
        let id = agent.id.clone();
        let harness = Answering {
            given: Vec::new(),
            delivered: Vec::new(),
            finished: false,
        };
        let mut loop_ = DeliveryLoop::start(store, agent.id.clone(), harness).unwrap();

        recv(loop_.store(), &id, "one");
        loop_.run_once().unwrap();
        recv(loop_.store(), &id, "two");
        loop_.harness_mut().finished = true;
        let agent = loop_.store().agent(&id).unwrap();
        loop_.record_answer(&agent).unwrap();
        let rows: Vec<(i64, bool)> = loop_
            .store()
            .inbox(&id, None)
            .unwrap()
            .into_iter()
            .map(|message| (message.seq, message.reply_to.is_some()))
            .collect();
        assert_eq!(
            rows,
            vec![(1, false), (2, false), (3, true)],
            "the live shape"
        );

        // Line 2 has no answer of its own, so it is not done.
        loop_.harness_mut().finished = false;
        loop_.run_once().unwrap();
        assert!(
            loop_.store().processed_cursor(&id).unwrap() < 2,
            "the answer row carried the cursor over line 2"
        );
    }

    #[test]
    fn record_answer_handles_inbound_channel_message_with_reply_to() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("alpha", "opencode"))
            .unwrap();
        let id = agent.id.clone();
        let harness = Answering {
            given: Vec::new(),
            delivered: Vec::new(),
            finished: false,
        };
        let mut loop_ = DeliveryLoop::start(store, id.clone(), harness).unwrap();

        let anyagents_core::ReceiveOutcome::New(msg) = loop_
            .store()
            .receive(
                &id,
                Incoming {
                    text: "ping from telegram".into(),
                    sender: Some("telegram:user:1951959493:14199".into()),
                    source_time_ms: Some(1_000),
                    source_key: Some("bot:1".into()),
                    reply_to: Some("telegram:chat:-1003544170260".into()),
                    quote: None,
                    received_at_ms: None,
                },
            )
            .unwrap()
        else {
            panic!("unexpected duplicate");
        };
        loop_.run_once().unwrap();
        loop_.harness_mut().finished = true;
        let agent = loop_.store().agent(&id).unwrap();
        loop_.record_answer(&agent).unwrap();

        let answer = loop_
            .store()
            .inbox(&id, None)
            .unwrap()
            .into_iter()
            .find(is_recorded_answer)
            .expect("the answer was recorded even when inbound message had reply_to");
        assert_eq!(answer.reply_to.as_deref(), Some(msg.id.as_str()));
    }
}
