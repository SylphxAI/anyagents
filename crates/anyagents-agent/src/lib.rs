//! Delivery loop. It hands each message past the delivery cursor to the
//! harness, in sequence order, and moves the cursor only over the contiguous
//! prefix the harness's own input or steer API has taken. A line the vendor
//! holds in flight never stops the lines behind it. It never looks at a turn.

use anyagents_core::{AgentId, Message, Store};
use anyagents_harness::{DeliverOutcome, Harness, HarnessError, OutboundMessage};

pub use anyagents_harness::{Delivery, DeliveryLevel, DeliveryMode, Proof, TurnState};

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
    pub delivery: Delivery,
    pub quota_reset_at_ms: Option<i64>,
    pub sign_in_fault: Option<String>,
    pub oldest_undelivered_age_ms: Option<i64>,
}

/// A message the harness refused for good, parked past the cursor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rejection {
    pub message_id: String,
    pub seq: i64,
    pub reason: String,
}

/// One running agent: the mailbox, the per-agent lock, and its harness.
pub struct DeliveryLoop<H: Harness> {
    store: Store,
    agent_id: AgentId,
    harness: H,
    // Held so the process lock stays until this loop is dropped.
    _lock: anyagents_core::AgentLock,
    quota_reset_at_ms: Option<i64>,
    /// The recovery note being offered for an interrupted turn, kept so that
    /// every offer carries the same id until the harness proves it took it.
    recovery: Option<OutboundMessage>,
    /// Rejections parked since the host last asked.
    rejections: Vec<Rejection>,
    /// The reason the last message was parked for, until a message is taken.
    /// The same reason again means the vendor refuses everything, not one
    /// message: the breaker stops parking.
    last_rejection: Option<String>,
}

/// Commit, in sequence order, every message past the cursor `harness`
/// proved it took, stopping at the first it has not.
fn commit_proofs<H: Harness>(store: &Store, agent_id: &AgentId, harness: &H) -> Result<u64, Error> {
    let mut committed = 0;
    while let Some(message) = store.next_undelivered(agent_id)? {
        match harness.proven(message.id.as_str()) {
            Some(Proof::Taken { turn_finished }) => {
                store.note_delivered(agent_id, message.seq)?;
                if turn_finished && harness.health().sign_in_fault.is_none() {
                    store.note_turn_completed(agent_id)?;
                }
            }
            Some(Proof::Duplicate) => {
                store.note_delivered(agent_id, message.seq)?;
            }
            None => break,
        }
        committed += 1;
    }
    Ok(committed)
}

/// The reason the newest parked message was refused for, when nothing was
/// taken after it (the cursor still stands on it) and the reason can be
/// told to be no single message's own (`names_message_size`). The store keeps
/// no `message_specific` flag, so another reason of one message alone could
/// be read as the vendor's; that only parks less, never loses mail. `None`
/// when the store cannot say.
fn last_refusal(store: &Store, agent_id: &AgentId) -> Option<String> {
    let newest = store.rejected_messages(agent_id).ok()?.into_iter().last()?;
    let cursor = store.delivered_cursor(agent_id).ok()?;
    (cursor == newest.seq && !anyagents_harness::names_message_size(&newest.reason))
        .then_some(newest.reason)
}

/// More refusals than this in one pass, of reasons that are not one
/// message's own, are a vendor that refuses everything: the breaker stops
/// parking and fails the harness instead.
pub const MAX_REJECTIONS_PER_PASS: u32 = 2;

impl<H: Harness> DeliveryLoop<H> {
    /// Take the per-agent lock and refuse a second loop for the same agent.
    pub fn start(store: Store, agent_id: AgentId, harness: H) -> Result<Self, Error> {
        store.agent(&agent_id)?;
        let lock = store.try_lock_agent(&agent_id)?;
        Self::start_with_lock(store, lock, agent_id, harness)
    }

    /// A delivery loop for a harness taken over from a previous kernel,
    /// under the process lock its keeper handed back. The caller has read the
    /// agent already; nothing here can fail, so a transient store error never
    /// costs the running harness.
    pub fn adopted(
        store: Store,
        lock: anyagents_core::AgentLock,
        agent_id: AgentId,
        harness: H,
    ) -> Self {
        // A sender notice a crash cut off is sent now.
        if let Err(err) = store.send_pending_rejection_notices(&agent_id) {
            eprintln!("anyagents: notices for parked messages: {err}");
        }
        // The process is the one that refused last: its breaker goes on from
        // the last refusal, unless a message was taken since.
        let last_rejection = last_refusal(&store, &agent_id);
        Self {
            store,
            agent_id,
            harness,
            _lock: lock,
            quota_reset_at_ms: None,
            recovery: None,
            rejections: Vec::new(),
            last_rejection,
        }
    }

    /// Construct a delivery loop from an already-acquired per-agent process lock.
    pub fn start_with_lock(
        store: Store,
        lock: anyagents_core::AgentLock,
        agent_id: AgentId,
        harness: H,
    ) -> Result<Self, Error> {
        store.agent(&agent_id)?;
        // A sender notice a crash cut off is sent now.
        if let Err(err) = store.send_pending_rejection_notices(&agent_id) {
            eprintln!("anyagents: notices for parked messages: {err}");
        }
        Ok(Self {
            store,
            agent_id,
            harness,
            _lock: lock,
            quota_reset_at_ms: None,
            recovery: None,
            rejections: Vec::new(),
            last_rejection: None,
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

    /// One pass over the mailbox, in sequence order. Every line the harness
    /// can take is handed over in this pass; the cursor moves only over the
    /// contiguous prefix the harness proved consumed. Behind a line it holds
    /// in flight, the pass keeps writing, so the vendor takes the whole inbox
    /// at its next step, and the next pass commits what it consumed.
    ///
    /// A quota answer is returned, not retried. `Delivered` counts the lines
    /// committed as new work. A line the vendor
    /// already consumed in an earlier process (`Duplicate`) is committed too,
    /// but it woke nothing, so an interrupted turn still gets its recovery note.
    pub fn run_once(&mut self) -> Result<RunOutcome, Error> {
        let mut delivered = 0u64;
        if self.harness.health().turn_state == TurnState::Idle
            && !self.store.has_actionable_undelivered(&self.agent_id)?
        {
            return Ok(RunOutcome::Idle);
        }
        // The last line handed over that is not committed: once there is one,
        // later lines are written behind it and none of them is committed.
        let mut behind: Option<i64> = None;
        let mut rejected = 0u32;
        loop {
            if delivered > 0
                && behind.is_none()
                && self.harness.health().turn_state == TurnState::Idle
                && !self.store.has_actionable_undelivered(&self.agent_id)?
            {
                return Ok(RunOutcome::Delivered { delivered });
            }
            let next = match behind {
                None => self.store.next_undelivered(&self.agent_id)?,
                Some(seq) => self.store.undelivered_after(&self.agent_id, seq)?,
            };
            let Some(message) = next else {
                return Ok(if delivered == 0 {
                    RunOutcome::Idle
                } else {
                    RunOutcome::Delivered { delivered }
                });
            };
            let seq = message.seq;
            match self.harness.deliver(&outbound(&message)) {
                // Consumed, but an older line is not yet: the pass that sees
                // that one consumed commits both, in order.
                // A rejection behind an unproven line is answered again when
                // that line is committed and this one is at the head.
                Ok(
                    DeliverOutcome::Accepted { .. }
                    | DeliverOutcome::Duplicate
                    | DeliverOutcome::InFlight
                    | DeliverOutcome::Rejected { .. },
                ) if behind.is_some() => behind = Some(seq),
                // A reason of this message alone (its size, its content)
                // is parked at once and never trips the breaker: two
                // oversize messages are two parked messages.
                Ok(DeliverOutcome::Rejected {
                    reason,
                    message_specific: true,
                }) => {
                    self.reject(&message, &reason)?;
                }
                Ok(DeliverOutcome::Rejected {
                    reason,
                    message_specific: false,
                }) => {
                    // A breaker: a vendor that refuses message after message
                    // for one reason refuses everything. That is the
                    // harness's fault, not the mail's; nothing more is parked.
                    rejected += 1;
                    if self.last_rejection.as_deref() == Some(reason.as_str())
                        || rejected > MAX_REJECTIONS_PER_PASS
                    {
                        return Err(Error::HarnessFailed(format!(
                            "the harness refuses message after message ({reason}); \
                             the mail stays in the mailbox"
                        )));
                    }
                    self.reject(&message, &reason)?;
                    self.last_rejection = Some(reason);
                }
                Ok(DeliverOutcome::InFlight) => behind = Some(seq),
                Ok(DeliverOutcome::Duplicate) => {
                    // Part of the turn the earlier process ran: it extends
                    // that turn and never completes it.
                    self.store.note_delivered(&self.agent_id, seq)?;
                    self.quota_reset_at_ms = None;
                    self.last_rejection = None;
                }
                Ok(DeliverOutcome::Accepted { turn_finished }) => {
                    self.store.note_delivered(&self.agent_id, seq)?;
                    self.last_rejection = None;
                    if turn_finished && self.harness.health().sign_in_fault.is_none() {
                        self.store.note_turn_completed(&self.agent_id)?;
                    }
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

    /// Commit, in sequence order, every message past the cursor the harness
    /// already proved it took, and stop at the first it has not. Nothing is
    /// written, so this is safe on a process about to be replaced, a dead
    /// one, or one held by a quota. Returns how many were committed.
    pub fn commit_proven(&mut self) -> Result<u64, Error> {
        let committed = commit_proofs(&self.store, &self.agent_id, &self.harness)?;
        if committed > 0 {
            self.last_rejection = None;
        }
        Ok(committed)
    }

    /// Retire another process of this agent, one an earlier replacement
    /// left beside this loop's (a kernel that died in the middle of it):
    /// stop it, read it to the end, commit every proof it gave, then end it.
    /// What it took is never handed to this loop's process again.
    pub fn retire_other(&mut self, mut other: H) -> Result<u64, Error> {
        other.stop();
        let committed = commit_proofs(&self.store, &self.agent_id, &other)?;
        if committed > 0 {
            self.last_rejection = None;
        }
        Ok(committed)
    }

    /// Rejections parked since the last call, oldest first.
    pub fn take_rejections(&mut self) -> Vec<Rejection> {
        std::mem::take(&mut self.rejections)
    }

    /// Park a message the harness refused for good: past the cursor, with
    /// its reason, and a note to its sender.
    fn reject(&mut self, message: &Message, reason: &str) -> Result<(), Error> {
        self.store
            .reject_message(&self.agent_id, message.seq, reason)?;
        self.rejections.push(Rejection {
            message_id: message.id.as_str().to_string(),
            seq: message.seq,
            reason: reason.to_string(),
        });
        Ok(())
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
            delivery: harness.delivery,
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

    /// Stop the vendor process and commit every proof it gave, so nothing
    /// it took is handed to its replacement. The harness stays in place,
    /// dead, until `replace_harness`.
    pub fn retire_harness(&mut self) -> Result<u64, Error> {
        self.harness.stop();
        self.commit_proven()
    }

    /// Swap the vendor process without releasing the agent lock. A recovery
    /// note the old process never proved is rebuilt for the new one. Call
    /// `retire_harness` first, or the old process's last proofs are lost.
    pub fn replace_harness(&mut self, harness: H) {
        self.harness = harness;
        self.quota_reset_at_ms = None;
        self.recovery = None;
    }

    /// Clear a quota wait on the loop and on the harness.
    pub fn clear_quota(&mut self) {
        self.quota_reset_at_ms = None;
        self.harness.clear_quota();
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    /// Offer the `[Recovery]` note for an open turn a process crash or server
    /// restart interrupted. The note goes ahead of any new mail: it carries
    /// the interrupted turn's messages, which are older. Every offer carries
    /// the same note until the harness proves it took it; then the turn is
    /// marked recovered. `InFlight` means written and not yet proved: new
    /// mail may follow it. `Deferred` or `Quota` means not written: new mail
    /// waits behind it.
    pub fn recover_open_turn(
        &mut self,
        open: &anyagents_core::OpenTurn,
    ) -> Result<DeliverOutcome, Error> {
        let note = self
            .recovery
            .get_or_insert_with(|| recovery_outbound(open))
            .clone();
        let outcome = match self.harness.deliver(&note) {
            Ok(outcome) => outcome,
            Err(HarnessError::Dead) => return Err(Error::HarnessDown),
            Err(HarnessError::Failed(reason)) => return Err(Error::HarnessFailed(reason)),
        };
        match &outcome {
            // A note an earlier process already consumed was delivered then.
            DeliverOutcome::Duplicate | DeliverOutcome::Accepted { .. } => {
                self.store
                    .note_turn_recovered(&self.agent_id, open.opened_seq)?;
                if matches!(
                    outcome,
                    DeliverOutcome::Accepted {
                        turn_finished: true
                    }
                ) {
                    self.store.note_turn_completed(&self.agent_id)?;
                }
                self.recovery = None;
                self.quota_reset_at_ms = None;
                Ok(DeliverOutcome::Accepted {
                    turn_finished: matches!(
                        outcome,
                        DeliverOutcome::Accepted {
                            turn_finished: true
                        }
                    ),
                })
            }
            // A note the harness can never take is not offered again.
            DeliverOutcome::Rejected { .. } => {
                self.store
                    .note_turn_recovered(&self.agent_id, open.opened_seq)?;
                self.recovery = None;
                Ok(outcome)
            }
            DeliverOutcome::InFlight | DeliverOutcome::Deferred => Ok(outcome),
            DeliverOutcome::Quota { reset_at_ms } => {
                self.quota_reset_at_ms = Some(*reset_at_ms);
                Ok(outcome)
            }
        }
    }
}

/// Build the synthetic `[Recovery]` user message that wakes a resumed CLI
/// session when its previous process died mid-turn (`opened_seq > completed_seq`).
pub fn recovery_outbound(open: &anyagents_core::OpenTurn) -> OutboundMessage {
    let last = open
        .messages
        .last()
        .expect("open turn has at least one message");
    let mut lines = vec![
        "[Recovery] Your previous turn was interrupted: the process running it stopped, and this session was resumed from its durable log.".to_string(),
        String::new(),
        "The message(s) in that interrupted turn:".to_string(),
    ];
    for msg in &open.messages {
        let from = msg.sender.as_deref().unwrap_or("unknown");
        lines.push(format!("- anyagents-message:{} (from {from}):", msg.id));
        // Whole: the vendor may never have read it.
        for line in msg.text.lines() {
            lines.push(format!("  > {line}"));
        }
    }
    lines.push(String::new());
    if open.completed_outbounds.is_empty() {
        lines.push("No outbound messages were recorded before the interruption.".to_string());
    } else {
        lines.push(
            "What the durable log records about outbound messages sent since that turn began:"
                .to_string(),
        );
        for out in &open.completed_outbounds {
            lines.push(format!(
                "- send(to={}, id={}): completed (do not resend): {}",
                out.destination,
                out.id,
                clip_chars(&out.body, 300).replace('\n', " ")
            ));
        }
    }
    lines.push(String::new());
    lines.push(
        "Check the actual state before repeating anything that may have taken effect, then carry on with the task."
            .to_string(),
    );
    OutboundMessage {
        id: format!("recovery_{}_{}", last.id, open.recovery_count + 1),
        seq: open.opened_seq,
        text: lines.join("\n"),
        sender: last.sender.clone(),
        reply_to: last.reply_to.clone(),
        source_time_ms: last.source_time_ms,
        // The note is no stored message: a reply to it answers the last
        // message of the turn it covers.
        context: anyagents_harness::Context {
            reply_to_message: Some(last.id.to_string()),
            ..last.context.clone()
        },
    }
}

fn clip_chars(text: &str, max: usize) -> String {
    let mut chars = text.chars();
    let head: String = chars.by_ref().take(max).collect();
    if chars.next().is_some() {
        format!("{head}…")
    } else {
        head
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
        // The trusted header the server wrote with this message: the author, the
        // channel, the chat and topic, and the reply a bare answer names.
        context: message.context.clone(),
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

    /// A harness that answers each id with what the test says the vendor has
    /// proved so far: a line it was handed is in flight until then.
    struct Proving {
        answers: std::collections::HashMap<String, DeliverOutcome>,
        written: Vec<String>,
    }

    impl Harness for Proving {
        fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
            let answer = self.answers.entry(message.id.clone()).or_insert_with(|| {
                self.written.push(message.id.clone());
                DeliverOutcome::InFlight
            });
            Ok(answer.clone())
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
        fn health(&self) -> anyagents_harness::HarnessHealth {
            anyagents_harness::HarnessHealth {
                process_alive: true,
                turn_state: TurnState::InTurn,
                delivery: Delivery {
                    level: DeliveryLevel::NextStep,
                    idempotent: true,
                    proof: std::borrow::Cow::Borrowed("scripted proof"),
                    peer: None,
                },
                quota_reset_at_ms: None,
                sign_in_fault: None,
            }
        }
    }

    fn proving() -> (
        tempfile::TempDir,
        DeliveryLoop<Proving>,
        AgentId,
        Vec<Message>,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("alpha", "spiron"))
            .unwrap();
        let messages = ["one", "two", "three", "four"]
            .iter()
            .map(
                |text| match store.receive(&agent.id, Incoming::new(*text)).unwrap() {
                    anyagents_core::ReceiveOutcome::New(message) => message,
                    anyagents_core::ReceiveOutcome::Duplicate(_) => panic!("unexpected duplicate"),
                },
            )
            .collect();
        let harness = Proving {
            answers: std::collections::HashMap::new(),
            written: Vec::new(),
        };
        let id = agent.id.clone();
        (
            dir,
            DeliveryLoop::start(store, agent.id, harness).unwrap(),
            id,
            messages,
        )
    }

    fn prove(loop_: &mut DeliveryLoop<Proving>, message: &Message, answer: DeliverOutcome) {
        loop_
            .harness_mut()
            .answers
            .insert(message.id.as_str().to_string(), answer);
    }

    #[test]
    fn lines_behind_one_in_flight_are_written_at_once_and_committed_only_in_order() {
        let (_dir, mut loop_, _id, messages) = proving();
        let consumed = DeliverOutcome::Accepted {
            turn_finished: false,
        };
        // Nothing is proved: the whole inbox is written, nothing committed.
        assert_eq!(loop_.run_once().unwrap(), RunOutcome::Idle);
        assert_eq!(loop_.harness().written.len(), 4);
        assert_eq!(loop_.delivered_cursor().unwrap(), 0);
        // The first and third are proved; the second still is not, so only
        // the first is committed.
        prove(&mut loop_, &messages[0], consumed.clone());
        prove(&mut loop_, &messages[2], consumed.clone());
        assert_eq!(
            loop_.run_once().unwrap(),
            RunOutcome::Delivered { delivered: 1 }
        );
        assert_eq!(loop_.delivered_cursor().unwrap(), messages[0].seq);
        // The second is proved: it and the third are committed, in order.
        prove(&mut loop_, &messages[1], consumed.clone());
        assert_eq!(
            loop_.run_once().unwrap(),
            RunOutcome::Delivered { delivered: 2 }
        );
        assert_eq!(loop_.delivered_cursor().unwrap(), messages[2].seq);
        assert_eq!(loop_.harness().written.len(), 4, "nothing is written twice");
        assert_eq!(loop_.run_once().unwrap(), RunOutcome::Idle);
        assert_eq!(loop_.delivered_cursor().unwrap(), messages[2].seq);
    }

    #[test]
    fn a_line_an_earlier_process_consumed_is_committed_but_wakes_nothing() {
        let (_dir, mut loop_, id, messages) = proving();
        loop_.store().note_delivered(&id, messages[0].seq).unwrap();
        prove(&mut loop_, &messages[1], DeliverOutcome::Duplicate);
        prove(&mut loop_, &messages[2], DeliverOutcome::Duplicate);
        prove(&mut loop_, &messages[3], DeliverOutcome::Deferred);
        // Only replays: the cursor moves, but nothing new was delivered, so
        // the interrupted turn they belong to stays open for its recovery.
        assert_eq!(loop_.run_once().unwrap(), RunOutcome::Idle);
        assert_eq!(loop_.delivered_cursor().unwrap(), messages[2].seq);
        let open = loop_.store().open_turn(&id).unwrap().unwrap();
        let ids: Vec<&str> = open.messages.iter().map(|m| m.id.as_str()).collect();
        assert_eq!(
            ids,
            [
                messages[0].id.as_str(),
                messages[1].id.as_str(),
                messages[2].id.as_str()
            ]
        );
        prove(
            &mut loop_,
            &messages[3],
            DeliverOutcome::Accepted {
                turn_finished: false,
            },
        );
        assert_eq!(
            loop_.run_once().unwrap(),
            RunOutcome::Delivered { delivered: 1 }
        );
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

    #[test]
    fn a_message_refused_for_good_is_parked_and_the_mail_behind_it_flows() {
        let (_dir, mut loop_, id) = setup(DeliveryMode::Steer);
        let poison = recv(loop_.store(), &id, "never taken");
        let after = recv(loop_.store(), &id, "after it");
        let poison_id = loop_
            .store()
            .inbox(&id, None)
            .unwrap()
            .into_iter()
            .find(|message| message.seq == poison)
            .unwrap()
            .id;
        loop_
            .harness_mut()
            .refuse_forever(poison_id.as_str(), "malformed request");
        assert_eq!(
            loop_.run_once().unwrap(),
            RunOutcome::Delivered { delivered: 1 }
        );
        assert_eq!(loop_.delivered_cursor().unwrap(), after);
        let rejections = loop_.take_rejections();
        assert_eq!(rejections.len(), 1);
        assert_eq!(rejections[0].seq, poison);
        assert_eq!(rejections[0].reason, "malformed request");
        let parked = loop_.store().rejected_messages(&id).unwrap();
        assert_eq!(parked.len(), 1);
        assert_eq!(parked[0].message_id, poison_id.as_str());
        // It was never written, and is not offered again.
        assert_eq!(loop_.run_once().unwrap(), RunOutcome::Idle);
        assert!(
            !loop_
                .harness()
                .written_ids()
                .contains(&poison_id.as_str().to_string())
        );
    }

    #[test]
    fn a_vendor_refusing_message_after_message_trips_the_breaker_instead_of_parking_them() {
        let (_dir, mut loop_, id) = setup(DeliveryMode::Steer);
        let seqs: Vec<i64> = ["one", "two", "three"]
            .iter()
            .map(|text| recv(loop_.store(), &id, text))
            .collect();
        let ids: Vec<String> = loop_
            .store()
            .inbox(&id, None)
            .unwrap()
            .into_iter()
            .map(|message| message.id.as_str().to_string())
            .collect();
        for message in &ids {
            loop_
                .harness_mut()
                .refuse_forever(message, "invalid params (-32602)");
        }
        // The first is parked; the second, refused for the same reason, is a
        // vendor that refuses everything: it stays, and the harness failed.
        assert!(matches!(loop_.run_once(), Err(Error::HarnessFailed(_))));
        assert_eq!(loop_.delivered_cursor().unwrap(), seqs[0]);
        assert_eq!(loop_.store().rejected_messages(&id).unwrap().len(), 1);
        assert!(matches!(loop_.run_once(), Err(Error::HarnessFailed(_))));
        assert_eq!(loop_.store().rejected_messages(&id).unwrap().len(), 1);
        // Once a message is taken, a single refusal parks again.
        let mut fresh = FakeHarness::new(DeliveryMode::Steer);
        fresh.refuse_forever(&ids[2], "too large");
        loop_.replace_harness(fresh);
        assert_eq!(
            loop_.run_once().unwrap(),
            RunOutcome::Delivered { delivered: 1 }
        );
        assert_eq!(loop_.delivered_cursor().unwrap(), seqs[2]);
        assert_eq!(loop_.store().rejected_messages(&id).unwrap().len(), 2);
    }

    /// A kernel that takes over the process that refused last keeps its
    /// breaker: the same reason again is still a vendor that refuses
    /// everything. A message taken since the refusal resets it.
    #[test]
    fn the_breaker_survives_a_kernel_that_takes_the_process_over() {
        for taken_since in [false, true] {
            let (dir, mut loop_, id) = setup(DeliveryMode::Steer);
            recv(loop_.store(), &id, "one");
            let one = loop_.store().inbox(&id, None).unwrap()[0].id.to_string();
            loop_
                .harness_mut()
                .refuse_forever(&one, "invalid params (-32602)");
            loop_.run_once().unwrap();
            assert_eq!(loop_.store().rejected_messages(&id).unwrap().len(), 1);
            if taken_since {
                recv(loop_.store(), &id, "two");
                assert_eq!(
                    loop_.run_once().unwrap(),
                    RunOutcome::Delivered { delivered: 1 }
                );
            }
            drop(loop_);
            let store = Store::open(dir.path().join("state.db")).unwrap();
            let next = recv(&store, &id, "three");
            let three = store
                .inbox(&id, None)
                .unwrap()
                .into_iter()
                .find(|message| message.seq == next)
                .unwrap()
                .id
                .to_string();
            let mut process = FakeHarness::new(DeliveryMode::Steer);
            process.refuse_forever(&three, "invalid params (-32602)");
            let lock = store.try_lock_agent(&id).unwrap();
            let mut adopted = DeliveryLoop::adopted(store, lock, id.clone(), process);
            let outcome = adopted.run_once();
            let parked = adopted.store().rejected_messages(&id).unwrap().len();
            if taken_since {
                assert!(outcome.is_ok(), "{outcome:?}");
                assert_eq!(parked, 2);
            } else {
                assert!(
                    matches!(outcome, Err(Error::HarnessFailed(_))),
                    "the breaker was reset by the restart: {outcome:?}"
                );
                assert_eq!(parked, 1);
            }
        }
    }

    #[test]
    fn oversize_messages_are_each_parked_and_never_trip_the_breaker() {
        let (_dir, mut loop_, id) = setup(DeliveryMode::Steer);
        let seqs: Vec<i64> = ["huge one", "huge two", "huge three", "fine"]
            .iter()
            .map(|text| recv(loop_.store(), &id, text))
            .collect();
        let ids: Vec<String> = loop_
            .store()
            .inbox(&id, None)
            .unwrap()
            .into_iter()
            .map(|message| message.id.as_str().to_string())
            .collect();
        for message in &ids[..3] {
            loop_
                .harness_mut()
                .refuse_this_message(message, "-32002 inputTooLarge");
        }
        // Three refusals of the same reason in one pass, each the message's
        // own: three parked messages, and the mail behind them flows.
        assert_eq!(
            loop_.run_once().unwrap(),
            RunOutcome::Delivered { delivered: 1 }
        );
        assert_eq!(loop_.delivered_cursor().unwrap(), seqs[3]);
        assert_eq!(loop_.store().rejected_messages(&id).unwrap().len(), 3);
    }

    #[test]
    fn a_systemic_refusal_still_trips_the_breaker_after_an_oversize_one() {
        let (_dir, mut loop_, id) = setup(DeliveryMode::Steer);
        let seqs: Vec<i64> = ["huge", "one", "two"]
            .iter()
            .map(|text| recv(loop_.store(), &id, text))
            .collect();
        let ids: Vec<String> = loop_
            .store()
            .inbox(&id, None)
            .unwrap()
            .into_iter()
            .map(|message| message.id.as_str().to_string())
            .collect();
        loop_
            .harness_mut()
            .refuse_this_message(&ids[0], "-32002 inputTooLarge");
        for message in &ids[1..] {
            loop_
                .harness_mut()
                .refuse_forever(message, "-32602 invalid params");
        }
        // The oversize one is parked; the first systemic refusal is parked
        // too, the second, for the same reason, trips the breaker.
        assert!(matches!(loop_.run_once(), Err(Error::HarnessFailed(_))));
        assert_eq!(loop_.delivered_cursor().unwrap(), seqs[1]);
        assert_eq!(loop_.store().rejected_messages(&id).unwrap().len(), 2);
    }

    #[test]
    fn commit_proven_commits_the_proved_prefix_and_writes_nothing() {
        let (_dir, mut loop_, id) = setup(DeliveryMode::Steer);
        let one = recv(loop_.store(), &id, "one");
        recv(loop_.store(), &id, "two");
        // A process took `one` and proved it, then the cursor was not moved.
        loop_.deliver_next_without_commit().unwrap();
        let written = loop_.harness().written_ids().to_vec();
        assert_eq!(written.len(), 1);
        loop_.harness_mut().kill();
        assert_eq!(loop_.commit_proven().unwrap(), 1);
        assert_eq!(loop_.delivered_cursor().unwrap(), one);
        // `two` was never proved, and nothing was written for it.
        assert_eq!(loop_.commit_proven().unwrap(), 0);
        assert_eq!(loop_.harness().written_ids(), written.as_slice());
    }

    #[test]
    fn a_recovery_note_keeps_its_id_until_taken_and_carries_long_messages_whole() {
        let (_dir, mut loop_, id) = setup(DeliveryMode::Queue);
        let long = format!("{}the end", "z".repeat(3_000));
        recv(loop_.store(), &id, &long);
        assert_eq!(
            loop_.run_once().unwrap(),
            RunOutcome::Delivered { delivered: 1 }
        );
        let open = loop_.store().open_turn(&id).unwrap().expect("open turn");
        // The queue harness takes nothing while its turn runs.
        assert_eq!(
            loop_.recover_open_turn(&open).unwrap(),
            DeliverOutcome::Deferred
        );
        let note = loop_.recovery.clone().expect("the note is kept");
        assert!(note.text.contains(&long), "the note clipped the message");
        recv(loop_.store(), &id, "later mail");
        let open = loop_.store().open_turn(&id).unwrap().expect("open turn");
        assert_eq!(
            loop_.recover_open_turn(&open).unwrap(),
            DeliverOutcome::Deferred
        );
        assert_eq!(loop_.recovery.as_ref().map(|n| &n.id), Some(&note.id));
        loop_.harness_mut().complete_turn();
        assert!(matches!(
            loop_.recover_open_turn(&open).unwrap(),
            DeliverOutcome::Accepted { .. }
        ));
        assert!(loop_.recovery.is_none());
        assert!(loop_.harness().written_ids().contains(&note.id));
    }
}

// This uses real OS pipes and a real store, rather than FakeHarness's custody
// simulation. Only the incomplete write is tested across replacement: a full
// pipe write still does not prove vendor/model consumption.
#[cfg(all(test, target_os = "linux"))]
mod stream_custody_tests {
    use super::*;
    use anyagents_core::{Incoming, NewAgent};
    use anyagents_harness_agy::AgyHarness;
    use std::{
        fs,
        os::unix::fs::PermissionsExt,
        thread,
        time::{Duration, Instant},
    };

    #[test]
    fn partial_pipe_write_survives_loop_and_process_replacement_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("state.db");
        let program = dir.path().join("vendor");
        fs::write(
            &program,
            "#!/usr/bin/python3\nimport signal\nsignal.pause()\n",
        )
        .unwrap();
        fs::set_permissions(&program, fs::Permissions::from_mode(0o700)).unwrap();
        let store = Store::open(&db).unwrap();
        let agent = store.create_agent(NewAgent::new("custody", "agy")).unwrap();
        let mut incoming = Incoming::new("x".repeat(2 * 1024 * 1024));
        incoming.sender = Some("original-sender".into());
        incoming.reply_to = Some("original-reply".into());
        store.receive(&agent.id, incoming).unwrap();
        store.receive(&agent.id, Incoming::new("second")).unwrap();
        let harness = AgyHarness::start(&program, None, &[]).unwrap();
        let mut delivery = DeliveryLoop::start(store, agent.id.clone(), harness).unwrap();
        assert_eq!(delivery.run_once().unwrap(), RunOutcome::Idle);
        assert_eq!(delivery.delivered_cursor().unwrap(), 0);
        assert!(delivery.harness().written_ids().is_empty());
        drop(delivery);

        fs::write(&program, "#!/usr/bin/python3\nimport sys\nfor line in sys.stdin:\n print('{\"type\":\"result\"}', flush=True)\n").unwrap();
        let store = Store::open(&db).unwrap();
        let pending = store.next_undelivered(&agent.id).unwrap().unwrap();
        assert_eq!(pending.sender.as_deref(), Some("original-sender"));
        assert_eq!(pending.reply_to.as_deref(), Some("original-reply"));
        let ids: Vec<String> = store
            .inbox(&agent.id, None)
            .unwrap()
            .iter()
            .map(|m| m.id.as_str().to_string())
            .collect();
        let harness = AgyHarness::start(&program, None, &[]).unwrap();
        let mut delivery = DeliveryLoop::start(store, agent.id, harness).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while delivery.delivered_cursor().unwrap() < 2 {
            delivery.run_once().unwrap();
            assert!(
                Instant::now() < deadline,
                "replacement must drain pending messages"
            );
            thread::yield_now();
        }
        assert_eq!(delivery.harness().written_ids(), ids);
        assert_eq!(delivery.run_once().unwrap(), RunOutcome::Idle);
        assert_eq!(delivery.harness().written_ids(), ids);
    }
}
