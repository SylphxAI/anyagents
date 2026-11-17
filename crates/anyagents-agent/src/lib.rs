//! Delivery loop. External-user mail can be offered ahead of trusted child or
//! schedule mail, within bounded causal spans. The cursor and accounting still
//! move only over the contiguous prefix proved taken by the harness. In-flight
//! input never prevents later offers; hard admission gates stop the pass.

mod priority;
#[cfg(test)]
mod priority_tests;

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

/// Custody of the latest mailbox delivery admitted by this loop.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TurnCustody {
    /// Admitted `delivered_seq` whose terminal outcome is still open.
    Open(i64),
    /// Synchronous `turn_finished` already accepted `note_turn_completed_for(seq)`.
    Completed(i64),
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
    custody: Option<TurnCustody>,
}

/// Commit, in sequence order, every message past the cursor `harness`
/// proved it took, stopping at the first it has not.
fn commit_proofs<H: Harness>(
    store: &Store,
    agent_id: &AgentId,
    harness: &H,
    mut custody: Option<&mut Option<TurnCustody>>,
) -> Result<u64, Error> {
    let mut committed = 0;
    while let Some(message) = store.next_undelivered(agent_id)? {
        match harness.proven(message.id.as_str()) {
            Some(Proof::Taken { turn_finished }) => {
                store.note_delivered(agent_id, message.seq)?;
                if turn_finished
                    && harness.health().sign_in_fault.is_none()
                    && priority::may_complete(store, agent_id, harness, &message)?
                {
                    let accepted = store.note_turn_completed_for(agent_id, message.seq)?;
                    if let Some(slot) = custody.as_deref_mut() {
                        *slot = accepted.then_some(TurnCustody::Completed(message.seq));
                    }
                } else if let Some(slot) = custody.as_deref_mut() {
                    *slot = Some(TurnCustody::Open(message.seq));
                }
            }
            Some(Proof::Duplicate) => {
                store.note_delivered(agent_id, message.seq)?;
                if let Some(slot) = custody.as_deref_mut() {
                    *slot = Some(TurnCustody::Open(message.seq));
                }
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
            custody: None,
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
            custody: None,
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

    /// One pass over bounded causal spans, offering external-user mail before
    /// background mail. The cursor moves only over the
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
        if self.harness.pending_input_owner().is_none()
            && self.harness.health().turn_state == TurnState::Idle
            && !self.store.has_actionable_undelivered(&self.agent_id)?
        {
            return Ok(RunOutcome::Idle);
        }
        // The last line handed over that is not committed: once there is one,
        // later lines are written behind it and none of them is committed.
        let mut behind: Option<i64> = None;
        let mut rejected = 0u32;
        let mut scanned_through = 0;
        let mut offered = std::collections::HashMap::new();
        // A write already started cannot be preempted with another line.
        // Resume only its exact owner, never a guessed head/background input.
        // Its result is cached, not committed out of sequence. Recovery notes
        // have synthetic IDs and stay on their own accounting path.
        if let Some(owner) = self.harness.pending_input_owner() {
            let (seq, outcome) = if self.recovery.as_ref().is_some_and(|n| n.id == owner) {
                let open = self.store.open_turn(&self.agent_id)?.ok_or_else(|| {
                    Error::HarnessFailed("pending recovery input has no open turn".into())
                })?;
                (None, self.recover_open_turn(&open)?)
            } else {
                let cursor = self.store.delivered_cursor(&self.agent_id)?;
                let message =
                    self.store
                        .message(&self.agent_id, &owner)?
                        .filter(|m| m.seq > cursor)
                        .ok_or_else(|| {
                            Error::HarnessFailed(
                        "pending input is not owned by undelivered mail or the recovery note".into()
                    )
                        })?;
                let outcome =
                    self.harness
                        .deliver(&outbound(&message))
                        .map_err(|err| match err {
                            HarnessError::Dead => Error::HarnessDown,
                            HarnessError::Failed(reason) => Error::HarnessFailed(reason),
                        })?;
                (Some(message.seq), outcome)
            };
            match outcome {
                DeliverOutcome::Deferred => return Ok(RunOutcome::Idle),
                DeliverOutcome::Quota { reset_at_ms } => {
                    self.quota_reset_at_ms = Some(reset_at_ms);
                    return Ok(RunOutcome::Quota { reset_at_ms });
                }
                outcome => {
                    if let Some(seq) = seq {
                        offered.insert(seq, outcome);
                    }
                }
            }
        }
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
            if seq > scanned_through {
                let (through, external) = priority::ahead(&self.store, &self.agent_id, &message)?;
                scanned_through = through;
                for urgent in external {
                    if offered.contains_key(&urgent.seq) {
                        continue;
                    }
                    match self.harness.deliver(&outbound(&urgent)) {
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
                        Err(HarnessError::Failed(reason)) => {
                            return Err(Error::HarnessFailed(reason));
                        }
                        Ok(outcome) => {
                            offered.insert(urgent.seq, outcome);
                        }
                    }
                }
            }
            // Cache offers only for this pass. Vendor proofs, not the cache,
            // survive interruption; every store mutation still occurs at head.
            let outcome = match offered.remove(&seq) {
                Some(cached) => Ok(match self.harness.proven(message.id.as_str()) {
                    Some(Proof::Taken { turn_finished }) => {
                        DeliverOutcome::Accepted { turn_finished }
                    }
                    Some(Proof::Duplicate) => DeliverOutcome::Duplicate,
                    None => cached,
                }),
                None => self.harness.deliver(&outbound(&message)),
            };
            match outcome {
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
                    self.custody = Some(TurnCustody::Open(seq));
                    self.quota_reset_at_ms = None;
                    self.last_rejection = None;
                }
                Ok(DeliverOutcome::Accepted { turn_finished }) => {
                    self.store.note_delivered(&self.agent_id, seq)?;
                    self.last_rejection = None;
                    if turn_finished
                        && self.harness.health().sign_in_fault.is_none()
                        && priority::may_complete(
                            &self.store,
                            &self.agent_id,
                            &self.harness,
                            &message,
                        )?
                    {
                        let accepted = self.store.note_turn_completed_for(&self.agent_id, seq)?;
                        self.custody = accepted.then_some(TurnCustody::Completed(seq));
                    } else {
                        self.custody = Some(TurnCustody::Open(seq));
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
        let committed = commit_proofs(
            &self.store,
            &self.agent_id,
            &self.harness,
            Some(&mut self.custody),
        )?;
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
        let committed = commit_proofs(&self.store, &self.agent_id, &other, None)?;
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
        if matches!(self.custody, Some(TurnCustody::Open(_))) {
            self.custody = Some(TurnCustody::Open(message.seq));
        }
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
    /// Checked retirement keeps both harness and process lock on failure.
    /// Final proofs are committed only after the adapter acknowledges drain.
    pub fn retire_harness_verified(&mut self) -> Result<u64, Error> {
        self.harness.stop_verified().map_err(|err| match err {
            HarnessError::Dead => Error::HarnessDown,
            HarnessError::Failed(reason) => Error::HarnessFailed(reason),
        })?;
        self.commit_proven()
    }

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
        self.custody = None;
    }

    /// Clear a quota wait on the loop and on the harness.
    pub fn clear_quota(&mut self) {
        self.quota_reset_at_ms = None;
        self.harness.clear_quota();
    }

    /// Admitted `delivered_seq` for an in-progress turn held by this loop.
    pub fn admitted_seq(&self) -> Option<i64> {
        match self.custody {
            Some(TurnCustody::Open(seq)) => Some(seq),
            _ => None,
        }
    }

    /// Restore open turn custody carried across a detached process takeover.
    pub fn restore_admitted_seq(&mut self, delivered_seq: Option<i64>) {
        self.custody = delivered_seq.filter(|&seq| seq > 0).map(TurnCustody::Open);
    }

    /// Complete callback-owned delivery custody. Returns `Ok(false)` without
    /// mutating recovery or error state when custody is absent, stale, or
    /// already consumed.
    pub fn complete_turn(&mut self) -> Result<bool, Error> {
        match self.custody {
            Some(TurnCustody::Open(seq)) => {
                let accepted = self.store.note_turn_completed_for(&self.agent_id, seq)?;
                self.custody = None;
                Ok(accepted)
            }
            Some(TurnCustody::Completed(seq)) => {
                let current = self.store.delivered_cursor(&self.agent_id)? == seq;
                self.custody = None;
                Ok(current)
            }
            None => Ok(false),
        }
    }

    /// Record an errored turn only when this loop holds exact open custody for
    /// `delivered_seq`. Returns `Ok(false)` when stale, duplicate, or unowned.
    pub fn error_turn(&mut self, error: &str, strike: bool) -> Result<bool, Error> {
        let Some(TurnCustody::Open(seq)) = self.custody else {
            self.custody = None;
            return Ok(false);
        };
        let accepted = self
            .store
            .note_turn_errored_for(&self.agent_id, seq, error, strike)?;
        self.custody = None;
        Ok(accepted)
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
                let seq = self
                    .store
                    .delivered_cursor(&self.agent_id)?
                    .max(open.opened_seq);
                self.store
                    .note_turn_recovered(&self.agent_id, open.opened_seq)?;
                // A recovered turn that ended in an error is not completed:
                // the host closes it with that outcome and counts it.
                let turn_finished = matches!(
                    outcome,
                    DeliverOutcome::Accepted {
                        turn_finished: true
                    }
                ) && self.harness.health().sign_in_fault.is_none()
                    && priority::recovery_covers(
                        &self.store,
                        &self.agent_id,
                        &self.harness,
                        note.seq,
                    )?;
                if turn_finished {
                    let accepted = self.store.note_turn_completed_for(&self.agent_id, seq)?;
                    self.custody = accepted.then_some(TurnCustody::Completed(seq));
                } else {
                    self.custody = Some(TurnCustody::Open(seq));
                }
                self.recovery = None;
                self.quota_reset_at_ms = None;
                Ok(DeliverOutcome::Accepted { turn_finished })
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

/// The `[Recovery]` note that follows a turn the harness ended with an
/// error. The host closes that turn with the error as its outcome once this
/// note, stored in the agent's own mailbox, is there: it starts the next turn
/// with the work the failed one held, together with any mail that waits.
/// `strike_limit` is how many errors of the session's own in a row park the
/// agent, when this error is one; `None` for a systemic error, which never
/// parks it.
pub fn errored_turn_note(
    turn: &anyagents_core::ErroredTurn,
    error: &str,
    strike_limit: Option<i64>,
) -> String {
    let mut lines = vec![
        format!(
            "[Recovery] Your previous turn ended with an error, and AnyAgents closed it: {}",
            clip_chars(error.trim(), 1_000)
        ),
        String::new(),
    ];
    match turn.open.as_ref().filter(|open| !open.messages.is_empty()) {
        Some(open) => {
            lines.push("The message(s) in that turn:".to_string());
            for msg in &open.messages {
                let from = msg.sender.as_deref().unwrap_or("unknown");
                lines.push(format!("- anyagents-message:{} (from {from}):", msg.id));
                // Clipped: the whole message stays in the mailbox under its id.
                for line in clip_chars(&msg.text, 2_000).lines() {
                    lines.push(format!("  > {line}"));
                }
            }
            if !open.completed_outbounds.is_empty() {
                lines.push(String::new());
                lines.push("Outbound messages already sent since that turn began:".to_string());
                for out in &open.completed_outbounds {
                    lines.push(format!(
                        "- send(to={}, id={}): completed (do not resend): {}",
                        out.destination,
                        out.id,
                        clip_chars(&out.body, 300).replace('\n', " ")
                    ));
                }
            }
        }
        None => lines.push(
            "That turn carried no new mailbox message (it was itself a recovery turn).".to_string(),
        ),
    }
    lines.push(String::new());
    lines.push(match strike_limit {
        Some(limit) => format!(
            "This is error {} of {limit} in a row about this session's own content. At {limit}, the agent is paused, its mail is held, and the agent above it (or the operator) is told the error.",
            turn.streak
        ),
        None => "The error came from the provider, the model or the host, not from this session: it was retried after a backoff, and it never pauses the agent.".to_string(),
    });
    lines.push(
        "Check the actual state before repeating anything that may have taken effect, then carry on with the task in smaller steps."
            .to_string(),
    );
    lines.join("\n")
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

    #[test]
    fn checked_turn_custody_rejects_unowned_stale_and_duplicate_callbacks() {
        let (_dir, mut loop_, id) = setup(DeliveryMode::Steer);
        assert!(!loop_.complete_turn().unwrap());
        assert!(!loop_.error_turn("unowned", true).unwrap());
        assert!(loop_.store().turn_errors(&id).unwrap().is_none());
        assert_eq!(loop_.store().agent_turn_snapshot(&id).unwrap().turn_seq, 0);

        let first = recv(loop_.store(), &id, "first");
        assert_eq!(
            loop_.run_once().unwrap(),
            RunOutcome::Delivered { delivered: 1 }
        );
        assert_eq!(loop_.admitted_seq(), Some(first));

        // Simulate a newer delivery advancing the mailbox cursor past this loop's
        // admitted sequence: stale completion and error callbacks must return false
        // without closing the newer turn or recording strikes.
        let second = recv(loop_.store(), &id, "second");
        loop_.store().note_delivered(&id, second).unwrap();
        assert!(!loop_.complete_turn().unwrap());
        assert!(loop_.store().has_open_turn(&id).unwrap());
        assert_eq!(loop_.store().agent_turn_snapshot(&id).unwrap().turn_seq, 0);

        loop_.restore_admitted_seq(Some(first));
        assert!(!loop_.error_turn("stale strike", true).unwrap());
        assert!(loop_.store().turn_errors(&id).unwrap().is_none());
        assert!(loop_.store().has_open_turn(&id).unwrap());

        // Exact current custody succeeds once and suppresses duplicates.
        loop_.restore_admitted_seq(Some(second));
        assert!(loop_.complete_turn().unwrap());
        assert!(!loop_.store().has_open_turn(&id).unwrap());
        let done = loop_.store().agent_turn_snapshot(&id).unwrap();
        assert_eq!(done.turn_seq, 1);
        assert_eq!(done.outcome.as_deref(), Some("completed"));
        assert!(!loop_.complete_turn().unwrap());
        assert!(!loop_.error_turn("late error", true).unwrap());
        assert_eq!(loop_.store().agent_turn_snapshot(&id).unwrap(), done);
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

        fs::write(&program, "#!/usr/bin/python3\nimport sys\nfor line in sys.stdin:\n print('{\"type\":\"result\",\"response\":\"ok\"}', flush=True)\n").unwrap();
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

    #[test]
    fn partial_background_pipe_write_resumes_in_place_then_prioritizes_external_mail() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("state.db");
        let program = dir.path().join("vendor");
        let gate = dir.path().join("gate.fifo");
        let ready = dir.path().join("ready");
        let order = dir.path().join("order.log");
        fs::write(
            &program,
            r#"#!/usr/bin/python3
import json, os, re, sys
gate = os.environ["TEST_GATE"]
ready = os.environ["TEST_READY"]
order = os.environ["TEST_ORDER"]
os.mkfifo(gate)
with open(ready, "w") as f:
    f.write("1")
with open(gate, "r") as f:
    f.read()
for raw in sys.stdin:
    obj = json.loads(raw)
    match = re.search(r"anyagents-message:([A-Za-z0-9_:-]+)", json.dumps(obj))
    assert match is not None
    with open(order, "a") as out:
        print(match.group(1), file=out, flush=True)
    print('{"type":"result","response":"ok"}', flush=True)
"#,
        )
        .unwrap();
        fs::set_permissions(&program, fs::Permissions::from_mode(0o700)).unwrap();

        let store = Store::open(&db).unwrap();
        let agent = store.create_agent(NewAgent::new("custody", "agy")).unwrap();
        let mut child_spec = NewAgent::new("child", "agy");
        child_spec.parent_id = Some(agent.id.clone());
        let child = store.create_agent(child_spec).unwrap();
        let child_sender = format!("agent:{}", child.id);

        let recv = |store: &Store, sender: &str, text: String| match store
            .receive(&agent.id, {
                let mut inc = Incoming::new(text);
                inc.sender = Some(sender.to_string());
                inc
            })
            .unwrap()
        {
            anyagents_core::ReceiveOutcome::New(m) => m,
            anyagents_core::ReceiveOutcome::Duplicate(_) => panic!("unexpected duplicate"),
        };

        let b1 = recv(&store, &child_sender, "x".repeat(2 * 1024 * 1024));
        let env = [
            ("TEST_GATE", gate.to_str().unwrap()),
            ("TEST_READY", ready.to_str().unwrap()),
            ("TEST_ORDER", order.to_str().unwrap()),
        ];
        let harness = AgyHarness::start(&program, None, &env).unwrap();
        let mut delivery = DeliveryLoop::start(store, agent.id.clone(), harness).unwrap();

        // First pass lazily spawns the child, fills the OS pipe partway with b1, and retains it.
        assert_eq!(delivery.run_once().unwrap(), RunOutcome::Idle);
        let wait_ready = Instant::now() + Duration::from_secs(5);
        while !ready.exists() {
            assert!(Instant::now() < wait_ready, "vendor script did not ready");
            thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(
            delivery.harness().pending_input_owner().as_deref(),
            Some(b1.id.as_str())
        );
        assert!(!delivery.harness().input_ready());
        assert_eq!(delivery.delivered_cursor().unwrap(), 0);
        assert!(delivery.harness().written_ids().is_empty());

        // Queue another background message plus two urgent external messages.
        let b2 = recv(delivery.store(), &child_sender, "background-2".into());
        let u1 = recv(delivery.store(), "telegram:user:10", "urgent-1".into());
        let u2 = recv(delivery.store(), "telegram:user:11", "urgent-2".into());

        // While the pipe remains blocked, urgent mail cannot preempt or clear b1.
        assert_eq!(delivery.run_once().unwrap(), RunOutcome::Idle);
        assert_eq!(
            delivery.harness().pending_input_owner().as_deref(),
            Some(b1.id.as_str())
        );
        assert_eq!(delivery.delivered_cursor().unwrap(), 0);

        // Release the child reader in the same process (no replacement).
        fs::write(&gate, b"go").unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while delivery.delivered_cursor().unwrap() < 4 {
            delivery.run_once().unwrap();
            assert!(
                Instant::now() < deadline,
                "same-process pending write + priority mail must drain without deadlock"
            );
            thread::sleep(Duration::from_millis(1));
        }

        let expected = vec![
            b1.id.as_str().to_string(),
            u1.id.as_str().to_string(),
            u2.id.as_str().to_string(),
            b2.id.as_str().to_string(),
        ];
        assert_eq!(delivery.harness().pending_input_owner(), None);
        assert_eq!(delivery.harness().written_ids(), expected);
        let wire_order: Vec<String> = fs::read_to_string(&order)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect();
        assert_eq!(wire_order, expected);
        assert!(delivery.store().open_turn(&agent.id).unwrap().is_none());
    }
}
