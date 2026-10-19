//! Scripted harness. It records every id it is given, writes each id once, and
//! can be killed between steps.

use std::collections::HashSet;

use crate::{
    DeliverOutcome, DeliveryMode, Harness, HarnessError, HarnessHealth, OutboundMessage,
    SteerSupport, TurnReport, TurnState, TurnUsage,
};

#[derive(Clone, Debug)]
struct Written {
    id: String,
    seq: i64,
}

/// In-memory harness with one delivery mode and no vendor process.
#[derive(Debug)]
pub struct FakeHarness {
    mode: DeliveryMode,
    alive: bool,
    given: Vec<String>,
    written: Vec<String>,
    seen: HashSet<String>,
    open: Vec<Written>,
    followups: Vec<Written>,
    finished: HashSet<String>,
    failed: Vec<(String, String)>,
    turns: std::collections::BTreeMap<String, u64>,
    turns_started: usize,
    quota_reset_at_ms: Option<i64>,
    auto_finish: bool,
    resumed: Vec<String>,
}

impl FakeHarness {
    pub fn new(mode: DeliveryMode) -> Self {
        Self {
            mode,
            alive: true,
            given: Vec::new(),
            written: Vec::new(),
            seen: HashSet::new(),
            open: Vec::new(),
            followups: Vec::new(),
            finished: HashSet::new(),
            failed: Vec::new(),
            turns: std::collections::BTreeMap::new(),
            turns_started: 0,
            quota_reset_at_ms: None,
            auto_finish: false,
            resumed: Vec::new(),
        }
    }

    /// Later `deliver` calls fail and write nothing.
    pub fn kill(&mut self) {
        self.alive = false;
    }

    pub fn set_quota(&mut self, reset_at_ms: Option<i64>) {
        self.quota_reset_at_ms = reset_at_ms;
    }

    /// When set, a message written into the open turn finishes that turn inside `deliver`.
    pub fn set_auto_finish(&mut self, yes: bool) {
        self.auto_finish = yes;
    }

    /// Finish the open turn. Returns its highest sequence, then promotes staged followups
    /// into a new open turn. Does not mark those followups processed.
    pub fn complete_turn(&mut self) -> Option<i64> {
        if self.open.is_empty() {
            return None;
        }
        let seq = self.open.iter().map(|message| message.seq).max();
        for message in self.open.drain(..) {
            self.finished.insert(message.id);
        }
        if !self.followups.is_empty() {
            self.turns_started += 1;
            self.open.append(&mut self.followups);
        }
        seq
    }

    /// Finish the open turn as a failed one. Its ids stay written and are not
    /// marked processed by the supervisor.
    pub fn fail_turn(&mut self, reason: &str) -> Option<i64> {
        if self.open.is_empty() {
            return None;
        }
        let seq = self.open.iter().map(|message| message.seq).max();
        for message in self.open.drain(..) {
            self.finished.insert(message.id.clone());
            self.failed.push((message.id, reason.to_string()));
        }
        if !self.followups.is_empty() {
            self.turns_started += 1;
            self.open.append(&mut self.followups);
        }
        seq
    }

    pub fn given_ids(&self) -> &[String] {
        &self.given
    }

    pub fn written_ids(&self) -> &[String] {
        &self.written
    }

    pub fn turns_started(&self) -> usize {
        self.turns_started
    }

    pub fn open_ids(&self) -> Vec<String> {
        self.open.iter().map(|message| message.id.clone()).collect()
    }

    fn accept(&mut self, message: &OutboundMessage, into_open: bool) -> DeliverOutcome {
        let written = Written {
            id: message.id.clone(),
            seq: message.seq,
        };
        if into_open {
            if self.open.is_empty() {
                self.turns_started += 1;
            }
            self.open.push(written);
        } else {
            self.followups.push(written);
        }
        self.seen.insert(message.id.clone());
        *self.turns.entry(message.id.clone()).or_insert(0) += 1;
        self.written.push(message.id.clone());
        if self.auto_finish && self.open.iter().any(|item| item.id == message.id) {
            self.complete_turn();
        }
        DeliverOutcome::Accepted {
            turn_finished: self.finished.contains(&message.id),
        }
    }
}

impl Harness for FakeHarness {
    fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        self.given.push(message.id.clone());
        if !self.alive {
            return Err(HarnessError::Dead);
        }
        if self.seen.contains(&message.id) {
            return Ok(DeliverOutcome::Accepted {
                turn_finished: self.finished.contains(&message.id),
            });
        }
        if let Some(reset_at_ms) = self.quota_reset_at_ms {
            return Ok(DeliverOutcome::Quota { reset_at_ms });
        }
        Ok(match self.mode {
            DeliveryMode::Steer => self.accept(message, true),
            DeliveryMode::Followup => self.accept(message, self.open.is_empty()),
            DeliveryMode::Queue if !self.open.is_empty() => DeliverOutcome::Deferred,
            DeliveryMode::Queue => self.accept(message, true),
        })
    }

    fn background(&mut self) -> Result<(), HarnessError> {
        if !self.alive {
            return Err(HarnessError::Dead);
        }
        Ok(())
    }

    fn resume(&mut self, session: &str) -> Result<(), HarnessError> {
        if !self.alive {
            return Err(HarnessError::Dead);
        }
        self.resumed.push(session.to_string());
        Ok(())
    }

    fn turn_version(&self, id: &str) -> u64 {
        self.turns.get(id).copied().unwrap_or(0)
    }

    fn turn_report_for(&self, id: &str) -> Option<TurnReport> {
        if let Some((_, reason)) = self.failed.iter().rev().find(|(item, _)| item == id) {
            return Some(TurnReport::Failed(reason.clone()));
        }
        self.finished
            .contains(id)
            .then(|| TurnReport::Succeeded(TurnUsage::default()))
    }

    fn cancel(&mut self) -> Result<(), HarnessError> {
        if !self.alive {
            return Err(HarnessError::Dead);
        }
        // Drop the open turn without marking its messages processed.
        self.open.clear();
        Ok(())
    }

    fn health(&self) -> HarnessHealth {
        HarnessHealth {
            process_alive: self.alive,
            turn_state: if self.open.is_empty() {
                TurnState::Idle
            } else {
                TurnState::InTurn
            },
            delivery_mode: self.mode,
            steer_support: match self.mode {
                DeliveryMode::Steer => SteerSupport::Yes,
                DeliveryMode::Followup | DeliveryMode::Queue => SteerSupport::No,
            },
            quota_reset_at_ms: self.quota_reset_at_ms,
            sign_in_fault: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(id: &str, seq: i64) -> OutboundMessage {
        OutboundMessage {
            id: id.to_string(),
            seq,
            text: "hello".into(),
            sender: None,
            reply_to: None,
            source_time_ms: None,
        }
    }

    #[test]
    fn records_every_id_and_writes_once() {
        let mut harness = FakeHarness::new(DeliveryMode::Steer);
        harness.deliver(&message("m1", 1)).unwrap();
        harness.deliver(&message("m1", 1)).unwrap();
        assert_eq!(harness.given_ids(), ["m1", "m1"]);
        assert_eq!(harness.written_ids(), ["m1"]);
        assert_eq!(harness.turns_started(), 1);
        harness.background().unwrap();
        harness.resume("session").unwrap();
        assert_eq!(harness.resumed, ["session"]);
        harness.cancel().unwrap();
        assert!(harness.health().process_alive);
        harness.kill();
        assert!(matches!(
            harness.deliver(&message("m2", 2)),
            Err(HarnessError::Dead)
        ));
        assert!(harness.background().is_err());
        assert!(harness.resume("again").is_err());
        assert!(harness.cancel().is_err());
        assert!(!harness.health().process_alive);
        assert_eq!(harness.written_ids(), ["m1"]);
    }

    #[test]
    fn quota_does_not_write_and_duplicate_wins_over_quota() {
        let mut harness = FakeHarness::new(DeliveryMode::Steer);
        harness.set_quota(Some(9_000));
        let outcome = harness.deliver(&message("m1", 1)).unwrap();
        assert_eq!(outcome, DeliverOutcome::Quota { reset_at_ms: 9_000 });
        assert!(harness.written_ids().is_empty());
        harness.set_quota(None);
        harness.deliver(&message("m1", 1)).unwrap();
        harness.set_quota(Some(9_000));
        let replay = harness.deliver(&message("m1", 1)).unwrap();
        assert_eq!(
            replay,
            DeliverOutcome::Accepted {
                turn_finished: false
            }
        );
        assert_eq!(harness.written_ids(), ["m1"]);
        assert_eq!(harness.turns_started(), 1);
    }

    #[test]
    fn queue_defers_without_remembering_the_id() {
        let mut harness = FakeHarness::new(DeliveryMode::Queue);
        harness.deliver(&message("m1", 1)).unwrap();
        assert_eq!(
            harness.deliver(&message("m2", 2)).unwrap(),
            DeliverOutcome::Deferred
        );
        assert_eq!(harness.written_ids(), ["m1"]);
        assert_eq!(harness.complete_turn(), Some(1));
        harness.deliver(&message("m2", 2)).unwrap();
        assert_eq!(harness.written_ids(), ["m1", "m2"]);
        assert_eq!(harness.turns_started(), 2);
    }

    #[test]
    fn auto_finish_closes_the_open_turn_only() {
        let mut harness = FakeHarness::new(DeliveryMode::Followup);
        harness.set_auto_finish(true);
        let first = harness.deliver(&message("m1", 1)).unwrap();
        assert_eq!(
            first,
            DeliverOutcome::Accepted {
                turn_finished: true
            }
        );
        assert_eq!(harness.health().turn_state, TurnState::Idle);
    }
}
