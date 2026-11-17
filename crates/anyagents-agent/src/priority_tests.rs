//! Store-backed tests of offer order versus contiguous commit/completion order.
use super::*;
use anyagents_core::{Incoming, NewAgent, ReceiveOutcome};
use anyagents_harness::{DeliveryMode, FakeHarness, HarnessHealth};
use std::collections::HashMap;

#[derive(Clone, Copy)]
enum Failure {
    Dead,
    Failed,
}

struct Recording {
    answers: HashMap<i64, DeliverOutcome>,
    failures: HashMap<i64, Failure>,
    proofs: HashMap<String, Proof>,
    pending_owner: Option<String>,
    idle: bool,
    offered: Vec<i64>,
    applied: Vec<String>,
    base: FakeHarness,
}

impl Recording {
    fn new() -> Self {
        Self {
            answers: HashMap::new(),
            failures: HashMap::new(),
            proofs: HashMap::new(),
            pending_owner: None,
            idle: false,
            offered: Vec::new(),
            applied: Vec::new(),
            base: FakeHarness::new(DeliveryMode::Steer),
        }
    }
}

impl Harness for Recording {
    fn deliver(&mut self, m: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        self.offered.push(m.seq);
        if self.pending_owner.as_deref().is_some_and(|id| id != m.id) {
            return Ok(DeliverOutcome::Deferred);
        }
        if let Some(proof) = self.proven(&m.id) {
            return Ok(match proof {
                Proof::Taken { turn_finished } => DeliverOutcome::Accepted { turn_finished },
                Proof::Duplicate => DeliverOutcome::Duplicate,
            });
        }
        match self.failures.get(&m.seq) {
            Some(Failure::Dead) => return Err(HarnessError::Dead),
            Some(Failure::Failed) => return Err(HarnessError::Failed("transport failed".into())),
            None => {}
        }
        let result = self
            .answers
            .get(&m.seq)
            .cloned()
            .unwrap_or(DeliverOutcome::Accepted {
                turn_finished: false,
            });
        if matches!(
            result,
            DeliverOutcome::Accepted { .. } | DeliverOutcome::InFlight
        ) && !self.applied.contains(&m.id)
        {
            self.applied.push(m.id.clone());
        }
        match result {
            DeliverOutcome::Accepted { turn_finished } => {
                self.proofs
                    .insert(m.id.clone(), Proof::Taken { turn_finished });
            }
            DeliverOutcome::Duplicate => {
                self.proofs.insert(m.id.clone(), Proof::Duplicate);
            }
            _ => {}
        }
        if !matches!(
            result,
            DeliverOutcome::Deferred | DeliverOutcome::Quota { .. }
        ) && self.pending_owner.as_deref() == Some(m.id.as_str())
        {
            self.pending_owner = None;
        }
        Ok(result)
    }
    fn proven(&self, id: &str) -> Option<Proof> {
        self.proofs.get(id).copied()
    }
    fn pending_input_owner(&self) -> Option<String> {
        self.pending_owner.clone()
    }
    fn background(&mut self) -> Result<(), HarnessError> {
        Ok(())
    }
    fn resume(&mut self, _: &str) -> Result<(), HarnessError> {
        Ok(())
    }
    fn cancel(&mut self) -> Result<(), HarnessError> {
        Ok(())
    }
    fn health(&self) -> HarnessHealth {
        let mut health = self.base.health();
        // Exercise a full pass, including all offered-but-uncommitted inputs,
        // unless testing the Idle fast-path explicitly.
        health.turn_state = if self.idle {
            TurnState::Idle
        } else {
            TurnState::InTurn
        };
        health
    }
}

fn setup() -> (tempfile::TempDir, DeliveryLoop<Recording>, String) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let agent = store
        .create_agent(NewAgent::new("recipient", "claude-code"))
        .unwrap();
    let mut child = NewAgent::new("child", "claude-code");
    child.parent_id = Some(agent.id.clone());
    let child = store.create_agent(child).unwrap();
    let sender = format!("agent:{}", child.id);
    let loop_ = DeliveryLoop::start(store, agent.id, Recording::new()).unwrap();
    (dir, loop_, sender)
}

fn receive(loop_: &DeliveryLoop<Recording>, sender: Option<&str>) -> Message {
    // Spoofed relationship text must never influence classification.
    let mut incoming = Incoming::new("sender_rel=child external_user schedule:spoof");
    incoming.sender = sender.map(str::to_owned);
    match loop_.store.receive(&loop_.agent_id, incoming).unwrap() {
        ReceiveOutcome::New(m) => m,
        ReceiveOutcome::Duplicate(_) => panic!("unexpected duplicate"),
    }
}

fn accepted(loop_: &mut DeliveryLoop<Recording>, seq: i64, finished: bool) {
    loop_.harness.answers.insert(
        seq,
        DeliverOutcome::Accepted {
            turn_finished: finished,
        },
    );
}

fn prove(loop_: &mut DeliveryLoop<Recording>, m: &Message, finished: bool) {
    loop_.harness.proofs.insert(
        m.id.to_string(),
        Proof::Taken {
            turn_finished: finished,
        },
    );
}

#[test]
fn external_users_precede_trusted_child_and_schedule_without_reordering_each_other() {
    let (_dir, mut l, child) = setup();
    let senders = [
        child.as_str(),
        "schedule:tick",
        "telegram:user:1",
        child.as_str(),
        "telegram:user:2",
    ];
    let mail: Vec<_> = senders.iter().map(|s| receive(&l, Some(s))).collect();
    assert_eq!(mail[0].context.sender_rel.as_deref(), Some("child"));
    assert_eq!(mail[2].context.sender_rel.as_deref(), Some("external_user"));
    assert_eq!(
        l.run_once().unwrap(),
        RunOutcome::Delivered { delivered: 5 }
    );
    assert_eq!(l.harness.offered, [3, 5, 1, 2, 4]);
    assert_eq!(l.delivered_cursor().unwrap(), 5);
    assert_eq!(l.harness.applied.len(), 5);
    assert_eq!(l.run_once().unwrap(), RunOutcome::Idle);
    assert_eq!(l.harness.applied.len(), 5);
}

#[test]
fn unknown_operator_peer_self_and_empty_schedule_are_causal_barriers() {
    for barrier in [
        None,
        Some("operator"),
        Some("agent:unknown"),
        Some("schedule:"),
    ] {
        let (_dir, mut l, child) = setup();
        receive(&l, Some(&child));
        receive(&l, barrier);
        receive(&l, Some("telegram:user:1"));
        l.run_once().unwrap();
        assert_eq!(l.harness.offered, [1, 2, 3], "barrier {barrier:?}");
    }
    let (_dir, mut l, child) = setup();
    receive(&l, Some(&child));
    receive(&l, Some(&format!("agent:{}", l.agent_id)));
    receive(&l, Some("telegram:user:1"));
    l.run_once().unwrap();
    assert_eq!(l.harness.offered, [1, 2, 3]);
}

#[test]
fn lookahead_is_bounded_and_leaves_the_cursor_untouched() {
    let (_dir, l, child) = setup();
    let head = receive(&l, Some(&child));
    for _ in 1..priority::LOOKAHEAD {
        receive(&l, Some(&child));
    }
    let external = receive(&l, Some("telegram:user:1"));
    let (through, urgent) = priority::ahead(&l.store, &l.agent_id, &head).unwrap();
    assert_eq!(through, external.seq - 1);
    assert!(urgent.is_empty());
    assert_eq!(l.delivered_cursor().unwrap(), 0);
}

#[test]
fn all_hard_priority_gates_stop_before_background_and_preserve_earlier_acceptance() {
    for gate in 0..4 {
        let (_dir, mut l, child) = setup();
        receive(&l, Some(&child));
        let first = receive(&l, Some("telegram:user:1"));
        let blocked = receive(&l, Some("telegram:user:2"));
        match gate {
            0 => {
                l.harness
                    .answers
                    .insert(blocked.seq, DeliverOutcome::Deferred);
            }
            1 => {
                l.harness.answers.insert(
                    blocked.seq,
                    DeliverOutcome::Quota {
                        reset_at_ms: i64::MAX,
                    },
                );
            }
            2 => {
                l.harness.failures.insert(blocked.seq, Failure::Dead);
            }
            _ => {
                l.harness.failures.insert(blocked.seq, Failure::Failed);
            }
        }
        let result = l.run_once();
        match gate {
            0 => assert_eq!(result.unwrap(), RunOutcome::Idle),
            1 => assert_eq!(
                result.unwrap(),
                RunOutcome::Quota {
                    reset_at_ms: i64::MAX
                }
            ),
            2 => assert!(matches!(result, Err(Error::HarnessDown))),
            _ => assert!(matches!(result, Err(Error::HarnessFailed(_)))),
        }
        assert_eq!(l.harness.offered, [first.seq, blocked.seq]);
        assert_eq!(l.delivered_cursor().unwrap(), 0);
        assert_eq!(
            l.commit_proven().unwrap(),
            0,
            "no proof may skip the background head"
        );
        l.harness.answers.clear();
        l.harness.failures.clear();
        l.clear_quota();
        assert_eq!(
            l.run_once().unwrap(),
            RunOutcome::Delivered { delivered: 3 }
        );
        assert_eq!(
            l.harness
                .applied
                .iter()
                .filter(|id| id.as_str() == first.id.as_str())
                .count(),
            1
        );
        assert_eq!(l.delivered_cursor().unwrap(), 3);
    }
}

#[test]
fn inflight_external_keeps_the_cursor_at_the_unproven_head_and_replay_is_idempotent() {
    let (_dir, mut l, child) = setup();
    let background = receive(&l, Some(&child));
    let urgent = receive(&l, Some("telegram:user:1"));
    let later = receive(&l, Some("telegram:user:2"));
    l.harness
        .answers
        .insert(background.seq, DeliverOutcome::InFlight);
    l.harness
        .answers
        .insert(urgent.seq, DeliverOutcome::InFlight);
    assert_eq!(l.run_once().unwrap(), RunOutcome::Idle);
    assert_eq!(l.harness.offered, [2, 3, 1]);
    assert_eq!(l.delivered_cursor().unwrap(), 0);
    prove(&mut l, &background, false);
    assert_eq!(l.commit_proven().unwrap(), 1);
    assert_eq!(l.delivered_cursor().unwrap(), 1);
    prove(&mut l, &urgent, false);
    assert_eq!(l.commit_proven().unwrap(), 2);
    assert_eq!(l.delivered_cursor().unwrap(), later.seq);
    assert_eq!(l.harness.applied.len(), 3);
}

#[test]
fn a_deferred_background_head_does_not_erase_the_external_proof() {
    let (_dir, mut l, child) = setup();
    let background = receive(&l, Some(&child));
    let urgent = receive(&l, Some("telegram:user:1"));
    l.harness
        .answers
        .insert(background.seq, DeliverOutcome::Deferred);
    assert_eq!(l.run_once().unwrap(), RunOutcome::Idle);
    assert_eq!(l.delivered_cursor().unwrap(), 0);
    l.harness.answers.remove(&background.seq);
    assert_eq!(
        l.run_once().unwrap(),
        RunOutcome::Delivered { delivered: 2 }
    );
    assert_eq!(
        l.harness
            .applied
            .iter()
            .filter(|id| id.as_str() == urgent.id.as_str())
            .count(),
        1
    );
}

#[test]
fn finished_external_proofs_cannot_close_intervening_unfinished_background() {
    let (_dir, mut l, child) = setup();
    let a = receive(&l, Some(&child));
    let b = receive(&l, Some("telegram:user:1"));
    let c = receive(&l, Some(&child));
    let d = receive(&l, Some("telegram:user:2"));
    accepted(&mut l, b.seq, true);
    accepted(&mut l, d.seq, true);
    assert_eq!(
        l.run_once().unwrap(),
        RunOutcome::Delivered { delivered: 4 }
    );
    assert_eq!(l.harness.offered, [2, 4, 1, 3]);
    let open = l.store.open_turn(&l.agent_id).unwrap().unwrap();
    assert_eq!(
        open.messages.iter().map(|m| m.seq).collect::<Vec<_>>(),
        [1, 2, 3, 4]
    );
    assert!(matches!(
        l.harness.proven(a.id.as_str()),
        Some(Proof::Taken {
            turn_finished: false
        })
    ));
    assert!(matches!(
        l.harness.proven(c.id.as_str()),
        Some(Proof::Taken {
            turn_finished: false
        })
    ));
}

#[test]
fn proof_only_commit_and_retirement_do_not_complete_a_newer_background_turn() {
    for retire in [false, true] {
        let (_dir, mut l, child) = setup();
        let a = receive(&l, Some(&child));
        let b = receive(&l, Some("telegram:user:1"));
        let c = receive(&l, Some(&child));
        let d = receive(&l, Some("telegram:user:2"));
        prove(&mut l, &a, false);
        prove(&mut l, &b, true);
        prove(&mut l, &c, false);
        prove(&mut l, &d, true);
        let committed = if retire {
            l.retire_harness()
        } else {
            l.commit_proven()
        };
        assert_eq!(committed.unwrap(), 4);
        assert!(l.store.open_turn(&l.agent_id).unwrap().is_some());
        l.replace_harness(Recording::new());
        assert!(l.store.open_turn(&l.agent_id).unwrap().is_some());
    }
}

#[test]
fn missing_or_duplicate_historical_proof_is_never_completion_evidence() {
    for duplicate in [false, true] {
        let (_dir, mut l, child) = setup();
        let background = receive(&l, Some(&child));
        let urgent = receive(&l, Some("telegram:user:1"));
        l.store.note_delivered(&l.agent_id, background.seq).unwrap();
        if duplicate {
            l.harness
                .proofs
                .insert(background.id.to_string(), Proof::Duplicate);
        }
        prove(&mut l, &urgent, true);
        assert_eq!(l.commit_proven().unwrap(), 1);
        let open = l.store.open_turn(&l.agent_id).unwrap().unwrap();
        // Even a finished recovery result cannot turn absent historical proof
        // into evidence for a potentially newer background turn.
        accepted(&mut l, open.opened_seq, true);
        assert_eq!(
            l.recover_open_turn(&open).unwrap(),
            DeliverOutcome::Accepted {
                turn_finished: false
            }
        );
        assert!(l.store.open_turn(&l.agent_id).unwrap().is_some());
    }
}

#[test]
fn recovery_that_finishes_before_newer_background_cannot_close_it() {
    let (_dir, mut l, child) = setup();
    let first = receive(&l, Some("telegram:user:1"));
    l.store.note_delivered(&l.agent_id, first.seq).unwrap();
    let open = l.store.open_turn(&l.agent_id).unwrap().unwrap();
    l.harness
        .answers
        .insert(open.opened_seq, DeliverOutcome::InFlight);
    assert_eq!(
        l.recover_open_turn(&open).unwrap(),
        DeliverOutcome::InFlight
    );
    let note_id = l.recovery.as_ref().unwrap().id.clone();
    let newer = receive(&l, Some(&child));
    l.store.note_delivered(&l.agent_id, newer.seq).unwrap();
    l.harness.proofs.insert(
        note_id,
        Proof::Taken {
            turn_finished: true,
        },
    );
    let expanded = l.store.open_turn(&l.agent_id).unwrap().unwrap();
    assert_eq!(
        l.recover_open_turn(&expanded).unwrap(),
        DeliverOutcome::Accepted {
            turn_finished: false
        }
    );
    assert!(l.store.open_turn(&l.agent_id).unwrap().is_some());
}

#[test]
fn complete_historical_proofs_allow_recovery_to_finish() {
    let (_dir, mut l, child) = setup();
    let background = receive(&l, Some(&child));
    let urgent = receive(&l, Some("telegram:user:1"));
    l.store.note_delivered(&l.agent_id, background.seq).unwrap();
    l.store.note_delivered(&l.agent_id, urgent.seq).unwrap();
    prove(&mut l, &background, true);
    prove(&mut l, &urgent, true);
    let open = l.store.open_turn(&l.agent_id).unwrap().unwrap();
    accepted(&mut l, open.opened_seq, true);
    assert_eq!(
        l.recover_open_turn(&open).unwrap(),
        DeliverOutcome::Accepted {
            turn_finished: true
        }
    );
    assert!(l.store.open_turn(&l.agent_id).unwrap().is_none());
}

#[test]
fn an_external_only_finished_turn_still_completes() {
    let (_dir, mut l, _child) = setup();
    let urgent = receive(&l, Some("telegram:user:1"));
    accepted(&mut l, urgent.seq, true);
    assert_eq!(
        l.run_once().unwrap(),
        RunOutcome::Delivered { delivered: 1 }
    );
    assert!(l.store.open_turn(&l.agent_id).unwrap().is_none());
}

#[test]
fn a_priority_rejection_is_parked_only_at_contiguous_head() {
    let (_dir, mut l, child) = setup();
    let background = receive(&l, Some(&child));
    let urgent = receive(&l, Some("telegram:user:1"));
    l.harness
        .answers
        .insert(background.seq, DeliverOutcome::InFlight);
    l.harness
        .answers
        .insert(urgent.seq, DeliverOutcome::rejected("too large", true));
    assert_eq!(l.run_once().unwrap(), RunOutcome::Idle);
    assert_eq!(l.delivered_cursor().unwrap(), 0);
    assert!(l.rejections.is_empty());
    prove(&mut l, &background, false);
    assert_eq!(
        l.run_once().unwrap(),
        RunOutcome::Delivered { delivered: 1 }
    );
    assert_eq!(l.delivered_cursor().unwrap(), 2);
    assert_eq!(l.rejections.len(), 1);
    assert_eq!(l.rejections[0].message_id, urgent.id.to_string());
}

#[test]
fn pending_background_owner_resumes_only_started_input_before_urgent_mail() {
    let (_dir, mut l, child) = setup();
    let b1 = receive(&l, Some(&child));
    let b2 = receive(&l, Some(&child));
    let u1 = receive(&l, Some("telegram:user:1"));
    let u2 = receive(&l, Some("telegram:user:2"));
    let _ = (b2, u1, u2);
    l.harness.pending_owner = Some(b1.id.to_string());

    assert_eq!(
        l.run_once().unwrap(),
        RunOutcome::Delivered { delivered: 4 }
    );
    assert_eq!(l.harness.offered, [1, 3, 4, 2]);
    assert_eq!(l.delivered_cursor().unwrap(), 4);
    assert_eq!(l.harness.pending_input_owner(), None);
}

#[test]
fn pending_external_owner_resumes_without_skipping_unproven_background_head() {
    let (_dir, mut l, child) = setup();
    let b1 = receive(&l, Some(&child));
    let u1 = receive(&l, Some("telegram:user:1"));
    let u2 = receive(&l, Some("telegram:user:2"));
    l.harness.answers.insert(b1.seq, DeliverOutcome::InFlight);
    l.harness.pending_owner = Some(u1.id.to_string());

    assert_eq!(l.run_once().unwrap(), RunOutcome::Idle);
    assert_eq!(l.harness.offered, [u1.seq, u2.seq, b1.seq]);
    assert_eq!(l.delivered_cursor().unwrap(), 0);

    prove(&mut l, &b1, false);
    assert_eq!(l.commit_proven().unwrap(), 3);
    assert_eq!(l.delivered_cursor().unwrap(), 3);
    assert_eq!(l.harness.applied.len(), 3);
}

#[test]
fn pending_recovery_owner_resumes_even_when_idle_with_no_undelivered_mail() {
    let (_dir, mut l, _child) = setup();
    let first = receive(&l, Some("telegram:user:1"));
    l.store.note_delivered(&l.agent_id, first.seq).unwrap();
    let open = l.store.open_turn(&l.agent_id).unwrap().unwrap();
    l.harness.idle = true;
    l.harness
        .answers
        .insert(open.opened_seq, DeliverOutcome::Deferred);
    assert_eq!(
        l.recover_open_turn(&open).unwrap(),
        DeliverOutcome::Deferred
    );
    let note_id = l.recovery.as_ref().unwrap().id.clone();
    l.harness.pending_owner = Some(note_id);
    l.harness.answers.remove(&open.opened_seq);
    accepted(&mut l, open.opened_seq, true);

    assert_eq!(l.run_once().unwrap(), RunOutcome::Idle);
    assert_eq!(l.harness.pending_input_owner(), None);
    assert!(l.recovery.is_none());
    assert!(l.store.open_turn(&l.agent_id).unwrap().is_none());
}

#[test]
fn pending_owner_hard_gates_and_invalid_owners_fail_closed() {
    for gate in 0..4 {
        let (_dir, mut l, child) = setup();
        let b1 = receive(&l, Some(&child));
        receive(&l, Some("telegram:user:1"));
        l.harness.pending_owner = Some(b1.id.to_string());
        match gate {
            0 => {
                l.harness.answers.insert(b1.seq, DeliverOutcome::Deferred);
                assert_eq!(l.run_once().unwrap(), RunOutcome::Idle);
            }
            1 => {
                l.harness.answers.insert(
                    b1.seq,
                    DeliverOutcome::Quota {
                        reset_at_ms: i64::MAX,
                    },
                );
                assert_eq!(
                    l.run_once().unwrap(),
                    RunOutcome::Quota {
                        reset_at_ms: i64::MAX
                    }
                );
            }
            2 => {
                l.harness.failures.insert(b1.seq, Failure::Dead);
                assert!(matches!(l.run_once(), Err(Error::HarnessDown)));
            }
            _ => {
                l.harness.failures.insert(b1.seq, Failure::Failed);
                assert!(matches!(l.run_once(), Err(Error::HarnessFailed(_))));
            }
        }
        assert_eq!(l.harness.offered, [b1.seq]);
        assert_eq!(l.delivered_cursor().unwrap(), 0);
    }

    // Unknown message ID fails closed before touching any mail.
    let (_dir, mut l, child) = setup();
    let b1 = receive(&l, Some(&child));
    l.harness.pending_owner = Some("msg_unknown".into());
    assert!(matches!(l.run_once(), Err(Error::HarnessFailed(_))));
    assert!(l.harness.offered.is_empty());

    // Already-delivered message ID fails closed.
    l.harness.pending_owner = None;
    assert_eq!(
        l.run_once().unwrap(),
        RunOutcome::Delivered { delivered: 1 }
    );
    receive(&l, Some("telegram:user:1"));
    l.harness.pending_owner = Some(b1.id.to_string());
    assert!(matches!(l.run_once(), Err(Error::HarnessFailed(_))));
    assert_eq!(l.delivered_cursor().unwrap(), 1);
}
