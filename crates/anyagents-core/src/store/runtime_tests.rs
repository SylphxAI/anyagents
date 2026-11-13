//! Runtime state machines, credential bindings, harness leases, lifecycle ops
//! and schedule coalescing, through the production Store API.
use super::*;

fn setup() -> (tempfile::TempDir, Store, Agent) {
    let dir = crate::test_tempdir();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let agent = store.create_agent(NewAgent::new("worker", "test")).unwrap();
    (dir, store, agent)
}

fn receive(store: &Store, agent: &Agent, text: &str) -> Message {
    match store.receive(&agent.id, Incoming::new(text)).unwrap() {
        ReceiveOutcome::New(message) | ReceiveOutcome::Duplicate(message) => message,
    }
}

fn legacy_state(store: &Store, agent: &Agent) -> String {
    store.agent(&agent.id).unwrap().state
}

fn legacy_turn(store: &Store, agent: &Agent) -> (i64, i64, i64) {
    store
        .conn()
        .query_row(
            "SELECT opened_seq, completed_seq, recovery_count FROM turn_recovery WHERE agent_id = ?1",
            [agent.id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap()
}

/// Drive a fresh agent to `live` and return the live generation.
fn go_live(store: &Store, agent: &Agent) -> i64 {
    let start = store
        .transition_runtime(
            &agent.id,
            RuntimeState::Released,
            RuntimeState::Starting,
            RuntimeChange::default(),
        )
        .unwrap();
    store
        .transition_runtime(
            &agent.id,
            RuntimeState::Starting,
            RuntimeState::Live,
            RuntimeChange::default(),
        )
        .unwrap();
    start.generation
}

fn holder(pid: i64) -> ProcessIdentity {
    ProcessIdentity {
        pid,
        start_time: 1_000 + pid,
    }
}

#[test]
fn new_agent_has_a_released_runtime_row() {
    let (_dir, store, agent) = setup();
    let runtime = store.agent_runtime(&agent.id).unwrap().unwrap();
    assert_eq!(runtime.state, RuntimeState::Released);
    assert_eq!((runtime.generation, runtime.attempt), (0, 0));
    assert!(runtime.fault.is_none());
}

#[test]
fn runtime_transition_table_matches_the_adr() {
    use RuntimeState::*;
    let allowed = [
        (Released, Starting),
        (Starting, Live),
        (Starting, Backoff),
        (Live, Backoff),
        (Live, QuotaBlocked),
        (Backoff, Starting),
        (QuotaBlocked, Starting),
        (Live, Released),
        (Starting, Draining),
        (Live, Draining),
        (Backoff, Draining),
        (QuotaBlocked, Draining),
        (Draining, Paused),
        (Draining, Archived),
        (Draining, Released),
        (Released, Paused),
        (Faulted, Paused),
        (Released, Archived),
        (Paused, Archived),
        (Faulted, Archived),
        (Paused, Released),
        (Faulted, Released),
        (Archived, Released),
    ];
    for &from in RuntimeState::ALL {
        for &to in RuntimeState::ALL {
            let expected = allowed.contains(&(from, to))
                || (to == Faulted && from != Archived && from != Faulted);
            assert_eq!(from.allows(to), expected, "{from} -> {to}");
        }
    }
}

#[test]
fn runtime_transition_is_compare_and_set_and_writes_nothing_on_refusal() {
    let (_dir, store, agent) = setup();
    let err = store
        .transition_runtime(
            &agent.id,
            RuntimeState::Live,
            RuntimeState::Released,
            RuntimeChange::default(),
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            Error::StateConflict {
                machine: "runtime",
                ..
            }
        ),
        "{err:?}"
    );
    let err = store
        .transition_runtime(
            &agent.id,
            RuntimeState::Released,
            RuntimeState::Live,
            RuntimeChange::default(),
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            Error::InvalidTransition {
                machine: "runtime",
                ..
            }
        ),
        "{err:?}"
    );
    let runtime = store.agent_runtime(&agent.id).unwrap().unwrap();
    assert_eq!(
        (runtime.state, runtime.generation),
        (RuntimeState::Released, 0)
    );
    assert_eq!(legacy_state(&store, &agent), "created");
}

#[test]
fn starting_opens_a_generation_and_backoff_counts_attempts() {
    let (_dir, store, agent) = setup();
    let generation = go_live(&store, &agent);
    assert_eq!(generation, 1);
    assert_eq!(legacy_state(&store, &agent), "running");
    let err = store
        .transition_runtime(
            &agent.id,
            RuntimeState::Live,
            RuntimeState::Backoff,
            RuntimeChange::default(),
        )
        .unwrap_err();
    assert!(err.to_string().contains("until_ms"), "{err}");
    let backoff = store
        .transition_runtime(
            &agent.id,
            RuntimeState::Live,
            RuntimeState::Backoff,
            RuntimeChange {
                until_ms: Some(42),
                ..RuntimeChange::default()
            },
        )
        .unwrap();
    assert_eq!((backoff.attempt, backoff.until_ms), (1, Some(42)));
    let restart = store
        .transition_runtime(
            &agent.id,
            RuntimeState::Backoff,
            RuntimeState::Starting,
            RuntimeChange::default(),
        )
        .unwrap();
    assert_eq!(
        (restart.generation, restart.attempt, restart.until_ms),
        (2, 1, None)
    );
}

#[test]
fn faulted_requires_a_fault_and_other_states_clear_it() {
    let (_dir, store, agent) = setup();
    assert!(
        store
            .transition_runtime(
                &agent.id,
                RuntimeState::Released,
                RuntimeState::Faulted,
                RuntimeChange::default()
            )
            .is_err()
    );
    let fault = Fault {
        kind: FaultKind::Auth,
        detail: "refused".into(),
    };
    let faulted = store
        .transition_runtime(
            &agent.id,
            RuntimeState::Released,
            RuntimeState::Faulted,
            RuntimeChange {
                fault: Some(fault.clone()),
                ..RuntimeChange::default()
            },
        )
        .unwrap();
    assert_eq!(faulted.fault, Some(fault));
    assert_eq!(legacy_state(&store, &agent), "running");
    let released = store
        .transition_runtime(
            &agent.id,
            RuntimeState::Faulted,
            RuntimeState::Released,
            RuntimeChange::default(),
        )
        .unwrap();
    assert!(released.fault.is_none());
    assert_eq!(legacy_state(&store, &agent), "idle");
}

#[test]
fn release_requires_closed_turn_drained_mailbox_and_quiet_children() {
    let (_dir, store, agent) = setup();
    let generation = go_live(&store, &agent);
    let message = receive(&store, &agent, "hello");
    let err = store
        .transition_runtime(
            &agent.id,
            RuntimeState::Live,
            RuntimeState::Released,
            RuntimeChange::default(),
        )
        .unwrap_err();
    assert!(matches!(err, Error::Precondition { .. }), "{err:?}");
    store.advance_delivered(&agent.id, message.seq).unwrap();
    let turn = store
        .begin_turn(&agent.id, generation, message.seq)
        .unwrap();
    assert!(matches!(
        store
            .transition_runtime(
                &agent.id,
                RuntimeState::Live,
                RuntimeState::Released,
                RuntimeChange::default()
            )
            .unwrap_err(),
        Error::Precondition { .. }
    ));
    store
        .end_turn(&agent.id, &turn.turn_id, TurnOutcome::Completed)
        .unwrap();

    let mut child = NewAgent::new("helper", "test");
    child.parent_id = Some(agent.id.clone());
    let child = store.create_agent(child).unwrap();
    go_live(&store, &child);
    assert!(matches!(
        store
            .transition_runtime(
                &agent.id,
                RuntimeState::Live,
                RuntimeState::Released,
                RuntimeChange::default()
            )
            .unwrap_err(),
        Error::Precondition { .. }
    ));
    let fault = Fault {
        kind: FaultKind::Fatal,
        detail: "stopped".into(),
    };
    store
        .transition_runtime(
            &child.id,
            RuntimeState::Live,
            RuntimeState::Faulted,
            RuntimeChange {
                fault: Some(fault),
                ..RuntimeChange::default()
            },
        )
        .unwrap();
    let released = store
        .transition_runtime(
            &agent.id,
            RuntimeState::Live,
            RuntimeState::Released,
            RuntimeChange::default(),
        )
        .unwrap();
    assert_eq!(
        (released.state, released.attempt),
        (RuntimeState::Released, 0)
    );
}

#[test]
fn losing_the_process_interrupts_the_open_turn_in_the_same_commit() {
    let (_dir, store, agent) = setup();
    let generation = go_live(&store, &agent);
    let message = receive(&store, &agent, "work");
    let turn = store
        .begin_turn(&agent.id, generation, message.seq)
        .unwrap();
    store
        .transition_runtime(
            &agent.id,
            RuntimeState::Live,
            RuntimeState::Backoff,
            RuntimeChange {
                until_ms: Some(1),
                ..RuntimeChange::default()
            },
        )
        .unwrap();
    let interrupted = store.unfinished_turn(&agent.id).unwrap().unwrap();
    assert_eq!(
        (interrupted.turn_id, interrupted.state),
        (turn.turn_id, TurnState::Interrupted)
    );
    assert_eq!(
        legacy_turn(&store, &agent),
        (message.seq, message.seq - 1, 0)
    );
}

#[test]
fn turn_lifecycle_mirrors_turn_recovery() {
    let (_dir, store, agent) = setup();
    let generation = go_live(&store, &agent);
    let first = receive(&store, &agent, "one");
    let second = receive(&store, &agent, "two");
    let err = store
        .begin_turn(&agent.id, generation + 1, first.seq)
        .unwrap_err();
    assert!(
        matches!(
            err,
            Error::StateConflict {
                machine: "runtime",
                ..
            }
        ),
        "{err:?}"
    );
    let turn = store.begin_turn(&agent.id, generation, first.seq).unwrap();
    assert_eq!(legacy_turn(&store, &agent), (first.seq, first.seq - 1, 0));
    assert!(store.begin_turn(&agent.id, generation, second.seq).is_err());
    let turn = store
        .extend_turn(&agent.id, &turn.turn_id, second.seq)
        .unwrap();
    assert_eq!(turn.last_seq, second.seq);
    assert!(
        store
            .extend_turn(&agent.id, &turn.turn_id, first.seq)
            .is_err()
    );
    assert!(store.has_open_turn(&agent.id).unwrap());
    let closed = store
        .end_turn(&agent.id, &turn.turn_id, TurnOutcome::Completed)
        .unwrap();
    assert_eq!(
        (closed.state, closed.outcome),
        (TurnState::Closed, Some(TurnOutcome::Completed))
    );
    assert!(!store.has_open_turn(&agent.id).unwrap());
    assert_eq!(legacy_turn(&store, &agent), (second.seq, second.seq, 0));
    assert!(
        store
            .end_turn(&agent.id, &turn.turn_id, TurnOutcome::Completed)
            .is_err()
    );
    assert!(store.unfinished_turn(&agent.id).unwrap().is_none());
}

#[test]
fn recovery_is_offered_up_to_the_limit_then_faults_the_runtime() {
    let (_dir, store, agent) = setup();
    let mut generation = go_live(&store, &agent);
    let message = receive(&store, &agent, "work");
    let turn = store
        .begin_turn(&agent.id, generation, message.seq)
        .unwrap();
    store.interrupt_turn(&agent.id, &turn.turn_id).unwrap();
    let RecoveryOffer::Offered(offered) = store
        .offer_turn_recovery(&agent.id, &turn.turn_id, 1)
        .unwrap()
    else {
        panic!("first offer must be made");
    };
    assert_eq!(
        (offered.state, offered.recovery_count),
        (TurnState::RecoveryOffered, 1)
    );
    assert_eq!(legacy_turn(&store, &agent).2, 1);
    let next = store
        .accept_turn_recovery(&agent.id, &turn.turn_id, generation, message.seq)
        .unwrap();
    assert_eq!(next.recovers.as_deref(), Some(turn.turn_id.as_str()));
    assert_eq!((next.state, next.first_seq), (TurnState::Open, message.seq));
    let old = store
        .recent_turns(&agent.id, 10)
        .unwrap()
        .into_iter()
        .find(|t| t.turn_id == turn.turn_id)
        .unwrap();
    assert_eq!(old.outcome, Some(TurnOutcome::Recovered));

    store
        .transition_runtime(
            &agent.id,
            RuntimeState::Live,
            RuntimeState::Backoff,
            RuntimeChange {
                until_ms: Some(1),
                ..RuntimeChange::default()
            },
        )
        .unwrap();
    store
        .transition_runtime(
            &agent.id,
            RuntimeState::Backoff,
            RuntimeState::Starting,
            RuntimeChange::default(),
        )
        .unwrap();
    generation += 1;
    assert_eq!(
        store.agent_runtime(&agent.id).unwrap().unwrap().generation,
        generation
    );
    let exhausted = store
        .offer_turn_recovery(&agent.id, &next.turn_id, 1)
        .unwrap();
    assert!(matches!(exhausted, RecoveryOffer::Exhausted(_)));
    let runtime = store.agent_runtime(&agent.id).unwrap().unwrap();
    assert_eq!(runtime.state, RuntimeState::Faulted);
    assert_eq!(runtime.fault.unwrap().kind, FaultKind::RecoveryExhausted);
    assert_eq!(
        store.unfinished_turn(&agent.id).unwrap().unwrap().state,
        TurnState::Interrupted
    );
}

#[test]
fn steer_table_and_single_redelivery() {
    use SteerState::*;
    let allowed: &[(SteerState, Option<SteerState>)] = &[
        (Held, Some(Acked)),
        (Acked, None),
        (Acked, Some(Unconsumed)),
        (Acked, Some(Consumed)),
        (Held, Some(Reconciling)),
        (Acked, Some(Reconciling)),
        (Reconciling, Some(Unconsumed)),
        (Reconciling, Some(Consumed)),
        (Reconciling, None),
        (Unconsumed, Some(Held)),
        (Consumed, None),
    ];
    for &from in SteerState::ALL {
        for to in SteerState::ALL.iter().copied().map(Some).chain([None]) {
            assert_eq!(
                steer_allows(from, to),
                allowed.contains(&(from, to)),
                "{from} -> {to:?}"
            );
        }
    }

    let (_dir, store, agent) = setup();
    let first = receive(&store, &agent, "one");
    let second = receive(&store, &agent, "two");
    store.advance_delivered(&agent.id, first.seq).unwrap();
    store.advance_delivered(&agent.id, second.seq).unwrap();
    let id = second.id.as_str();
    store.hold_steer(&agent.id, id, 1, None).unwrap();
    assert!(store.hold_steer(&agent.id, id, 1, None).is_err());
    let err = store
        .transition_steer(&agent.id, id, Acked, None)
        .unwrap_err();
    assert!(
        matches!(
            err,
            Error::StateConflict {
                machine: "steer",
                ..
            }
        ),
        "{err:?}"
    );
    store
        .transition_steer(&agent.id, id, Held, Some(Acked))
        .unwrap();
    let hold = store
        .transition_steer(&agent.id, id, Acked, Some(Unconsumed))
        .unwrap()
        .unwrap();
    assert_eq!(hold.state, Unconsumed);
    assert_eq!(store.delivered_cursor(&agent.id).unwrap(), second.seq - 1);

    let again = store.hold_steer(&agent.id, id, 2, Some("turn_x")).unwrap();
    assert!(again.redelivered);
    assert_eq!(
        (again.generation, again.turn_id.as_deref()),
        (2, Some("turn_x"))
    );
    store
        .transition_steer(&agent.id, id, Held, Some(Acked))
        .unwrap();
    let err = store
        .transition_steer(&agent.id, id, Acked, Some(Unconsumed))
        .unwrap_err();
    assert!(
        matches!(
            err,
            Error::InvalidTransition {
                machine: "steer",
                ..
            }
        ),
        "{err:?}"
    );
    assert!(
        store
            .transition_steer(&agent.id, id, Acked, None)
            .unwrap()
            .is_none()
    );
    assert!(store.steer_holds(&agent.id).unwrap().is_empty());
}

#[test]
fn retiring_a_generation_archives_its_holds_only() {
    let (_dir, store, agent) = setup();
    let old = receive(&store, &agent, "old");
    let new = receive(&store, &agent, "new");
    store
        .hold_steer(&agent.id, old.id.as_str(), 1, None)
        .unwrap();
    store
        .hold_steer(&agent.id, new.id.as_str(), 2, None)
        .unwrap();
    assert_eq!(store.retire_steer_holds(&agent.id, 1).unwrap(), 1);
    let holds = store.steer_holds(&agent.id).unwrap();
    let state_of = |id: &str| holds.iter().find(|h| h.message_id == id).unwrap().state;
    assert_eq!(state_of(old.id.as_str()), SteerState::Archived);
    assert_eq!(state_of(new.id.as_str()), SteerState::Held);
    assert_eq!(store.retire_steer_holds(&agent.id, 1).unwrap(), 0);
}

#[test]
fn session_lease_is_exclusive_identity_checked_and_reclaimable() {
    let (_dir, store, agent) = setup();
    store
        .acquire_session_lease(&agent.id, Some("ses_1"), 1, holder(10), 10)
        .unwrap();
    let err = store
        .acquire_session_lease(&agent.id, Some("ses_1"), 2, holder(11), 11)
        .unwrap_err();
    assert!(
        matches!(
            err,
            Error::LeaseHeld {
                lease: "session",
                holder_pid: 10
            }
        ),
        "{err:?}"
    );
    let reused_pid = ProcessIdentity {
        pid: 10,
        start_time: 9,
    };
    assert!(
        !store
            .release_session_lease(&agent.id, 1, reused_pid)
            .unwrap()
    );
    assert!(
        !store
            .release_session_lease(&agent.id, 2, holder(10))
            .unwrap()
    );
    assert!(store.session_lease(&agent.id).unwrap().is_some());
    assert!(store.reclaim_session_leases(|_| true).unwrap().is_empty());
    let dropped = store.reclaim_session_leases(|p| p.pid != 10).unwrap();
    assert_eq!(dropped.len(), 1);
    assert_eq!(dropped[0].vendor_session_id.as_deref(), Some("ses_1"));
    let lease = store
        .acquire_session_lease(&agent.id, None, 2, holder(11), 11)
        .unwrap();
    assert_eq!((lease.generation, lease.pgid), (2, 11));
    assert!(
        store
            .release_session_lease(&agent.id, 2, holder(11))
            .unwrap()
    );
    assert!(store.session_lease(&agent.id).unwrap().is_none());
}

#[test]
fn quota_block_is_typed_and_estimates_follow_the_bounded_schedule() {
    let (_dir, store, agent) = setup();
    go_live(&store, &agent);
    let err = store
        .transition_runtime(
            &agent.id,
            RuntimeState::Live,
            RuntimeState::QuotaBlocked,
            RuntimeChange::default(),
        )
        .unwrap_err();
    assert!(err.to_string().contains("quota block"), "{err}");
    let now = 1_000_000;
    let mut expected = Vec::new();
    for attempt in 1..=7 {
        let block = store
            .block_quota(&agent.id, QuotaReset::Unknown, now)
            .unwrap();
        assert_eq!(
            (block.source, block.attempt),
            (QuotaSource::Estimated, attempt)
        );
        expected.push(block.until_ms - now);
    }
    assert_eq!(
        expected,
        [60_000, 120_000, 240_000, 480_000, 900_000, 900_000, 900_000]
    );
    let known = store
        .block_quota(&agent.id, QuotaReset::Known { until_ms: now + 5 }, now)
        .unwrap();
    assert_eq!(
        (known.source, known.attempt, known.until_ms),
        (QuotaSource::Known, 0, now + 5)
    );
    let blocked = store
        .transition_runtime(
            &agent.id,
            RuntimeState::Live,
            RuntimeState::QuotaBlocked,
            RuntimeChange::default(),
        )
        .unwrap();
    assert_eq!(blocked.until_ms, Some(now + 5));
    let estimate = store
        .block_quota(&agent.id, QuotaReset::Unknown, now)
        .unwrap();
    assert_eq!(
        estimate.attempt, 1,
        "a known reset restarts the estimate schedule"
    );
    assert!(store.clear_quota_block(&agent.id).unwrap());
    assert!(store.quota_block(&agent.id).unwrap().is_none());
    assert!(!store.clear_quota_block(&agent.id).unwrap());
}

#[test]
fn credential_values_parse_strictly_and_round_trip() {
    for text in [
        "model:anthropic",
        "tool:github",
        "tool:git_author",
        "tool:op",
        "tool:kube",
        "channel:chn_01abc",
    ] {
        assert_eq!(text.parse::<CredentialSlot>().unwrap().to_string(), text);
    }
    assert_eq!(
        "tool:github".parse::<CredentialSlot>().unwrap(),
        CredentialSlot::ToolGithub
    );
    for bad in [
        "",
        "model:",
        "model:Anthropic",
        "tool:-x",
        "vault:x",
        "channel:a/b",
        "model",
    ] {
        assert!(bad.parse::<CredentialSlot>().is_err(), "{bad}");
    }
    for text in [
        "record:oauth_subscription/anthropic/acct-a",
        "record:build_key/ci/k1",
        "pool:example",
    ] {
        assert_eq!(text.parse::<CredentialTarget>().unwrap().to_string(), text);
    }
    for bad in [
        "record:unknown/x/y",
        "record:api_key/x",
        "record:api_key/x/y/z",
        "pool:",
        "acct-a",
        "record:api_key/X/y",
    ] {
        assert!(bad.parse::<CredentialTarget>().is_err(), "{bad}");
    }
    for kind in CredentialKind::ALL {
        assert_eq!(kind.as_str().parse::<CredentialKind>().unwrap(), *kind);
    }
}

#[test]
fn binding_writes_one_audit_line_per_change_and_none_for_a_no_op() {
    let (_dir, store, agent) = setup();
    let slot: CredentialSlot = "model:anthropic".parse().unwrap();
    let a: CredentialTarget = "record:oauth_subscription/anthropic/acct-a"
        .parse()
        .unwrap();
    let b: CredentialTarget = "pool:example".parse().unwrap();
    assert!(
        store
            .bind_credential(&agent.id, &slot, &a, "", "why")
            .is_err()
    );
    let bound = store
        .bind_credential(&agent.id, &slot, &a, "operator", "initial")
        .unwrap();
    assert_eq!(
        (bound.target.clone(), bound.set_by.as_str()),
        (a.clone(), "operator")
    );
    let again = store
        .bind_credential(&agent.id, &slot, &a, "agent:x", "repeat")
        .unwrap();
    assert_eq!(again, bound);
    store
        .bind_credential(&agent.id, &slot, &b, "operator", "pool")
        .unwrap();
    assert_eq!(
        store
            .unbind_credential(&agent.id, &slot, "operator", "drop")
            .unwrap()
            .unwrap()
            .target,
        b
    );
    assert!(
        store
            .unbind_credential(&agent.id, &slot, "operator", "drop")
            .unwrap()
            .is_none()
    );
    let audit = store.credential_binding_audit(&agent.id).unwrap();
    let trail: Vec<_> = audit
        .iter()
        .map(|row| {
            (
                row.before_target.clone(),
                row.after_target.clone(),
                row.reason.as_str(),
            )
        })
        .collect();
    assert_eq!(
        trail,
        [
            (None, Some(a.clone()), "initial"),
            (Some(a), Some(b.clone()), "pool"),
            (Some(b), None, "drop"),
        ]
    );
    assert!(
        store
            .credential_binding(&agent.id, &slot)
            .unwrap()
            .is_none()
    );
}

#[test]
fn bindings_list_in_slot_order_and_observations_name_records() {
    let (_dir, store, agent) = setup();
    let record: CredentialTarget = "record:github_user/github/dev".parse().unwrap();
    for slot in ["tool:op", "model:openai", "tool:github"] {
        store
            .bind_credential(
                &agent.id,
                &slot.parse().unwrap(),
                &record,
                "operator",
                "setup",
            )
            .unwrap();
    }
    let slots: Vec<String> = store
        .credential_bindings_of(&agent.id)
        .unwrap()
        .iter()
        .map(|b| b.slot.to_string())
        .collect();
    assert_eq!(slots, ["model:openai", "tool:github", "tool:op"]);
    assert_eq!(store.list_credential_bindings().unwrap().len(), 3);
    let slot: CredentialSlot = "model:openai".parse().unwrap();
    assert!(
        store
            .record_credential_observation(&agent.id, &slot, &"pool:p".parse().unwrap(), 1, "pick")
            .is_err()
    );
    store
        .record_credential_observation(&agent.id, &slot, &record, 3, "most remaining")
        .unwrap();
    let seen = store
        .credential_observation(&agent.id, &slot)
        .unwrap()
        .unwrap();
    assert_eq!((seen.record, seen.generation), (record, 3));
}

#[test]
fn deleting_an_agent_audits_its_bindings_and_clears_runtime_rows() {
    let (_dir, store, agent) = setup();
    let slot: CredentialSlot = "tool:github".parse().unwrap();
    let target: CredentialTarget = "record:github_user/github/dev".parse().unwrap();
    store
        .bind_credential(&agent.id, &slot, &target, "operator", "setup")
        .unwrap();
    store
        .acquire_harness_lease(&agent.id, "codex", "1.0.0", holder(5))
        .unwrap();
    store
        .block_quota(&agent.id, QuotaReset::Unknown, 1)
        .unwrap();
    store.delete_agent_by(&agent.id, "operator").unwrap();
    let audit = store.credential_binding_audit(&agent.id).unwrap();
    let last = audit.last().unwrap();
    assert_eq!(
        (
            last.after_target.as_ref(),
            last.actor.as_str(),
            last.reason.as_str()
        ),
        (None, "operator", "agent deleted")
    );
    for table in [
        "credential_bindings",
        "agent_runtime",
        "harness_leases",
        "quota_blocks",
    ] {
        let n: i64 = store
            .conn()
            .query_row(
                &format!("SELECT count(*) FROM {table} WHERE agent_id = ?1"),
                [agent.id.as_str()],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n, 0, "{table}");
    }
}

#[test]
fn harness_leases_are_holder_checked_and_versions_fail_closed() {
    let (_dir, store, agent) = setup();
    let other = store.create_agent(NewAgent::new("other", "test")).unwrap();
    store
        .acquire_harness_lease(&agent.id, "codex", "1.0.0", holder(1))
        .unwrap();
    store
        .acquire_harness_lease(&other.id, "codex", "1.1.0", holder(2))
        .unwrap();
    let replaced = store
        .acquire_harness_lease(&agent.id, "codex", "1.1.0", holder(3))
        .unwrap();
    assert_eq!(replaced.holder, holder(3));
    assert!(
        !store
            .release_harness_lease(&agent.id, "codex", &holder(1))
            .unwrap()
    );
    assert_eq!(store.harness_leases("codex").unwrap().len(), 2);
    assert_eq!(
        store
            .leased_versions("codex")
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>(),
        ["1.1.0"]
    );
    assert_eq!(store.reclaim_harness_leases(|p| p.pid == 3).unwrap(), 1);
    assert!(
        store
            .release_harness_lease(&agent.id, "codex", &holder(3))
            .unwrap()
    );
    assert!(store.leased_versions("codex").unwrap().is_empty());
    store
        .conn()
        .execute_batch("DROP TABLE harness_leases")
        .unwrap();
    assert!(
        store.leased_versions("codex").is_err(),
        "a failed read is never an empty set"
    );
}

fn new_op(kind: LifecycleKind, agent_id: &AgentId, name: &str) -> NewLifecycleOp {
    NewLifecycleOp {
        kind,
        agent_id: agent_id.clone(),
        agent_name: name.into(),
        actor: "operator".into(),
        reason: "test".into(),
        evidence: serde_json::json!({ "requested": true }),
    }
}

#[test]
fn lifecycle_phases_move_forward_and_roll_back_only_before_commit() {
    use LifecyclePhase::*;
    for from in [Intent, Quiesce, Stop, Commit, Effects, Done, RolledBack] {
        for to in [Intent, Quiesce, Stop, Commit, Effects, Done, RolledBack] {
            let expected = match to {
                RolledBack => from < Commit,
                _ => !from.is_finished() && to > from,
            };
            assert_eq!(from.allows(to), expected, "{from} -> {to}");
        }
    }
    let (_dir, store, agent) = setup();
    let op = store
        .begin_lifecycle_op(&new_op(LifecycleKind::Pause, &agent.id, &agent.name))
        .unwrap();
    let err = store
        .begin_lifecycle_op(&new_op(LifecycleKind::Archive, &agent.id, &agent.name))
        .unwrap_err();
    assert!(matches!(err, Error::LifecycleOpActive { .. }), "{err:?}");
    store
        .advance_lifecycle_op(&op.op_id, Intent, Stop, serde_json::json!({"pgid": 7}))
        .unwrap();
    assert!(
        store
            .advance_lifecycle_op(&op.op_id, Intent, Commit, serde_json::Value::Null)
            .is_err()
    );
    store
        .advance_lifecycle_op(&op.op_id, Stop, Commit, serde_json::Value::Null)
        .unwrap();
    assert!(
        store
            .advance_lifecycle_op(&op.op_id, Commit, RolledBack, serde_json::Value::Null)
            .is_err()
    );
    store
        .note_lifecycle_op_error(&op.op_id, "cleanup pending")
        .unwrap();
    assert_eq!(store.unfinished_lifecycle_ops().unwrap().len(), 1);
    let done = store
        .advance_lifecycle_op(&op.op_id, Commit, Done, serde_json::Value::Null)
        .unwrap();
    assert!(done.finished_at_ms.is_some());
    assert_eq!(done.evidence.len(), 4);
    assert_eq!(done.evidence[1]["phase"], "stop");
    assert!(store.unfinished_lifecycle_ops().unwrap().is_empty());
    assert!(store.note_lifecycle_op_error(&op.op_id, "late").is_err());
    store
        .begin_lifecycle_op(&new_op(LifecycleKind::Archive, &agent.id, &agent.name))
        .unwrap();
    assert_eq!(store.lifecycle_ops_for(&agent.id, 10).unwrap().len(), 2);
}

#[test]
fn an_unfinished_spawn_reserves_its_name_until_it_rolls_back() {
    let (_dir, store, agent) = setup();
    let id = AgentId::generate();
    let err = store
        .begin_lifecycle_op(&new_op(LifecycleKind::Spawn, &id, &agent.name))
        .unwrap_err();
    assert!(matches!(err, Error::AgentExists(_)), "{err:?}");
    let op = store
        .begin_lifecycle_op(&new_op(LifecycleKind::Spawn, &id, "fresh"))
        .unwrap();
    let err = store
        .begin_lifecycle_op(&new_op(LifecycleKind::Spawn, &AgentId::generate(), "fresh"))
        .unwrap_err();
    assert!(matches!(err, Error::NameReserved(_)), "{err:?}");
    let err = store
        .create_agent(NewAgent::new("fresh", "test"))
        .unwrap_err();
    assert!(matches!(err, Error::NameReserved(_)), "{err:?}");
    store
        .advance_lifecycle_op(
            &op.op_id,
            LifecyclePhase::Intent,
            LifecyclePhase::RolledBack,
            serde_json::Value::Null,
        )
        .unwrap();
    store.create_agent(NewAgent::new("fresh", "test")).unwrap();
    let err = store
        .begin_lifecycle_op(&new_op(LifecycleKind::Pause, &AgentId::generate(), "ghost"))
        .unwrap_err();
    assert!(matches!(err, Error::AgentNotFound(_)), "{err:?}");
}

#[test]
fn blocked_schedule_ticks_coalesce_without_double_counting() {
    let (_dir, store, agent) = setup();
    let schedule = store
        .create_schedule(NewSchedule {
            agent_id: agent.id.clone(),
            kind: "every".into(),
            spec: "1m".into(),
            prompt: "tick".into(),
            interval_ms: Some(60_000),
            next_fire_at_ms: Some(60_000),
            watch_baseline: None,
        })
        .unwrap();
    assert!(store.schedule_pending(&schedule.id).unwrap().is_none());
    let first = store
        .coalesce_schedule_tick(&schedule.id, 60_000, 120_000)
        .unwrap();
    assert_eq!((first.since_ms, first.count), (60_000, 1));
    let err = store
        .coalesce_schedule_tick(&schedule.id, 60_000, 120_000)
        .unwrap_err();
    assert!(
        matches!(
            err,
            Error::StateConflict {
                machine: "schedule",
                ..
            }
        ),
        "{err:?}"
    );
    let second = store
        .coalesce_schedule_tick(&schedule.id, 120_000, 180_000)
        .unwrap();
    assert_eq!((second.since_ms, second.count), (60_000, 2));
    assert_eq!(
        store.take_schedule_pending(&schedule.id).unwrap(),
        Some(second)
    );
    assert!(store.take_schedule_pending(&schedule.id).unwrap().is_none());
    assert!(store.coalesce_schedule_tick("sch_missing", 1, 2).is_err());
}

#[test]
fn backfill_derives_runtime_and_turns_from_legacy_state_once() {
    let dir = crate::test_tempdir();
    let path = dir.path().join("state.db");
    let store = Store::open(&path).unwrap();
    let mut ids = Vec::new();
    for (name, state) in [
        ("a", "running"),
        ("b", "paused"),
        ("c", "archived"),
        ("d", "idle"),
        ("e", "running"),
    ] {
        let agent = store.create_agent(NewAgent::new(name, "test")).unwrap();
        store
            .conn()
            .execute(
                "UPDATE agents SET state = ?2 WHERE id = ?1",
                params![agent.id.as_str(), state],
            )
            .unwrap();
        ids.push(agent.id);
    }
    store
        .conn()
        .execute_batch(&format!(
            "DELETE FROM agent_runtime;
             INSERT OR REPLACE INTO turn_recovery (agent_id, opened_seq, completed_seq, recovery_count, opened_at_ms) VALUES ('{a}', 7, 4, 2, 9);
             INSERT OR REPLACE INTO turn_recovery (agent_id, opened_seq, completed_seq, recovery_count, opened_at_ms) VALUES ('{d}', 5, 5, 0, 3);",
            a = ids[0],
            d = ids[3]
        ))
        .unwrap();
    let faulted = ids[4].clone();
    let report = store
        .backfill_runtime_state(|id| (id == &faulted).then(|| "provider refused".to_string()))
        .unwrap();
    assert_eq!(
        report,
        BackfillReport {
            runtimes: 5,
            faulted: 1,
            interrupted_turns: 1,
            closed_turns: 1
        }
    );
    let state = |i: usize| store.agent_runtime(&ids[i]).unwrap().unwrap().state;
    assert_eq!(
        [state(0), state(1), state(2), state(3), state(4)],
        [
            RuntimeState::Released,
            RuntimeState::Paused,
            RuntimeState::Archived,
            RuntimeState::Released,
            RuntimeState::Faulted
        ]
    );
    let turn = store.unfinished_turn(&ids[0]).unwrap().unwrap();
    assert_eq!(
        (
            turn.state,
            turn.first_seq,
            turn.last_seq,
            turn.recovery_count
        ),
        (TurnState::Interrupted, 5, 7, 2)
    );
    assert!(store.unfinished_turn(&ids[3]).unwrap().is_none());
    assert_eq!(
        store.recent_turns(&ids[3], 5).unwrap()[0].outcome,
        Some(TurnOutcome::Completed)
    );
    assert_eq!(
        store.backfill_runtime_state(|_| None).unwrap(),
        BackfillReport::default()
    );
    drop(store);
    let reopened = Store::open(&path).unwrap();
    assert_eq!(
        reopened.recent_turns(&ids[0], 5).unwrap().len(),
        1,
        "reopen adds nothing"
    );
}

#[test]
fn opening_a_pre_runtime_store_backfills_every_agent() {
    let dir = crate::test_tempdir();
    let path = dir.path().join("state.db");
    let ids: Vec<AgentId> = {
        let store = Store::open(&path).unwrap();
        let ids = ["x", "y"]
            .into_iter()
            .map(|name| store.create_agent(NewAgent::new(name, "test")).unwrap().id)
            .collect();
        store
            .conn()
            .execute_batch("DELETE FROM agent_runtime; UPDATE agents SET state = 'paused';")
            .unwrap();
        ids
    };
    std::fs::create_dir_all(agent_config_dir(dir.path(), &ids[1])).unwrap();
    std::fs::write(
        agent_config_dir(dir.path(), &ids[1]).join(".anyagents-last-error.json"),
        r#"{"error":"legacy fault"}"#,
    )
    .unwrap();
    let store = Store::open(&path).unwrap();
    assert_eq!(
        store.agent_runtime(&ids[0]).unwrap().unwrap().state,
        RuntimeState::Paused
    );
    let faulted = store.agent_runtime(&ids[1]).unwrap().unwrap();
    assert_eq!(faulted.state, RuntimeState::Faulted);
    assert_eq!(
        faulted.fault,
        Some(Fault {
            kind: FaultKind::Legacy,
            detail: "legacy fault".into()
        })
    );
}

#[test]
fn migration_from_the_previous_schema_keeps_every_row() {
    let dir = crate::test_tempdir();
    let path = dir.path().join("state.db");
    let id = AgentId::generate();
    {
        let mut conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at_ms INTEGER NOT NULL);",
        )
        .unwrap();
        for (version, sql) in migrate::MIGRATIONS.iter().filter(|(v, _)| *v < 26) {
            let tx = conn.transaction().unwrap();
            tx.execute_batch(sql).unwrap();
            tx.execute("INSERT INTO schema_migrations VALUES (?1, 0)", [version])
                .unwrap();
            tx.commit().unwrap();
        }
        conn.execute_batch(&format!(
            "INSERT INTO agents (id, name, harness, state, created_at_ms) VALUES ('{id}', 'legacy', 'test', 'running', 1);
             INSERT INTO agent_schedules (id, agent_id, kind, spec, prompt, interval_ms, next_fire_at_ms, state, created_at_ms)
                 VALUES ('sch_legacy', '{id}', 'every', '1m', 'tick', 60000, 5, 'active', 1);"
        ))
        .unwrap();
    }
    let store = Store::open(&path).unwrap();
    assert_eq!(store.agent(&id).unwrap().name, "legacy");
    assert_eq!(
        store.agent_runtime(&id).unwrap().unwrap().state,
        RuntimeState::Released
    );
    let schedules = store.list_schedules(Some(&id), true).unwrap();
    assert_eq!(schedules.len(), 1);
    assert!(store.schedule_pending("sch_legacy").unwrap().is_none());
    assert_eq!(
        store
            .coalesce_schedule_tick("sch_legacy", 5, 60_005)
            .unwrap()
            .count,
        1
    );
}

#[test]
fn end_turn_outlasts_transient_lock_and_surfaces_on_budget_exhaustion() {
    let (dir, store, agent) = setup();
    let db = dir.path().join("state.db");
    let generation = go_live(&store, &agent);
    let first = receive(&store, &agent, "turn-1");
    store.note_delivered(&agent.id, first.seq).unwrap();
    let turn1 = store.begin_turn(&agent.id, generation, first.seq).unwrap();

    store
        .set_lock_wait(Duration::from_millis(20), Duration::from_secs(10))
        .unwrap();
    let hold = |duration| {
        let lock_conn = rusqlite::Connection::open(&db).unwrap();
        lock_conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        std::thread::spawn(move || {
            std::thread::sleep(duration);
            lock_conn.execute_batch("COMMIT").unwrap();
        })
    };

    let peer = hold(Duration::from_millis(250));
    let closed = store
        .end_turn(&agent.id, &turn1.turn_id, TurnOutcome::Completed)
        .expect("end_turn should outlast transient lock within retry budget");
    peer.join().unwrap();
    assert_eq!(
        (closed.state, closed.outcome),
        (TurnState::Closed, Some(TurnOutcome::Completed))
    );
    assert_eq!(
        store.agent_runtime(&agent.id).unwrap().unwrap().state,
        RuntimeState::Live
    );

    let second = receive(&store, &agent, "turn-2");
    store.note_delivered(&agent.id, second.seq).unwrap();
    let turn2 = store.begin_turn(&agent.id, generation, second.seq).unwrap();
    store
        .set_lock_wait(Duration::from_millis(10), Duration::from_millis(120))
        .unwrap();

    let started = std::time::Instant::now();
    let peer = hold(Duration::from_secs(2));
    let err = store
        .end_turn(&agent.id, &turn2.turn_id, TurnOutcome::Completed)
        .expect_err("exhausted retry budget must surface lock error");
    let elapsed = started.elapsed();
    peer.join().unwrap();

    assert!(err.is_transient_lock_error(), "unexpected error: {err}");
    assert!(
        elapsed < Duration::from_secs(2),
        "budget bounded the wait: {elapsed:?}"
    );
    assert_eq!(
        store.agent_runtime(&agent.id).unwrap().unwrap().state,
        RuntimeState::Live
    );
    assert_eq!(
        store
            .unfinished_turn(&agent.id)
            .unwrap()
            .map(|t| (t.turn_id, t.state)),
        Some((turn2.turn_id, TurnState::Open))
    );
}

#[test]
fn write_transaction_rolls_back_without_replaying_failed_body() {
    let (_dir, store, agent) = setup();
    let calls = std::cell::Cell::new(0);
    let result: Result<(), Error> = store.write_tx(|tx| {
        calls.set(calls.get() + 1);
        tx.execute(
            "UPDATE agents SET description = 'rolled back' WHERE id = ?1",
            [agent.id.as_str()],
        )?;
        Err(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_BUSY),
            None,
        )
        .into())
    });
    assert!(result.unwrap_err().is_transient_lock_error());
    assert_eq!(calls.get(), 1);
    assert_eq!(store.agent(&agent.id).unwrap().description, None);
    store
        .write_tx(|tx| {
            tx.execute(
                "UPDATE agents SET description = 'committed' WHERE id = ?1",
                [agent.id.as_str()],
            )?;
            Ok(())
        })
        .unwrap();
    assert_eq!(
        store.agent(&agent.id).unwrap().description.as_deref(),
        Some("committed")
    );
}
