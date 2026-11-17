use super::*;

fn schedule_row(store: &Store, id: &str) -> Result<AgentSchedule, Error> {
    Ok(store.get_schedule(id)?.unwrap())
}

fn schedule(agent: &Agent, kind: &str) -> NewSchedule {
    NewSchedule {
        agent_id: agent.id.clone(),
        kind: kind.into(),
        spec: if kind == "watch" {
            "github:pr:example/repo#1"
        } else {
            "1m"
        }
        .into(),
        prompt: format!("{kind} tick"),
        interval_ms: (kind == "every").then_some(60_000),
        next_fire_at_ms: Some(60_000),
        watch_baseline: None,
    }
}

#[test]
fn archive_cancels_timers_and_watches_but_preserves_history() {
    let dir = crate::test_tempdir();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let agent = store.create_agent(NewAgent::new("worker", "test")).unwrap();
    let timer = store.create_schedule(schedule(&agent, "every")).unwrap();
    let watch = store.create_schedule(schedule(&agent, "watch")).unwrap();
    let pending = store
        .coalesce_schedule_tick(&timer.id, 60_000, 120_000)
        .unwrap();
    let mut fired_input = schedule(&agent, "at");
    fired_input.spec = "historical".into();
    let fired = store.create_schedule(fired_input).unwrap();
    assert!(
        store
            .claim_schedule_tick(
                &fired.id,
                Some(60_000),
                "fired",
                None,
                Some(60_000),
                None,
                None
            )
            .unwrap()
    );
    store.set_agent_state(&agent.id, "archived").unwrap();
    for id in [&timer.id, &watch.id] {
        let row = schedule_row(&store, id).unwrap();
        assert_eq!(row.state, "cancelled");
        assert_eq!(row.next_fire_at_ms, None);
        assert!(
            !store
                .claim_schedule_tick(id, Some(60_000), "fired", None, Some(60_000), None, None)
                .unwrap()
        );
    }
    assert_eq!(schedule_row(&store, &fired.id).unwrap().state, "fired");
    assert_eq!(store.schedule_pending(&timer.id).unwrap(), Some(pending));
    assert_eq!(store.take_schedule_pending(&timer.id).unwrap(), None);
    assert!(
        store
            .coalesce_schedule_tick(&timer.id, 120_000, 180_000)
            .is_err()
    );
    assert_eq!(store.schedule_pending(&timer.id).unwrap(), Some(pending));
    // Even after an explicit revival, cancelled schedules never rearm.
    store.set_agent_state(&agent.id, "running").unwrap();
    assert_eq!(schedule_row(&store, &timer.id).unwrap().state, "cancelled");
}

#[test]
fn fired_schedule_pending_remains_consumable() {
    let dir = crate::test_tempdir();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let agent = store.create_agent(NewAgent::new("worker", "test")).unwrap();
    let timer = store.create_schedule(schedule(&agent, "every")).unwrap();
    let pending = store
        .coalesce_schedule_tick(&timer.id, 60_000, 120_000)
        .unwrap();
    assert!(
        store
            .claim_schedule_tick(
                &timer.id,
                Some(120_000),
                "fired",
                None,
                Some(120_000),
                None,
                None
            )
            .unwrap()
    );
    assert_eq!(
        store.get_schedule(&timer.id).unwrap().unwrap().state,
        "fired"
    );
    assert_eq!(
        store.take_schedule_pending(&timer.id).unwrap(),
        Some(pending)
    );
    assert_eq!(store.schedule_pending(&timer.id).unwrap(), None);
    assert_eq!(store.take_schedule_pending(&timer.id).unwrap(), None);
    assert_eq!(store.take_schedule_pending("sch_missing").unwrap(), None);
}

#[test]
fn archive_then_create_refuses_all_kinds_before_deduplication() {
    let dir = crate::test_tempdir();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let agent = store.create_agent(NewAgent::new("worker", "test")).unwrap();
    let old = store.create_schedule(schedule(&agent, "every")).unwrap();
    store.set_agent_state(&agent.id, "archived").unwrap();
    // Simulate a pre-upgrade active row: deduplication must not bypass owner admission.
    store
        .write_tx(|tx| {
            tx.execute(
                "UPDATE agent_schedules SET state = 'active' WHERE id = ?1",
                [&old.id],
            )?;
            Ok(())
        })
        .unwrap();
    for kind in ["every", "watch", "at"] {
        assert!(matches!(
            store.create_schedule(schedule(&agent, kind)),
            Err(Error::Precondition {
                machine: "schedule",
                ..
            })
        ));
    }
}

#[test]
fn typed_archive_and_failed_reservation_cancel_in_their_own_transaction() {
    for failed_reservation in [false, true] {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store.create_agent(NewAgent::new("worker", "test")).unwrap();
        let timer = store.create_schedule(schedule(&agent, "every")).unwrap();
        if failed_reservation {
            store.release_unadmitted_agent(&agent.id).unwrap();
        } else {
            store
                .transition_runtime(
                    &agent.id,
                    RuntimeState::Released,
                    RuntimeState::Archived,
                    RuntimeChange::default(),
                )
                .unwrap();
        }
        assert_eq!(store.agent(&agent.id).unwrap().state, "archived");
        assert_eq!(schedule_row(&store, &timer.id).unwrap().state, "cancelled");
    }
}

#[test]
fn archive_cancellation_failure_rolls_back_the_owner_transition() {
    for path in ["legacy", "runtime", "reservation"] {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store.create_agent(NewAgent::new("worker", "test")).unwrap();
        let timer = store.create_schedule(schedule(&agent, "every")).unwrap();
        store.write_tx(|tx| {
            tx.execute_batch("CREATE TRIGGER reject_cancel BEFORE UPDATE OF state ON agent_schedules
                WHEN NEW.state = 'cancelled' BEGIN SELECT RAISE(ABORT, 'injected cancellation failure'); END")?;
            Ok(())
        }).unwrap();
        let rt = store.agent_runtime(&agent.id).unwrap().unwrap();
        let result = match path {
            "legacy" => store.set_agent_state(&agent.id, "archived").map(|_| ()),
            "runtime" => store
                .transition_runtime(
                    &agent.id,
                    RuntimeState::Released,
                    RuntimeState::Archived,
                    RuntimeChange::default(),
                )
                .map(|_| ()),
            _ => store.release_unadmitted_agent(&agent.id).map(|_| ()),
        };
        assert!(result.is_err(), "{path}");
        let owner = store.agent(&agent.id).unwrap();
        assert_eq!(owner.name, "worker", "{path}");
        assert_eq!(owner.state, agent.state, "{path}");
        assert_eq!(
            store.agent_runtime(&agent.id).unwrap().unwrap().state,
            rt.state,
            "{path}"
        );
        assert_eq!(
            schedule_row(&store, &timer.id).unwrap().state,
            "active",
            "{path}"
        );
    }
}

#[test]
fn refused_archive_paths_do_not_cancel_active_schedules() {
    let dir = crate::test_tempdir();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let agent = store.create_agent(NewAgent::new("worker", "test")).unwrap();
    store.set_agent_running(&agent.id, "/work").unwrap();
    let timer = store.create_schedule(schedule(&agent, "every")).unwrap();
    assert!(store.release_unadmitted_agent(&agent.id).is_err());
    assert!(
        store
            .transition_runtime(
                &agent.id,
                RuntimeState::Paused,
                RuntimeState::Archived,
                RuntimeChange::default()
            )
            .is_err()
    );
    assert_eq!(schedule_row(&store, &timer.id).unwrap().state, "active");
}

#[test]
fn archived_watch_history_is_not_rearmed_by_startup_repair() {
    let dir = crate::test_tempdir();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let agent = store.create_agent(NewAgent::new("worker", "test")).unwrap();
    let watch = store.create_schedule(schedule(&agent, "watch")).unwrap();
    store.write_tx(|tx| {
        tx.execute("UPDATE agent_schedules SET state = 'fired', last_observed = 'checks_completed' WHERE id = ?1", [&watch.id])?;
        Ok(())
    }).unwrap();
    assert_eq!(store.restore_checks_completed_pr_watches().unwrap(), 1);
    store
        .write_tx(|tx| {
            tx.execute(
                "UPDATE agent_schedules SET state = 'fired' WHERE id = ?1",
                [&watch.id],
            )?;
            Ok(())
        })
        .unwrap();
    store.set_agent_state(&agent.id, "archived").unwrap();
    assert_eq!(store.restore_checks_completed_pr_watches().unwrap(), 0);
    assert_eq!(schedule_row(&store, &watch.id).unwrap().state, "fired");
}

#[test]
fn archive_racing_creation_serializes_to_no_active_schedule() {
    let dir = crate::test_tempdir();
    let path = dir.path().join("state.db");
    let store = Store::open(&path).unwrap();
    let agent = store.create_agent(NewAgent::new("worker", "test")).unwrap();
    let other = Store::open(&path).unwrap();
    let start = std::sync::Arc::new(std::sync::Barrier::new(2));
    std::thread::scope(|scope| {
        let start_create = std::sync::Arc::clone(&start);
        let owner = agent.clone();
        let create = scope.spawn(move || {
            start_create.wait();
            other.create_schedule(schedule(&owner, "every"))
        });
        start.wait();
        store.set_agent_state(&agent.id, "archived").unwrap();
        match create.join().unwrap() {
            Ok(row) => assert_eq!(schedule_row(&store, &row.id).unwrap().state, "cancelled"),
            Err(err) => assert!(matches!(
                err,
                Error::Precondition {
                    machine: "schedule",
                    ..
                }
            )),
        }
    });
}
