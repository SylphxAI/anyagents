use super::super::*;
use super::helpers::{agent, setup};

#[test]
fn steered_active_turn_advances_cursor_without_lag() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let agent = store.create_agent(NewAgent::new("steered", "agy")).unwrap();

    let mut first_in = Incoming::new("long running turn");
    first_in.received_at_ms = Some(1_000);
    let ReceiveOutcome::New(first) = store.receive(&agent.id, first_in).unwrap() else {
        panic!("expected new message");
    };
    store.note_delivered(&agent.id, first.seq).unwrap();

    let mut steer_in = Incoming::new("mid-turn steer");
    steer_in.received_at_ms = Some(60_000);
    let ReceiveOutcome::New(steer) = store.receive(&agent.id, steer_in).unwrap() else {
        panic!("expected new steer");
    };
    store.note_delivered(&agent.id, steer.seq).unwrap();

    assert_eq!(store.delivered_cursor(&agent.id).unwrap(), steer.seq);
    assert!(store.has_open_turn(&agent.id).unwrap());
    assert_eq!(
        store
            .lag(&agent.id, 3_600_000)
            .unwrap()
            .oldest_undelivered_age_ms,
        None
    );
    let open = store.open_turn(&agent.id).unwrap().unwrap();
    assert_eq!(open.messages.first().unwrap().received_at_ms, 1_000);
}
#[test]
fn concurrent_turn_completion_never_sees_database_locked() {
    let (dir, store) = setup();
    let db = dir.path().join("state.db");
    let ids: Vec<AgentId> = (0..16)
        .map(|i| agent(&store, &format!("concurrent-{i}")).id)
        .collect();
    // Each worker owns a separate Store (separate connection), mirroring
    // one supervisor process per agent finishing turns at once.
    let mut handles = Vec::new();
    for id in &ids {
        let db = db.clone();
        let id = id.clone();
        handles.push(std::thread::spawn(move || {
            let store = Store::open(&db).unwrap();
            for _ in 0..25 {
                store.note_turn_completed(&id).unwrap();
                store
                    .record_usage(NewUsage {
                        agent_id: id.clone(),
                        harness: "claude-code".into(),
                        model: Some("m".into()),
                        input_tokens: 1,
                        output_tokens: 2,
                        cache_read_tokens: 0,
                        cache_write_tokens: 0,
                        reported_cost_micros: Some(3),
                        estimated_cost_micros: None,
                        billing_mode: BillingMode::Subscription,
                        quota_state: None,
                        day: "2026-01-01".into(),
                    })
                    .unwrap();
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(store.list_usage(None).unwrap().len(), 16 * 25);
}

fn usage_for(id: &AgentId) -> NewUsage {
    NewUsage {
        agent_id: id.clone(),
        harness: "claude-code".into(),
        model: Some("m".into()),
        input_tokens: 1,
        output_tokens: 2,
        cache_read_tokens: 0,
        cache_write_tokens: 0,
        reported_cost_micros: None,
        estimated_cost_micros: None,
        billing_mode: BillingMode::Subscription,
        quota_state: None,
        day: "2026-01-01".into(),
    }
}

/// Hold the writer lock from another connection for `hold`, then release.
fn hold_writer_lock(db: &Path, hold: Duration) -> std::thread::JoinHandle<()> {
    let conn = Connection::open(db).unwrap();
    conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    std::thread::spawn(move || {
        std::thread::sleep(hold);
        conn.execute_batch("COMMIT").unwrap();
    })
}

#[test]
fn turn_persistence_outlasts_a_writer_lock_longer_than_busy_timeout() {
    let (dir, store) = setup();
    let db = dir.path().join("state.db");
    let worker = agent(&store, "lock-wait");
    store.set_agent_running(&worker.id, "/work").unwrap();
    store.receive(&worker.id, Incoming::new("task")).unwrap();
    store.note_delivered(&worker.id, 1).unwrap();
    assert!(store.has_open_turn(&worker.id).unwrap());
    // Each attempt gives up after 20 ms; the peer holds the lock ~15x longer.
    store
        .set_lock_wait(Duration::from_millis(20), Duration::from_secs(10))
        .unwrap();

    let started = std::time::Instant::now();
    let peer = hold_writer_lock(&db, Duration::from_millis(300));
    store.note_turn_completed(&worker.id).unwrap();
    assert!(started.elapsed() >= Duration::from_millis(250));
    assert!(!store.has_open_turn(&worker.id).unwrap());
    peer.join().unwrap();

    let peer = hold_writer_lock(&db, Duration::from_millis(300));
    let record = store.record_usage(usage_for(&worker.id)).unwrap();
    peer.join().unwrap();
    let rows = store.list_usage(Some(&worker.id)).unwrap();
    assert_eq!(rows.len(), 1, "a retried write records exactly one row");
    assert_eq!(rows[0].id, record.id);
    assert!(started.elapsed() < Duration::from_secs(10));
}

#[test]
fn turn_persistence_surfaces_the_lock_error_once_the_budget_is_spent() {
    let (dir, store) = setup();
    let db = dir.path().join("state.db");
    let worker = agent(&store, "lock-budget");
    store.set_agent_running(&worker.id, "/work").unwrap();
    store.receive(&worker.id, Incoming::new("task")).unwrap();
    store.note_delivered(&worker.id, 1).unwrap();
    store
        .set_lock_wait(Duration::from_millis(10), Duration::from_millis(150))
        .unwrap();

    let started = std::time::Instant::now();
    let peer = hold_writer_lock(&db, Duration::from_secs(2));
    let completed = store.note_turn_completed(&worker.id);
    let usage = store.record_usage(usage_for(&worker.id));
    let elapsed = started.elapsed();
    peer.join().unwrap();

    let err = completed.expect_err("lock outlasting the budget must surface");
    assert!(is_transient_lock_error(&err), "unexpected error: {err}");
    assert!(usage.is_err_and(|e| is_transient_lock_error(&e)));
    assert!(
        elapsed < Duration::from_secs(2),
        "budget bounds the wait: {elapsed:?}"
    );
    assert!(store.has_open_turn(&worker.id).unwrap());
    assert!(store.list_usage(Some(&worker.id)).unwrap().is_empty());
}

#[test]
fn non_lock_errors_are_not_retried() {
    let (_dir, store) = setup();
    let missing = AgentId::generate();
    let started = std::time::Instant::now();
    assert!(matches!(
        store.note_turn_completed(&missing),
        Err(Error::AgentNotFound(_))
    ));
    assert!(started.elapsed() < Duration::from_millis(100));
}

#[test]
fn turn_lifecycle_recovery_outlasts_writer_lock_longer_than_busy_timeout() {
    let (dir, store) = setup();
    let db = dir.path().join("state.db");
    let worker = agent(&store, "recovery-lock-wait");
    store.set_agent_running(&worker.id, "/work").unwrap();
    for i in 1..=6 {
        store
            .receive(&worker.id, Incoming::new(format!("task {i}")))
            .unwrap();
    }
    store.note_delivered(&worker.id, 1).unwrap();
    assert!(store.has_open_turn(&worker.id).unwrap());
    store
        .set_lock_wait(Duration::from_millis(20), Duration::from_secs(10))
        .unwrap();

    // note_turn_recovered: the fleet-wide P0 error path.
    let started = std::time::Instant::now();
    let peer = hold_writer_lock(&db, Duration::from_millis(400));
    store.note_turn_recovered(&worker.id, 1).unwrap();
    assert!(
        started.elapsed() >= Duration::from_millis(350),
        "must wait for lock"
    );
    assert_eq!(
        store.open_turn(&worker.id).unwrap().unwrap().recovery_count,
        1
    );
    peer.join().unwrap();

    // advance_delivered: the next cursor step.
    let started = std::time::Instant::now();
    let peer = hold_writer_lock(&db, Duration::from_millis(400));
    let seq = store.advance_delivered(&worker.id, 2).unwrap();
    assert_eq!(seq, 2);
    assert!(started.elapsed() >= Duration::from_millis(350));
    peer.join().unwrap();

    // note_delivered: records a delivery and re-opens the turn.
    let started = std::time::Instant::now();
    let peer = hold_writer_lock(&db, Duration::from_millis(400));
    store.note_delivered(&worker.id, 3).unwrap();
    assert!(started.elapsed() >= Duration::from_millis(350));
    assert!(store.has_open_turn(&worker.id).unwrap());
    peer.join().unwrap();

    assert!(
        started.elapsed() < Duration::from_secs(10),
        "budget bounds the wait"
    );
}

#[test]
fn turn_lifecycle_recovery_surfaces_the_lock_error_once_the_budget_is_spent() {
    let (dir, store) = setup();
    let db = dir.path().join("state.db");
    let worker = agent(&store, "recovery-lock-budget");
    store.set_agent_running(&worker.id, "/work").unwrap();
    for i in 1..=3 {
        store
            .receive(&worker.id, Incoming::new(format!("task {i}")))
            .unwrap();
    }
    store.note_delivered(&worker.id, 1).unwrap();
    store
        .set_lock_wait(Duration::from_millis(10), Duration::from_millis(150))
        .unwrap();

    let started = std::time::Instant::now();
    let peer = hold_writer_lock(&db, Duration::from_secs(2));
    let recovered = store.note_turn_recovered(&worker.id, 1);
    let advanced: Result<(), Error> = store.advance_delivered(&worker.id, 2).map(|_| ());
    let noted = store.note_delivered(&worker.id, 2);
    let elapsed = started.elapsed();
    peer.join().unwrap();

    for (name, res) in [
        ("note_turn_recovered", recovered),
        ("advance_delivered", advanced),
        ("note_delivered", noted),
    ] {
        let err = res.expect_err(&format!("{name} must surface lock after budget"));
        assert!(
            is_transient_lock_error(&err),
            "{name} unexpected error: {err:?}"
        );
    }
    assert!(
        elapsed < Duration::from_secs(2),
        "budget bounds the wait: {elapsed:?}"
    );
    // No partial state change escaped the failed attempts.
    assert_eq!(
        store.open_turn(&worker.id).unwrap().unwrap().recovery_count,
        0
    );
}

#[test]
fn turn_lifecycle_non_lock_errors_are_not_retried() {
    let (_dir, store) = setup();
    let missing = AgentId::generate();

    {
        let started = std::time::Instant::now();
        let result = store.note_turn_recovered(&missing, 1);
        assert!(
            matches!(result, Err(Error::AgentNotFound(_))),
            "note_turn_recovered: {result:?}"
        );
        assert!(
            started.elapsed() < Duration::from_millis(200),
            "note_turn_recovered must not retry"
        );
    }
    {
        let started = std::time::Instant::now();
        let result = store.advance_delivered(&missing, 1).map(|_| ());
        assert!(
            matches!(result, Err(Error::AgentNotFound(_))),
            "advance_delivered: {result:?}"
        );
        assert!(
            started.elapsed() < Duration::from_millis(200),
            "advance_delivered must not retry"
        );
    }
    {
        let started = std::time::Instant::now();
        let result = store.note_delivered(&missing, 1);
        assert!(
            matches!(result, Err(Error::AgentNotFound(_))),
            "note_delivered: {result:?}"
        );
        assert!(
            started.elapsed() < Duration::from_millis(200),
            "note_delivered must not retry"
        );
    }
}
