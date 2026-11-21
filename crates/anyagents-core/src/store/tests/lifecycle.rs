use super::super::*;
use super::helpers::{agent, setup};

#[test]
fn failed_creation_releases_its_name_and_admitted_agents_keep_theirs() {
    let (_dir, store) = setup();
    let failed = agent(&store, "reserved");
    store
        .set_agent_workdir_if_missing(&failed.id, "/allocated")
        .unwrap();
    assert_eq!(store.agent(&failed.id).unwrap().state, "created");

    let released = store.release_unadmitted_agent(&failed.id).unwrap();
    assert_eq!(released.name, failed.id.as_str());
    assert_eq!(released.state, "archived");
    assert_eq!(released.workdir, None);
    assert_eq!(store.agent(&failed.id).unwrap().workdir, None);
    assert!(matches!(
        store.agent_by_name("reserved"),
        Err(Error::AgentNotFound(_))
    ));
    assert_eq!(store.agent(&failed.id).unwrap().id, failed.id);
    agent(&store, "reserved");

    for state in ["running", "paused", "archived"] {
        let admitted = agent(&store, state);
        store.set_agent_running(&admitted.id, "/work").unwrap();
        store.set_agent_state(&admitted.id, state).unwrap();
        let refused = store.release_unadmitted_agent(&admitted.id).unwrap_err();
        assert!(refused.to_string().contains("was admitted"), "{refused}");
        let retained = store.agent(&admitted.id).unwrap();
        assert_eq!(retained.name, state);
        assert_eq!(retained.state, state);
        assert_eq!(retained.workdir.as_deref(), Some("/work"));
    }
}

#[test]
fn startup_repair_releases_every_ghost_reservation_once() {
    let (_dir, store) = setup();
    let interrupted = agent(&store, "interrupted");
    let allocated = agent(&store, "allocated");
    store
        .set_agent_workdir_if_missing(&allocated.id, "/allocated")
        .unwrap();
    let ghost = agent(&store, "ghost");
    store.set_agent_state(&ghost.id, "archived").unwrap();
    let running = agent(&store, "running");
    store.set_agent_running(&running.id, "/work").unwrap();
    let retired = agent(&store, "retired");
    store.set_agent_running(&retired.id, "/work").unwrap();
    store.set_agent_state(&retired.id, "archived").unwrap();

    let mut released = store.release_unadmitted_names().unwrap();
    released.sort_by(|a, b| a.1.cmp(&b.1));
    assert_eq!(
        released,
        vec![
            (allocated.id.clone(), "allocated".to_string()),
            (ghost.id.clone(), "ghost".to_string()),
            (interrupted.id.clone(), "interrupted".to_string()),
        ]
    );
    assert!(store.release_unadmitted_names().unwrap().is_empty());
    for released in [&allocated, &ghost, &interrupted] {
        let retained = store.agent(&released.id).unwrap();
        assert_eq!(retained.name, released.id.as_str());
        assert_eq!(retained.state, "archived");
        assert_eq!(retained.workdir, None);
        agent(&store, &released.name);
    }
    assert_eq!(store.agent(&running.id).unwrap().name, "running");
    assert_eq!(store.agent(&retired.id).unwrap().name, "retired");
}

#[test]
fn allocated_workdir_is_durable_without_starting_or_waking() {
    let (_dir, store) = setup();
    let agent = agent(&store, "alpha");
    let wakes = store.subscribe();
    let allocated = store
        .set_agent_workdir_if_missing(&agent.id, "/allocated")
        .unwrap();
    assert_eq!(allocated.state, agent.state);
    assert_eq!(allocated.workdir.as_deref(), Some("/allocated"));
    assert_eq!(store.agent(&agent.id).unwrap().workdir, allocated.workdir);
    let unchanged = store
        .set_agent_workdir_if_missing(&agent.id, "/stale")
        .unwrap();
    assert_eq!(unchanged.workdir, allocated.workdir);
    assert!(wakes.try_recv().is_err());
    assert!(matches!(
        store
            .set_agent_workdir_if_missing(&AgentId::generate(), "/missing")
            .unwrap_err(),
        Error::AgentNotFound(_)
    ));
}

#[test]
fn set_agent_running_stores_the_workdir_and_wakes() {
    let (_dir, store) = setup();
    let agent = agent(&store, "alpha");
    let wakes = store.subscribe();
    let running = store.set_agent_running(&agent.id, "/work").unwrap();
    assert_eq!(running.state, "running");
    assert_eq!(running.workdir.as_deref(), Some("/work"));
    assert!(matches!(
        wakes
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap(),
        Wake::Agent { .. }
    ));
    let missing = AgentId::generate();
    assert!(matches!(
        store.set_agent_running(&missing, "/work").unwrap_err(),
        Error::AgentNotFound(_)
    ));
}

#[test]
fn idle_release_preserves_pending_work_and_role_independent_identity() {
    let (_dir, store) = setup();
    let parent = agent(&store, "parent");
    for (name, parent_id, role) in [
        ("root", None, Some("Researcher")),
        ("labelled", Some(parent.id.clone()), Some("Researcher")),
        ("plain", Some(parent.id.clone()), None),
    ] {
        let mut new = NewAgent::new(name, "codex");
        new.parent_id = parent_id;
        new.role = role.map(str::to_string);
        new.instructions = Some("Keep serving future requests.".into());
        let agent = store.create_agent(new).unwrap();
        store.set_agent_running(&agent.id, "/work").unwrap();
        store.receive(&agent.id, Incoming::new("task")).unwrap();
        assert!(!store.complete_idle_agent(&agent.id).unwrap());
        store.note_delivered(&agent.id, 1).unwrap();
        assert!(!store.complete_idle_agent(&agent.id).unwrap());
        store.note_turn_completed(&agent.id).unwrap();
        assert!(store.complete_idle_agent(&agent.id).unwrap());
        let idle = store.agent(&agent.id).unwrap();
        assert_eq!(idle.role, agent.role);
        assert_eq!(idle.instructions, agent.instructions);
        assert_eq!(store.delivered_cursor(&agent.id).unwrap(), 1);
        store
            .receive(&agent.id, Incoming::new("follow-up"))
            .unwrap();
        assert_eq!(store.agent(&agent.id).unwrap().state, "running");
        assert!(!store.complete_idle_agent(&agent.id).unwrap());
    }
}

#[test]
fn idle_release_review_of_an_ineligible_agent_never_takes_the_writer_lock() {
    let (dir, store) = setup();
    let parent = agent(&store, "parent");
    store.set_agent_running(&parent.id, "/work").unwrap();
    store.receive(&parent.id, Incoming::new("task")).unwrap();
    store.note_delivered(&parent.id, 1).unwrap();
    store.note_turn_completed(&parent.id).unwrap();
    let mut new = NewAgent::new("child", "codex");
    new.parent_id = Some(parent.id.clone());
    let child = store.create_agent(new).unwrap();
    store.set_agent_running(&child.id, "/work").unwrap();
    // A peer holds the writer lock and this store never waits for it: any
    // write attempt fails at once with SQLITE_BUSY.
    store.set_lock_wait(Duration::ZERO, Duration::ZERO).unwrap();
    let peer = Connection::open(dir.path().join("state.db")).unwrap();
    peer.execute_batch("BEGIN IMMEDIATE").unwrap();
    // The parent has a running child: not eligible, answered by a read.
    assert!(!store.complete_idle_agent(&parent.id).unwrap());
    // The child never completed a turn: not eligible either.
    assert!(!store.complete_idle_agent(&child.id).unwrap());
    // An eligible agent still takes the guarded write once the lock frees.
    peer.execute_batch("ROLLBACK").unwrap();
    store.set_agent_state(&child.id, "paused").unwrap();
    assert!(store.complete_idle_agent(&parent.id).unwrap());
    assert_eq!(store.agent(&parent.id).unwrap().state, "idle");
}

#[test]
fn subagent_scales_to_idle_when_done_wakes_on_message_and_stays_archived_when_retired() {
    let (_dir, store) = setup();
    let root = agent(&store, "root");
    let mut child_new = NewAgent::new("root-worker", "opencode");
    child_new.parent_id = Some(root.id.clone());
    let child = store.create_agent(child_new).unwrap();
    store.set_agent_running(&child.id, "/work").unwrap();

    // Send initial task message (seq = 1), deliver it, and complete turn 1.
    store.receive(&child.id, Incoming::new("task 1")).unwrap();
    store.note_delivered(&child.id, 1).unwrap();
    store.note_turn_completed(&child.id).unwrap();

    // complete_idle_subagent transitions the sub-agent to `idle` (scaled to zero).
    assert!(store.complete_idle_subagent(&child.id).unwrap());
    assert_eq!(store.agent(&child.id).unwrap().state, "idle");

    // Sending a follow-up message wakes an `idle` sub-agent back to `running`.
    store.receive(&child.id, Incoming::new("task 2")).unwrap();
    assert_eq!(store.agent(&child.id).unwrap().state, "running");

    // Explicit archive transitions the sub-agent to `archived` (retired), which does NOT auto-wake.
    store.set_agent_state(&child.id, "archived").unwrap();
    assert_eq!(store.agent(&child.id).unwrap().state, "archived");
    store.receive(&child.id, Incoming::new("task 3")).unwrap();
    assert_eq!(store.agent(&child.id).unwrap().state, "archived");
}

#[test]
fn no_ingress_wakes_an_archived_agent_and_idle_still_wakes() {
    let (dir, store) = setup();
    let recipient = agent(&store, "recipient");
    store.set_agent_state(&recipient.id, "archived").unwrap();
    // Lifecycle, channel, schedule and authored agent ingress alike leave an
    // archived recipient archived: only an explicit wake revives it.
    for (sender, key, text) in [
        (
            "agent:child",
            "subagent_completed:child:1",
            "[Sub-Agent Completed] done",
        ),
        (
            "agent:child",
            "subagent-error:child:1",
            "[Sub-Agent Error] failed",
        ),
        ("system", "idle:1", "idle event"),
        ("telegram:user:123", "telegram:123:1", "channel update"),
        ("schedule:timer", "schedule:timer:1", "timer prompt"),
        ("schedule:watch", "watch:watch:1", "watch prompt"),
        ("agent:parent", "direct-1", "intentional follow-up"),
    ] {
        let mut incoming = Incoming::new(text);
        incoming.sender = Some(sender.into());
        incoming.source_key = Some(key.into());
        store.receive(&recipient.id, incoming).unwrap();
        assert_eq!(store.agent(&recipient.id).unwrap().state, "archived");
    }
    drop(store);
    let store = Store::open(dir.path().join("state.db")).unwrap();
    assert_eq!(store.agent(&recipient.id).unwrap().state, "archived");
    store.set_agent_state(&recipient.id, "idle").unwrap();
    let mut incoming = Incoming::new("after an explicit wake");
    incoming.sender = Some("agent:parent".into());
    store.receive(&recipient.id, incoming).unwrap();
    assert_eq!(store.agent(&recipient.id).unwrap().state, "running");
    assert_eq!(store.inbox(&recipient.id, None).unwrap().len(), 8);
}
