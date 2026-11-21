use super::super::agents::reset_session_route;
use super::super::agents_lifecycle::{
    delete_agent_route, pause_agent_route, remove_agent_identity_files,
};
use super::super::bootstrap::active_seat_ids;
use super::super::*;
use super::helpers::{archetype_validation_state, env_lock};
use anyagents_core::Agent;
use axum::Json;
use axum::http::StatusCode;

#[test]
fn deletion_retains_identity_and_resources_until_process_stop() {
    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    let agent = state
        .store
        .create_agent(anyagents_core::NewAgent::new("busy", "opencode"))
        .unwrap();
    let config = anyagents_core::agent_config_dir(dir.path(), &agent.id);
    std::fs::create_dir_all(&config).unwrap();
    std::fs::write(config.join("preserve"), "retained state").unwrap();
    let mut live = anyagents_supervisor::LiveState {
        process_alive: true,
        turn_state: anyagents_harness::TurnState::Idle,
        delivery: Some(crate::vendor::declared_delivery(&agent.harness)),
        quota_reset_at_ms: None,
        restart_at_ms: None,
        pid: None,
        live_harness: Some(agent.harness.clone()),
        live_model: None,
        process_started_at_ms: None,
        last_error: None,
        retry_at_ms: None,
    };
    state.health.publish(&agent.id, live.clone());
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let refused = delete_agent_route(
            Principal::Operator,
            State(state.clone()),
            AxumPath(agent.name.clone()),
        )
        .await
        .expect_err("live process must retain identity");
        assert_eq!(refused.status, StatusCode::CONFLICT);
        assert_eq!(state.store.agent(&agent.id).unwrap().state, "archived");
        assert_eq!(
            std::fs::read_to_string(config.join("preserve")).unwrap(),
            "retained state"
        );
        let process = state.store.try_lock_agent(&agent.id).unwrap();
        live.process_alive = false;
        state.health.publish(&agent.id, live);
        let refused = delete_agent_route(
            Principal::Operator,
            State(state.clone()),
            AxumPath(agent.name.clone()),
        )
        .await
        .expect_err("optimistic health must not revoke process custody");
        assert_eq!(refused.status, StatusCode::CONFLICT);
        assert!(state.store.agent(&agent.id).is_ok());
        assert_eq!(
            std::fs::read_to_string(config.join("preserve")).unwrap(),
            "retained state"
        );
        drop(process);
        let _ = delete_agent_route(
            Principal::Operator,
            State(state.clone()),
            AxumPath(agent.name.clone()),
        )
        .await
        .unwrap_or_else(|error| panic!("{}", error.message));
        assert!(state.store.agent(&agent.id).is_err());
        assert!(!config.exists());
    });
}

#[test]
fn deletion_waits_for_process_custody_after_optimistic_stopped_health() {
    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    let agent = state
        .store
        .create_agent(anyagents_core::NewAgent::new("stopping", "opencode"))
        .unwrap();
    state.store.set_agent_running(&agent.id, "/work").unwrap();
    let config = anyagents_core::agent_config_dir(dir.path(), &agent.id);
    std::fs::create_dir_all(&config).unwrap();
    std::fs::write(config.join("preserve"), "retained state").unwrap();
    let process = state.store.try_lock_agent(&agent.id).unwrap();
    // An absent health row is the same optimistic stopped projection used
    // during supervisor shutdown; actual process custody remains occupied.
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let deleting = tokio::spawn(delete_agent_route(
            Principal::Operator,
            State(state.clone()),
            AxumPath(agent.name.clone()),
        ));
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while state.store.agent(&agent.id).unwrap().state != "archived" {
            assert!(std::time::Instant::now() < deadline);
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;
        assert!(
            !deleting.is_finished(),
            "shutdown must wait for actual custody"
        );
        assert_eq!(
            std::fs::read_to_string(config.join("preserve")).unwrap(),
            "retained state"
        );
        drop(process);
        let _ = deleting
            .await
            .unwrap()
            .unwrap_or_else(|error| panic!("{}", error.message));
        assert!(state.store.agent(&agent.id).is_err());
        assert!(!config.exists());
    });
}

#[test]
fn reset_session_requires_management_authority_and_quiescence_before_any_pin_change() {
    use anyagents_core::host_session;
    let dir = crate::test_tempdir();
    let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("reset-parent", "opencode"))
        .unwrap();
    let mut new_child = anyagents_core::NewAgent::new("reset-child", "opencode");
    new_child.parent_id = Some(parent.id.clone());
    let child = store.create_agent(new_child).unwrap();
    let peer = store
        .create_agent(anyagents_core::NewAgent::new("reset-peer", "opencode"))
        .unwrap();
    store.set_agent_state(&child.id, "paused").unwrap();
    let root = dir.path().join("agents").join(child.id.as_str());
    std::fs::create_dir_all(root.join("config")).unwrap();
    std::fs::create_dir_all(root.join("memory")).unwrap();
    std::fs::write(root.join("memory/MEMORY.md"), "before reset").unwrap();
    let old = host_session::ensure_active(&root, "opencode").unwrap();
    std::fs::write(root.join("config/opencode-session-id"), "old-id\n").unwrap();
    std::fs::write(root.join("config/raw.jsonl"), "raw retained").unwrap();
    std::fs::write(root.join("memory/MEMORY.md"), "after reset").unwrap();
    let before = std::fs::read(root.join("sessions/registry.json")).unwrap();
    let pins = || std::fs::read_dir(root.join("sessions")).unwrap().count();
    let before_pins = pins();
    let state = AppState {
        store: Arc::clone(&store),
        health: HealthBoard::default(),
        sign_in: Arc::new(Mutex::new(ProcessRunner)),
        auth: auth::Mode::Trusted {
            data_dir: dir.path().into(),
        },
        identity: Arc::new(ServingIdentity::capture()),
        profiles: Default::default(),
        timer_backend: Arc::new(crate::timer::NoopTimerBackend),
    };
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let principal = |a: &Agent| Principal::Agent {
            agent_id: a.id.clone(),
            name: a.name.clone(),
        };
        let unchanged = || {
            assert_eq!(
                std::fs::read(root.join("sessions/registry.json")).unwrap(),
                before
            );
            assert_eq!(
                std::fs::read(root.join("config/opencode-session-id")).unwrap(),
                b"old-id\n"
            );
            assert_eq!(pins(), before_pins);
        };
        let denied = reset_session_route(
            principal(&peer),
            State(state.clone()),
            AxumPath(child.name.clone()),
        )
        .await
        .unwrap_err();
        assert_eq!(denied.status, StatusCode::FORBIDDEN);
        unchanged();
        let process = store.try_lock_agent(&child.id).unwrap();
        let busy = reset_session_route(
            principal(&parent),
            State(state.clone()),
            AxumPath(child.name.clone()),
        )
        .await
        .unwrap_err();
        assert_eq!(busy.status, StatusCode::CONFLICT);
        unchanged();
        drop(process);
        let lifecycle = store.lock_agent_lifecycle(&child.id).unwrap();
        let busy = reset_session_route(
            principal(&parent),
            State(state.clone()),
            AxumPath(child.name.clone()),
        )
        .await
        .unwrap_err();
        assert_eq!(busy.status, StatusCode::CONFLICT);
        unchanged();
        drop(lifecycle);
        store.set_agent_state(&child.id, "running").unwrap();
        let busy = reset_session_route(
            principal(&parent),
            State(state.clone()),
            AxumPath(child.name.clone()),
        )
        .await
        .unwrap_err();
        assert_eq!(busy.status, StatusCode::CONFLICT);
        unchanged();
        store.set_agent_state(&child.id, "paused").unwrap();
        let Json(result) = reset_session_route(
            principal(&parent),
            State(state),
            AxumPath(child.name.clone()),
        )
        .await
        .unwrap_or_else(|e| panic!("{}", e.message));
        let active = host_session::active(&root).unwrap().unwrap();
        assert_eq!(result["generation"], active.generation);
        assert_ne!(active.generation, old.generation);
        assert!(active.memory.section.contains("after reset"));
        assert_eq!(
            std::fs::read(root.join("config/raw.jsonl")).unwrap(),
            b"raw retained"
        );
        assert_eq!(
            host_session::resume_vendor(&root, "opencode", "old-id").unwrap(),
            old
        );
    });
}

#[test]
fn a_deleted_agents_name_frees_only_after_its_profile_and_config_are_gone() {
    let _env = env_lock();
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path();
    let store = Store::open(data.join("state.db")).unwrap();
    let agent = store
        .create_agent(anyagents_core::NewAgent::new("shopper", "codex"))
        .unwrap();
    let agent_dir = data.join("agents").join(agent.id.as_str());
    let profile = data.join("browser-profiles").join("shopper");
    for path in [
        agent_dir.join("config"),
        agent_dir.join("memory"),
        profile.join("user-data"),
    ] {
        std::fs::create_dir_all(&path).unwrap();
    }
    std::fs::write(profile.join("user-data").join("Cookies"), b"logged-in").unwrap();
    // The delete route removes these before the row; the row still holds
    // the name, so no agent can take it while they exist.
    remove_agent_identity_files(data, &agent, &agent_dir, None);
    assert!(!profile.exists());
    assert!(!agent_dir.join("config").exists());
    assert!(!agent_dir.join("memory").exists());
    assert!(
        store
            .create_agent(anyagents_core::NewAgent::new("shopper", "codex"))
            .is_err()
    );
    // Run again after a stop part way: nothing left, nothing fails.
    remove_agent_identity_files(data, &agent, &agent_dir, None);
    store.delete_agent(&agent.id).unwrap();
    let reused = store
        .create_agent(anyagents_core::NewAgent::new("shopper", "codex"))
        .unwrap();
    assert_ne!(reused.id, agent.id);
    assert!(!profile.join("user-data").join("Cookies").exists());
}

#[test]
fn active_seat_ids_excludes_archived_agents_from_startup_boundary() {
    let data = crate::test_tempdir();
    let store = Store::open(data.path().join("state.db")).unwrap();
    let active = store
        .create_agent(anyagents_core::NewAgent::new("active-root", "codex"))
        .unwrap();
    let archived = store
        .create_agent(anyagents_core::NewAgent::new("archived-root", "codex"))
        .unwrap();
    store.set_agent_state(&archived.id, "archived").unwrap();
    let seats = active_seat_ids(&store).unwrap();
    assert_eq!(seats, vec![active.id.to_string()]);
}

#[tokio::test]
async fn pause_agent_route_waits_on_lifecycle_and_rejects_archived() {
    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    let live = state
        .store
        .create_agent(anyagents_core::NewAgent::new("pause-live", "opencode"))
        .unwrap();
    let Json(paused) = pause_agent_route(
        Principal::Operator,
        State(state.clone()),
        AxumPath(live.name.clone()),
    )
    .await
    .unwrap();
    assert_eq!(paused.state, "paused");

    let archived = state
        .store
        .create_agent(anyagents_core::NewAgent::new("pause-archived", "opencode"))
        .unwrap();
    let held = state.store.lock_agent_lifecycle(&archived.id).unwrap();
    let route_state = state.clone();
    let route_name = archived.name.clone();
    let pending = tokio::spawn(async move {
        pause_agent_route(
            Principal::Operator,
            State(route_state),
            AxumPath(route_name),
        )
        .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(80)).await;
    assert!(
        !pending.is_finished(),
        "pause ran while lifecycle lock was held"
    );
    state
        .store
        .set_agent_state(&archived.id, "archived")
        .unwrap();
    drop(held);
    let err = pending.await.unwrap().unwrap_err();
    assert_eq!(err.status, StatusCode::CONFLICT);
    assert_eq!(
        state.store.agent(&archived.id).unwrap().state,
        "archived",
        "pause overwrote archived state"
    );
}
