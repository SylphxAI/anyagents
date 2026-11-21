use super::super::{Timer, create_agent_schedule, evaluate_single_watch_schedule};
use super::*;
use anyagents_core::{NewAgent, NewSchedule};
use anyagents_supervisor::HealthBoard;
use axum::{Json, Router, extract::State, http::StatusCode, routing::post};
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Clone)]
struct MockSandbox {
    updates: Arc<Mutex<Option<&'static str>>>,
    calls: Arc<AtomicUsize>,
    path: String,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sandbox_file_watch_uses_only_the_bound_sandbox_and_preserves_baseline_on_failure() {
    let updates = Arc::new(Mutex::new(Some("missing")));
    let calls = Arc::new(AtomicUsize::new(0));
    let observed_cwd = "/sandbox/workspace";
    // A quoted path containing shell syntax must arrive unchanged as one argument.
    let dir = crate::test_tempdir();
    let host_file = dir.path().join("host file ' $(not-a-command)");
    std::fs::write(&host_file, "host only").unwrap();
    let host_path = host_file.to_str().unwrap().to_owned();
    let router = Router::new()
        .route(
            "/v1/sandboxes/lease-fixture/exec",
            post(
                async move |State(mock): State<MockSandbox>, Json(body): Json<Value>| {
                    mock.calls.fetch_add(1, Ordering::SeqCst);
                    let command = body["command"].as_array().unwrap();
                    assert_eq!(
                        command,
                        &[
                            json!("python3"),
                            json!("-I"),
                            json!("-c"),
                            json!(STAT),
                            json!(mock.path)
                        ]
                    );
                    assert_eq!(body["cwd"], observed_cwd);
                    match *mock.updates.lock().unwrap() {
                        Some(stdout) => (
                            StatusCode::OK,
                            Json(json!({"stdout": stdout, "exit_code": 0})),
                        ),
                        None => (
                            StatusCode::SERVICE_UNAVAILABLE,
                            Json(json!({"error":"unavailable"})),
                        ),
                    }
                },
            ),
        )
        .with_state(MockSandbox {
            updates: updates.clone(),
            calls: calls.clone(),
            path: host_path.clone(),
        });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let peer = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

    let store = Store::open(dir.path().join("state.db")).unwrap();
    let agent = store
        .create_agent(NewAgent::new("remote-watch", "codex").with_runtime(
            ExecutionRuntime::Sandbox {
                provider: Some("http".into()),
            },
        ))
        .unwrap();
    let health = HealthBoard::default();
    let mut binding = anyagents_core::stateless::SandboxBinding::new(agent.id.clone(), "http");
    binding.remote_workdir = observed_cwd.into();
    binding.spec_json = json!({"endpoint": endpoint}).to_string();
    binding.lease_id = Some("lease-fixture".into());
    binding.lease_state = "ready".into();
    let bindings = store.sandbox_binding_store();
    bindings.upsert(&binding).unwrap();

    let spec = format!("file:{host_path}");
    let created = create_agent_schedule(
        &store,
        &health,
        &agent,
        None,
        None,
        Some(&spec),
        "file created",
    )
    .unwrap();
    let load = |id| store.get_schedule(id).unwrap().unwrap();
    assert_eq!(
        load(&created.id).watch_baseline.as_deref(),
        Some("sandbox:missing")
    );
    let legacy = store
        .create_schedule(NewSchedule {
            agent_id: agent.id.clone(),
            kind: "watch".into(),
            spec: spec.clone(),
            prompt: "legacy".into(),
            interval_ms: None,
            next_fire_at_ms: None,
            watch_baseline: Some("exists:1:1".into()),
        })
        .unwrap();
    let now = anyagents_core::now_ms();
    let mut timer = Timer::local_event_driven(true);
    assert!(
        timer
            .prepare_local_watches(&store, 1, now)
            .unwrap()
            .is_empty()
    );
    assert_eq!(timer.step(&store, &health, 1, now).fired, 0);
    for id in [&created.id, &legacy.id] {
        let row = load(id);
        assert_eq!(row.watch_baseline.as_deref(), Some("sandbox:missing"));
        assert_eq!(row.last_observed.as_deref(), Some("missing"));
    }
    bindings.clear_lease(&agent.id).unwrap();
    std::fs::write(&host_file, "the host changed, not the sandbox").unwrap();
    let before = calls.load(Ordering::SeqCst);
    let probe = |id| evaluate_single_watch_schedule(&store, &health, &load(id), now);
    assert_eq!(probe(&created.id), (false, Some(now + PROBE_INTERVAL_MS)));
    assert_eq!(calls.load(Ordering::SeqCst), before);
    assert_eq!(
        load(&created.id).last_observed.as_deref(),
        Some("sandbox_unavailable")
    );
    assert_eq!(
        load(&created.id).watch_baseline.as_deref(),
        Some("sandbox:missing")
    );
    bindings.upsert(&binding).unwrap();
    *updates.lock().unwrap() = None;
    assert!(!probe(&created.id).0);
    assert_eq!(
        load(&created.id).last_observed.as_deref(),
        Some("probe_error:sandbox_stat")
    );
    *updates.lock().unwrap() = Some("exists:10:3");
    assert!(probe(&created.id).0);
    assert_eq!(
        load(&created.id).last_observed.as_deref(),
        Some("missing->exists:10:3")
    );

    let modified = create_agent_schedule(
        &store,
        &health,
        &agent,
        None,
        None,
        Some(&spec),
        "file modified",
    )
    .unwrap();
    assert_eq!(
        load(&modified.id).watch_baseline.as_deref(),
        Some("sandbox:exists:10:3")
    );
    *updates.lock().unwrap() = Some("exists:11:4");
    assert!(probe(&modified.id).0);
    assert_eq!(
        load(&modified.id).last_observed.as_deref(),
        Some("exists:10:3->exists:11:4")
    );
    peer.abort();
}

#[test]
fn local_file_watch_keeps_workdir_resolution_and_host_change_detection() {
    let dir = crate::test_tempdir();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let agent = store
        .create_agent(NewAgent {
            workdir: Some(dir.path().display().to_string()),
            ..NewAgent::new("local-watch", "codex")
        })
        .unwrap();
    let health = HealthBoard::default();
    let created = create_agent_schedule(
        &store,
        &health,
        &agent,
        None,
        None,
        Some("path:signal"),
        "file created",
    )
    .unwrap();
    let row = store.get_schedule(&created.id).unwrap().unwrap();
    let path = dir.path().join("signal");
    assert_eq!(row.spec, format!("file:{}", path.display()));
    assert_eq!(row.watch_baseline.as_deref(), Some("missing"));
    let now = anyagents_core::now_ms();
    let mut timer = Timer::local_event_driven(true);
    assert_eq!(
        timer.prepare_local_watches(&store, 1, now).unwrap().len(),
        1
    );
    std::fs::write(path, "local signal").unwrap();
    assert!(evaluate_single_watch_schedule(&store, &health, &row, now).0);
}
