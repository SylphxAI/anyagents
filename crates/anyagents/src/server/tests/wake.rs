use super::super::agents_lifecycle::wake_agent_route;
use super::super::*;
use super::helpers::{archetype_validation_state, env_lock};
use anyagents_core::now_ms;
use axum::Json;
use axum::http::StatusCode;

struct RouteWakeHarness {
    generation: usize,
    alive: bool,
    in_turn: bool,
    diagnostic: Option<String>,
    allow_verified_stop: Arc<std::sync::atomic::AtomicBool>,
    stop_entered_tx: Option<std::sync::mpsc::Sender<()>>,
    stop_release_rx: Option<Arc<Mutex<std::sync::mpsc::Receiver<()>>>>,
    delivered: Arc<Mutex<Vec<(usize, String)>>>,
    active_processes: Arc<std::sync::atomic::AtomicUsize>,
}

impl Drop for RouteWakeHarness {
    fn drop(&mut self) {
        if self.alive {
            self.active_processes
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            self.alive = false;
        }
    }
}

impl anyagents_harness::Harness for RouteWakeHarness {
    fn deliver(
        &mut self,
        message: &anyagents_harness::OutboundMessage,
    ) -> Result<anyagents_harness::DeliverOutcome, anyagents_harness::HarnessError> {
        self.delivered
            .lock()
            .unwrap()
            .push((self.generation, message.text.clone()));
        self.in_turn = false;
        Ok(anyagents_harness::DeliverOutcome::Accepted {
            turn_finished: true,
        })
    }

    fn proven(&self, _message_id: &str) -> Option<anyagents_harness::Proof> {
        Some(anyagents_harness::Proof::Taken {
            turn_finished: true,
        })
    }

    fn stop_verified(&mut self) -> Result<(), anyagents_harness::HarnessError> {
        if let Some(tx) = self.stop_entered_tx.take() {
            let _ = tx.send(());
        }
        if let Some(rx) = &self.stop_release_rx {
            let _ = rx
                .lock()
                .unwrap()
                .recv_timeout(std::time::Duration::from_secs(5));
        }
        if !self
            .allow_verified_stop
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(anyagents_harness::HarnessError::Failed(
                "checked harness retirement is unsupported".into(),
            ));
        }
        if self.alive {
            self.active_processes
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            self.alive = false;
        }
        self.in_turn = false;
        Ok(())
    }

    fn background(&mut self) -> Result<(), anyagents_harness::HarnessError> {
        Ok(())
    }
    fn resume(&mut self, _session: &str) -> Result<(), anyagents_harness::HarnessError> {
        Ok(())
    }
    fn cancel(&mut self) -> Result<(), anyagents_harness::HarnessError> {
        Ok(())
    }
    fn exit_diagnostic(&self) -> Option<String> {
        self.diagnostic.clone()
    }
    fn health(&self) -> anyagents_harness::HarnessHealth {
        anyagents_harness::HarnessHealth {
            process_alive: self.alive,
            turn_state: if self.in_turn {
                anyagents_harness::TurnState::InTurn
            } else {
                anyagents_harness::TurnState::Idle
            },
            delivery: anyagents_harness::Delivery {
                level: anyagents_harness::DeliveryLevel::NextStep,
                idempotent: true,
                proof: "test".into(),
                peer: None,
            },
            quota_reset_at_ms: None,
            sign_in_fault: None,
        }
    }
}

struct FakeModeGuard(Option<std::ffi::OsString>);
impl FakeModeGuard {
    fn enter() -> Self {
        let prev = std::env::var_os("ANYAGENTS_FAKE_MODE");
        unsafe {
            std::env::set_var("ANYAGENTS_FAKE_MODE", "1");
        }
        Self(prev)
    }
}
impl Drop for FakeModeGuard {
    fn drop(&mut self) {
        unsafe {
            match self.0.take() {
                Some(v) => std::env::set_var("ANYAGENTS_FAKE_MODE", v),
                None => std::env::remove_var("ANYAGENTS_FAKE_MODE"),
            }
        }
    }
}

#[test]
fn wake_route_recovers_delivery_error_in_one_step_and_keeps_repeat_wake_idempotent() {
    let _env = env_lock();
    let _fake = FakeModeGuard::enter();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        let dir = crate::test_tempdir();
        let mut state = archetype_validation_state(dir.path());
        let launches = Arc::new(AtomicUsize::new(0));
        let active_processes = Arc::new(AtomicUsize::new(0));
        let max_concurrent = Arc::new(AtomicUsize::new(0));
        let delivered = Arc::new(Mutex::new(Vec::<(usize, String)>::new()));

        let launches_l = Arc::clone(&launches);
        let active_l = Arc::clone(&active_processes);
        let max_l = Arc::clone(&max_concurrent);
        let delivered_l = Arc::clone(&delivered);

        let sup = anyagents_supervisor::Supervisor::start(
            Arc::clone(&state.store),
            Arc::new(anyagents_supervisor::SystemClock::new()),
            Arc::new(move |_| {
                let gen_no = launches_l.fetch_add(1, Ordering::SeqCst) + 1;
                let cur = active_l.fetch_add(1, Ordering::SeqCst) + 1;
                max_l.fetch_max(cur, Ordering::SeqCst);
                Ok(RouteWakeHarness {
                    generation: gen_no,
                    alive: true,
                    in_turn: false,
                    // Generation 1 surfaces a terminal delivery_error; generation 2+ is healthy.
                    diagnostic: (gen_no == 1).then(|| "broken transport pipe".into()),
                    allow_verified_stop: Arc::new(AtomicBool::new(true)),
                    stop_entered_tx: None,
                    stop_release_rx: None,
                    delivered: Arc::clone(&delivered_l),
                    active_processes: Arc::clone(&active_l),
                })
            }),
            Vec::new(),
        )
        .unwrap();
        state.health = sup.board();

        let created = state
            .store
            .create_agent(anyagents_core::NewAgent::new("wake-target", "claude-code"))
            .unwrap();
        let cfg = anyagents_core::agent_config_dir(dir.path(), &created.id);
        std::fs::create_dir_all(&cfg).unwrap();
        std::fs::write(
            cfg.join(".anyagents-last-error.json"),
            r#"{"error":"broken transport pipe","timestamp_ms":9999999999999}"#,
        )
        .unwrap();
        let work = dir.path().join("work");
        std::fs::create_dir_all(&work).unwrap();
        let running = state
            .store
            .set_agent_running(&created.id, &work.display().to_string())
            .unwrap();

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while sup.live(&running.id).and_then(|l| l.last_error).as_deref()
            != Some("broken transport pipe")
        {
            assert!(
                std::time::Instant::now() < deadline,
                "fault never published"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let before = agent_view(&state.store, &state.health, &running).unwrap();
        assert_eq!(before.block_reason.as_deref(), Some("delivery_error"));

        let Json(woken) = wake_agent_route(
            Principal::Operator,
            State(state.clone()),
            AxumPath(running.name.clone()),
            axum::http::HeaderMap::new(),
        )
        .await
        .unwrap();
        assert_eq!(woken.state, "running");
        assert_eq!(woken.block_reason, None);
        assert_eq!(woken.last_error, None);
        assert!(!cfg.join(".anyagents-last-error.json").exists());

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while launches.load(Ordering::SeqCst) < 2 {
            assert!(
                std::time::Instant::now() < deadline,
                "replacement generation never launched"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        // Repeat wake while the healthy replacement worker is running must be
        // idempotent and must not rotate markers or spawn a third generation.
        let Json(repeat) = wake_agent_route(
            Principal::Operator,
            State(state.clone()),
            AxumPath(running.name.clone()),
            axum::http::HeaderMap::new(),
        )
        .await
        .unwrap();
        assert_eq!(repeat.state, "running");
        assert_eq!(repeat.block_reason, None);
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;
        assert_eq!(
            launches.load(Ordering::SeqCst),
            2,
            "repeat wake restarted healthy replacement worker"
        );

        state
            .store
            .receive(
                &running.id,
                anyagents_core::Incoming::new("post-wake message"),
            )
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !delivered
            .lock()
            .unwrap()
            .iter()
            .any(|(_, text)| text.contains("post-wake message"))
        {
            assert!(
                std::time::Instant::now() < deadline,
                "replacement generation never delivered post-wake mail"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(launches.load(Ordering::SeqCst), 2);
        assert_eq!(
            max_concurrent.load(Ordering::SeqCst),
            1,
            "old generation must retire before replacement launches"
        );
        assert_eq!(delivered.lock().unwrap()[0].0, 2);
        drop(sup);
    });
}

#[test]
fn wake_route_refuses_unproven_termination_and_cancellation_without_clearing_faults() {
    let _env = env_lock();
    let _fake = FakeModeGuard::enter();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        let dir = crate::test_tempdir();
        let mut state = archetype_validation_state(dir.path());
        let launches = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let allow_stop = Arc::new(AtomicBool::new(false));
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let release_rx = Arc::new(Mutex::new(release_rx));
        let entered_slot = Arc::new(Mutex::new(Some(entered_tx)));

        let launches_l = Arc::clone(&launches);
        let active_l = Arc::clone(&active);
        let allow_l = Arc::clone(&allow_stop);
        let entered_l = Arc::clone(&entered_slot);
        let release_l = Arc::clone(&release_rx);

        let sup = anyagents_supervisor::Supervisor::start(
            Arc::clone(&state.store),
            Arc::new(anyagents_supervisor::SystemClock::new()),
            Arc::new(move |_| {
                let gen_no = launches_l.fetch_add(1, Ordering::SeqCst) + 1;
                active_l.fetch_add(1, Ordering::SeqCst);
                Ok(RouteWakeHarness {
                    generation: gen_no,
                    alive: true,
                    in_turn: false,
                    diagnostic: (gen_no == 1).then(|| "remote keeper stop unproven".into()),
                    allow_verified_stop: Arc::clone(&allow_l),
                    stop_entered_tx: entered_l.lock().unwrap().take(),
                    stop_release_rx: (gen_no == 1).then(|| Arc::clone(&release_l)),
                    delivered: Arc::new(Mutex::new(Vec::new())),
                    active_processes: Arc::clone(&active_l),
                })
            }),
            Vec::new(),
        )
        .unwrap();
        state.health = sup.board();

        let created = state
            .store
            .create_agent(anyagents_core::NewAgent::new(
                "unproven-agent",
                "claude-code",
            ))
            .unwrap();
        let cfg = anyagents_core::agent_config_dir(dir.path(), &created.id);
        std::fs::create_dir_all(&cfg).unwrap();
        std::fs::write(
            cfg.join(".anyagents-last-error.json"),
            r#"{"error":"remote keeper stop unproven","timestamp_ms":9999999999999}"#,
        )
        .unwrap();
        let work = dir.path().join("work");
        std::fs::create_dir_all(&work).unwrap();
        let running = state
            .store
            .set_agent_running(&created.id, &work.display().to_string())
            .unwrap();

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while sup.live(&running.id).and_then(|l| l.last_error).as_deref()
            != Some("remote keeper stop unproven")
        {
            assert!(
                std::time::Instant::now() < deadline,
                "fault never published"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        // 1. Cancellation while stop_for_wake is waiting must fail closed before
        // clearing any fault or starting a replacement.
        let cancel_state = state.clone();
        let cancel_name = running.name.clone();
        let wake_fut = wake_agent_route(
            Principal::Operator,
            State(cancel_state),
            AxumPath(cancel_name),
            axum::http::HeaderMap::new(),
        );
        let timed = tokio::time::timeout(std::time::Duration::from_millis(60), wake_fut).await;
        assert!(
            timed.is_err(),
            "wake should still be waiting on gated stop_verified"
        );

        entered_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        release_tx.send(()).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;

        assert_eq!(state.store.agent(&running.id).unwrap().state, "paused");
        assert!(cfg.join(".anyagents-last-error.json").exists());
        assert_eq!(
            sup.live(&running.id).and_then(|l| l.last_error).as_deref(),
            Some("remote keeper stop unproven")
        );
        assert!(matches!(
            state.store.try_lock_agent(&running.id),
            Err(anyagents_core::Error::AgentBusy)
        ));
        assert_eq!(launches.load(Ordering::SeqCst), 1);

        // 2. Subsequent wake while stop_verified still returns Err refuses with
        // 409 Conflict and preserves both fault and quarantined custody.
        let _ = release_tx.send(());
        let err = wake_agent_route(
            Principal::Operator,
            State(state.clone()),
            AxumPath(running.name.clone()),
            axum::http::HeaderMap::new(),
        )
        .await
        .unwrap_err();
        assert_eq!(err.status, StatusCode::CONFLICT);
        assert!(err.message.contains("unsupported"), "{}", err.message);
        assert!(cfg.join(".anyagents-last-error.json").exists());
        assert!(matches!(
            state.store.try_lock_agent(&running.id),
            Err(anyagents_core::Error::AgentBusy)
        ));
        assert_eq!(launches.load(Ordering::SeqCst), 1);
        drop(sup);
    });
}

#[test]
fn wake_route_preserves_unrelated_auth_quota_and_migration_faults() {
    let _env = env_lock();
    let _fake = FakeModeGuard::enter();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let dir = crate::test_tempdir();
        let state = archetype_validation_state(dir.path());
        for (idx, (fault_text, quota_ms, expected_reason)) in [
            ("not logged in to provider", None, "sign_in_required"),
            (
                "rate_limit quota exceeded",
                Some(now_ms() + 600_000),
                "quota_exhausted",
            ),
            (
                "memory_migration_wait: indexing host memory",
                None,
                "memory_migration_wait",
            ),
        ]
        .into_iter()
        .enumerate()
        {
            let msg = fault_text;
            let agent = state
                .store
                .create_agent(anyagents_core::NewAgent::new(
                    format!("unrelated-{idx}"),
                    "claude-code",
                ))
                .unwrap();
            let cfg = anyagents_core::agent_config_dir(dir.path(), &agent.id);
            std::fs::create_dir_all(&cfg).unwrap();
            std::fs::write(
                cfg.join(".anyagents-last-error.json"),
                serde_json::json!({
                    "error": msg,
                    "timestamp_ms": now_ms() + 1_000,
                })
                .to_string(),
            )
            .unwrap();
            let work = dir.path().join(format!("work-{idx}"));
            std::fs::create_dir_all(&work).unwrap();
            state.store.set_agent_state(&agent.id, "paused").unwrap();
            state.health.publish(
                &agent.id,
                anyagents_supervisor::LiveState {
                    process_alive: true,
                    turn_state: anyagents_harness::TurnState::Idle,
                    delivery: None,
                    quota_reset_at_ms: quota_ms,
                    restart_at_ms: None,
                    pid: Some(1000 + idx as u32),
                    live_harness: Some("claude-code".into()),
                    live_model: None,
                    process_started_at_ms: Some(now_ms()),
                    last_error: Some(msg.to_string()),
                    retry_at_ms: None,
                },
            );

            let Json(woken) = wake_agent_route(
                Principal::Operator,
                State(state.clone()),
                AxumPath(agent.name.clone()),
                axum::http::HeaderMap::new(),
            )
            .await
            .unwrap();
            assert_eq!(woken.state, "running");
            assert_eq!(
                woken.block_reason.as_deref(),
                Some(expected_reason),
                "unrelated fault cleared on paused->running wake for {expected_reason}"
            );
            assert!(cfg.join(".anyagents-last-error.json").exists());
            assert!(!cfg.join(".anyagents-quota-cleared-ms").exists());

            let Json(woken_again) = wake_agent_route(
                Principal::Operator,
                State(state.clone()),
                AxumPath(agent.name.clone()),
                axum::http::HeaderMap::new(),
            )
            .await
            .unwrap();
            assert_eq!(
                woken_again.block_reason.as_deref(),
                Some(expected_reason),
                "unrelated fault cleared on running wake for {expected_reason}"
            );
            assert!(cfg.join(".anyagents-last-error.json").exists());
        }
    });
}

/// A worker whose process died keeps the agent's process lock through its
/// restart backoff while the board shows `process: down`. A wake for the
/// persisted fault retires that generation and restarts in one step,
/// never beside it.
#[test]
fn wake_route_reclaims_the_lock_a_dead_process_worker_holds() {
    let _env = env_lock();
    let _fake = FakeModeGuard::enter();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        let dir = crate::test_tempdir();
        let mut state = archetype_validation_state(dir.path());
        let launches = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let max_concurrent = Arc::new(AtomicUsize::new(0));
        let delivered = Arc::new(Mutex::new(Vec::<(usize, String)>::new()));

        let (launches_l, active_l, max_l, delivered_l) = (
            Arc::clone(&launches),
            Arc::clone(&active),
            Arc::clone(&max_concurrent),
            Arc::clone(&delivered),
        );
        let sup = anyagents_supervisor::Supervisor::start(
            Arc::clone(&state.store),
            Arc::new(anyagents_supervisor::SystemClock::new()),
            Arc::new(move |request: anyagents_supervisor::LaunchRequest| {
                let gen_no = launches_l.fetch_add(1, Ordering::SeqCst) + 1;
                // Until the wake, every process the worker starts is
                // already gone: it waits out its backoff holding the lock.
                // The wake writes this marker only after it retired that
                // worker, so every later launch is healthy.
                let alive = request
                    .config_dir
                    .join(".anyagents-profile-rotated-ms")
                    .exists();
                if alive {
                    let cur = active_l.fetch_add(1, Ordering::SeqCst) + 1;
                    max_l.fetch_max(cur, Ordering::SeqCst);
                }
                Ok(RouteWakeHarness {
                    generation: gen_no,
                    alive,
                    in_turn: false,
                    diagnostic: None,
                    allow_verified_stop: Arc::new(AtomicBool::new(true)),
                    stop_entered_tx: None,
                    stop_release_rx: None,
                    delivered: Arc::clone(&delivered_l),
                    active_processes: Arc::clone(&active_l),
                })
            }),
            Vec::new(),
        )
        .unwrap();
        state.health = sup.board();

        let created = state
            .store
            .create_agent(anyagents_core::NewAgent::new("dead-worker", "claude-code"))
            .unwrap();
        let cfg = anyagents_core::agent_config_dir(dir.path(), &created.id);
        std::fs::create_dir_all(&cfg).unwrap();
        std::fs::write(
            cfg.join(".anyagents-last-error.json"),
            r#"{"error":"agy_empty_turn: turn ended without model output","timestamp_ms":9999999999999}"#,
        )
        .unwrap();
        let work = dir.path().join("work");
        std::fs::create_dir_all(&work).unwrap();
        let running = state
            .store
            .set_agent_running(&created.id, &work.display().to_string())
            .unwrap();

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let down = sup
                .live(&running.id)
                .is_some_and(|l| !l.process_alive && l.last_error.is_none());
            let held = matches!(
                state.store.try_lock_agent(&running.id),
                Err(anyagents_core::Error::AgentBusy)
            );
            if down && held {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "worker never held the lock over a dead process"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let before = agent_view(&state.store, &state.health, &running).unwrap();
        assert_eq!(before.state, "running");
        assert_eq!(before.process, "down");
        assert_eq!(before.turn_state, "errored");
        assert_eq!(before.block_reason.as_deref(), Some("delivery_error"));
        let dead_generations = launches.load(Ordering::SeqCst);

        let Json(woken) = wake_agent_route(
            Principal::Operator,
            State(state.clone()),
            AxumPath(running.name.clone()),
            axum::http::HeaderMap::new(),
        )
        .await
        .unwrap();
        assert_eq!(woken.state, "running");
        assert_eq!(woken.block_reason, None);
        assert_eq!(woken.last_error, None);
        assert!(!cfg.join(".anyagents-last-error.json").exists());
        assert!(cfg.join(".anyagents-profile-rotated-ms").exists());

        state
            .store
            .receive(&running.id, anyagents_core::Incoming::new("post-wake message"))
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !delivered
            .lock()
            .unwrap()
            .iter()
            .any(|(_, text)| text.contains("post-wake message"))
        {
            assert!(
                std::time::Instant::now() < deadline,
                "replacement generation never delivered post-wake mail: launches={} \
                 (dead before wake: {dead_generations}), state={}, live={:?}",
                launches.load(Ordering::SeqCst),
                state.store.agent(&running.id).unwrap().state,
                sup.live(&running.id),
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(delivered.lock().unwrap()[0].0 > dead_generations);
        assert_eq!(max_concurrent.load(Ordering::SeqCst), 1);
        drop(sup);
    });
}

/// A wake never takes the lock from a process that is alive, or from a
/// holder that is not this server's worker: it refuses and keeps the fault.
#[test]
fn wake_route_refuses_a_lock_held_by_a_live_process_or_another_holder() {
    let _env = env_lock();
    let _fake = FakeModeGuard::enter();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        let dir = crate::test_tempdir();
        let fault = r#"{"error":"broken transport pipe","timestamp_ms":9999999999999}"#;

        // A foreign holder: no worker of this server owns the lock.
        let state = archetype_validation_state(dir.path());
        let created = state
            .store
            .create_agent(anyagents_core::NewAgent::new("foreign-lock", "claude-code"))
            .unwrap();
        let cfg = anyagents_core::agent_config_dir(dir.path(), &created.id);
        std::fs::create_dir_all(&cfg).unwrap();
        std::fs::write(cfg.join(".anyagents-last-error.json"), fault).unwrap();
        let work = dir.path().join("work-foreign");
        std::fs::create_dir_all(&work).unwrap();
        let running = state
            .store
            .set_agent_running(&created.id, &work.display().to_string())
            .unwrap();
        let held = state.store.try_lock_agent(&running.id).unwrap();
        let err = wake_agent_route(
            Principal::Operator,
            State(state.clone()),
            AxumPath(running.name.clone()),
            axum::http::HeaderMap::new(),
        )
        .await
        .unwrap_err();
        assert_eq!(err.status, StatusCode::CONFLICT);
        assert!(
            err.message.contains("lock is still held"),
            "{}",
            err.message
        );
        assert!(cfg.join(".anyagents-last-error.json").exists());
        assert_eq!(state.store.agent(&running.id).unwrap().state, "running");
        drop(held);

        let dir = crate::test_tempdir();
        // A live process: its worker holds the lock and it is never stopped
        // for a persisted fault.
        let mut state = archetype_validation_state(dir.path());
        let launches = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let (launches_l, active_l) = (Arc::clone(&launches), Arc::clone(&active));
        let sup = anyagents_supervisor::Supervisor::start(
            Arc::clone(&state.store),
            Arc::new(anyagents_supervisor::SystemClock::new()),
            Arc::new(move |_| {
                let gen_no = launches_l.fetch_add(1, Ordering::SeqCst) + 1;
                active_l.fetch_add(1, Ordering::SeqCst);
                Ok(RouteWakeHarness {
                    generation: gen_no,
                    alive: true,
                    in_turn: false,
                    diagnostic: None,
                    allow_verified_stop: Arc::new(AtomicBool::new(true)),
                    stop_entered_tx: None,
                    stop_release_rx: None,
                    delivered: Arc::new(Mutex::new(Vec::new())),
                    active_processes: Arc::clone(&active_l),
                })
            }),
            Vec::new(),
        )
        .unwrap();
        state.health = sup.board();
        let created = state
            .store
            .create_agent(anyagents_core::NewAgent::new("live-lock", "claude-code"))
            .unwrap();
        let cfg = anyagents_core::agent_config_dir(dir.path(), &created.id);
        std::fs::create_dir_all(&cfg).unwrap();
        std::fs::write(cfg.join(".anyagents-last-error.json"), fault).unwrap();
        let work = dir.path().join("work-live");
        std::fs::create_dir_all(&work).unwrap();
        let running = state
            .store
            .set_agent_running(&created.id, &work.display().to_string())
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !sup.live(&running.id).is_some_and(|l| l.process_alive) {
            assert!(
                std::time::Instant::now() < deadline,
                "process never came up"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let err = wake_agent_route(
            Principal::Operator,
            State(state.clone()),
            AxumPath(running.name.clone()),
            axum::http::HeaderMap::new(),
        )
        .await
        .unwrap_err();
        assert_eq!(err.status, StatusCode::CONFLICT);
        assert!(
            err.message.contains("lock is still held"),
            "{}",
            err.message
        );
        assert!(cfg.join(".anyagents-last-error.json").exists());
        assert_eq!(state.store.agent(&running.id).unwrap().state, "running");
        assert_eq!(launches.load(Ordering::SeqCst), 1);
        assert_eq!(active.load(Ordering::SeqCst), 1);
        assert!(sup.live(&running.id).is_some_and(|l| l.process_alive));
        drop(sup);
    });
}
