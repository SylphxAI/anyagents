//! Local persistent-coordinator [`TimerBackend`] backed by exact per-schedule
//! `tokio::time::sleep_until` tasks with zero 60s periodic polling and zero
//! idle wakeups.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use anyagents_core::Store;
use anyagents_supervisor::HealthBoard;
use tokio::task::AbortHandle;

use super::{TimerBackend, fire_schedule_by_id};

struct LocalTimerInner {
    store: Arc<Store>,
    health: HealthBoard,
    handle: tokio::runtime::Handle,
    tasks: Mutex<HashMap<String, (u64, AbortHandle)>>,
    next_gen: AtomicU64,
    wake_count: AtomicU64,
}

impl Drop for LocalTimerInner {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.tasks.lock() {
            for (_, (_, abort)) in guard.drain() {
                abort.abort();
            }
        }
    }
}

/// Exact per-schedule Tokio timer backend for local / persistent coordinator mode.
///
/// - Each active schedule gets its own `tokio::time::sleep_until` task (sub-second precision).
/// - Zero background polling loops; zero wakes when no schedules are armed.
/// - `cancel(schedule_id)` aborts the pending Tokio task immediately.
/// - Recurring (`every`) schedules automatically re-arm their next `next_fire_at_ms`;
///   one-shot (`at`) schedules complete without re-arming.
#[derive(Clone)]
pub struct LocalTokioTimerBackend {
    inner: Arc<LocalTimerInner>,
}

impl LocalTokioTimerBackend {
    pub fn new(store: Arc<Store>, health: HealthBoard, handle: tokio::runtime::Handle) -> Self {
        Self {
            inner: Arc::new(LocalTimerInner {
                store,
                health,
                handle,
                tasks: Mutex::new(HashMap::new()),
                next_gen: AtomicU64::new(1),
                wake_count: AtomicU64::new(0),
            }),
        }
    }

    /// Rehydrate all active `at` / `every` schedules from `Store` on startup,
    /// arming one exact timer task per schedule (or firing immediately if overdue).
    pub fn rehydrate_from_store(&self) -> Result<usize, String> {
        let active = self
            .inner
            .store
            .list_schedules(None, true)
            .map_err(|err| format!("failed to list active schedules for rehydration: {err}"))?;
        let mut armed = 0usize;
        for sched in active {
            if !matches!(sched.kind.as_str(), "at" | "every") {
                continue;
            }
            if let Some(fire_at_ms) = sched.next_fire_at_ms {
                self.schedule_one_shot(&sched.id, fire_at_ms)?;
                armed += 1;
            }
        }
        Ok(armed)
    }

    /// Number of currently armed in-memory Tokio timer tasks.
    #[cfg(test)]
    pub fn pending_count(&self) -> usize {
        self.inner
            .tasks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len()
    }

    /// Total number of timer wakeups executed by this backend.
    #[cfg(test)]
    pub fn wake_count(&self) -> u64 {
        self.inner.wake_count.load(Ordering::SeqCst)
    }

    /// Abort all pending local timer tasks (used on graceful shutdown).
    #[allow(dead_code)]
    pub fn cancel_all(&self) {
        let mut guard = self.inner.tasks.lock().unwrap_or_else(|e| e.into_inner());
        for (_, (_, abort)) in guard.drain() {
            abort.abort();
        }
    }
}

impl TimerBackend for LocalTokioTimerBackend {
    fn schedule_one_shot(
        &self,
        schedule_id: &str,
        fire_at_ms: i64,
    ) -> Result<Option<String>, String> {
        let id = schedule_id.trim().to_string();
        if id.is_empty() {
            return Err("schedule_id cannot be empty".to_string());
        }
        let generation = self.inner.next_gen.fetch_add(1, Ordering::SeqCst);
        let weak: Weak<LocalTimerInner> = Arc::downgrade(&self.inner);
        let task_id = id.clone();

        let join = self.inner.handle.spawn(async move {
            loop {
                let now = anyagents_core::now_ms();
                if now >= fire_at_ms {
                    break;
                }
                let remaining_ms = (fire_at_ms - now) as u64;
                let deadline = tokio::time::Instant::now() + Duration::from_millis(remaining_ms);
                tokio::time::sleep_until(deadline).await;
            }

            let Some(inner) = weak.upgrade() else {
                return;
            };
            // Remove our own handle before firing so a recurring schedule's
            // re-arm call never aborts the currently executing task.
            {
                let mut guard = inner.tasks.lock().unwrap_or_else(|e| e.into_inner());
                if guard.get(&task_id).is_some_and(|(g, _)| *g == generation) {
                    guard.remove(&task_id);
                }
            }
            inner.wake_count.fetch_add(1, Ordering::SeqCst);

            let backend = LocalTokioTimerBackend {
                inner: Arc::clone(&inner),
            };
            let store = Arc::clone(&inner.store);
            let health = inner.health.clone();
            let sid = task_id.clone();
            let _ = tokio::task::spawn_blocking(move || {
                if let Err(err) = fire_schedule_by_id(
                    &store,
                    &health,
                    &backend,
                    &sid,
                    Some(fire_at_ms),
                    anyagents_core::now_ms(),
                ) {
                    eprintln!("anyagents-timer: local fire failed for {sid}: {err}");
                }
            })
            .await;
        });

        let abort_handle = join.abort_handle();
        let mut guard = self.inner.tasks.lock().unwrap_or_else(|e| e.into_inner());
        if let Some((_, previous)) = guard.insert(id, (generation, abort_handle)) {
            previous.abort();
        }
        Ok(None)
    }

    fn cancel(&self, schedule_id: &str, _external_run_id: Option<&str>) -> Result<(), String> {
        let mut guard = self.inner.tasks.lock().unwrap_or_else(|e| e.into_inner());
        if let Some((_, abort)) = guard.remove(schedule_id.trim()) {
            abort.abort();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyagents_core::{NewAgent, NewSchedule};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_tokio_timer_sub_60s_one_shot_fires_precisely_without_rearm() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let health = HealthBoard::default();
        let agent = store
            .create_agent(NewAgent::new("timer-oneshot", "codex"))
            .unwrap();
        store.set_agent_running(&agent.id, "/work").unwrap();

        let fire_at = anyagents_core::now_ms() + 120;
        let sched = store
            .create_schedule(NewSchedule {
                agent_id: agent.id.clone(),
                kind: "at".into(),
                spec: "120ms".into(),
                prompt: "sub-60s one-shot wake".into(),
                interval_ms: None,
                next_fire_at_ms: Some(fire_at),
                watch_baseline: None,
            })
            .unwrap();

        let backend = LocalTokioTimerBackend::new(
            Arc::clone(&store),
            health,
            tokio::runtime::Handle::current(),
        );
        let started = std::time::Instant::now();
        backend.schedule_one_shot(&sched.id, fire_at).unwrap();
        assert_eq!(backend.pending_count(), 1);

        // Wait up to 1s for the 120ms timer to fire.
        while store.inbox(&agent.id, None).unwrap().is_empty() {
            assert!(
                started.elapsed() < Duration::from_secs(1),
                "one-shot timer did not fire within 1s"
            );
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
        assert!(started.elapsed() >= Duration::from_millis(90));
        assert_eq!(store.inbox(&agent.id, None).unwrap().len(), 1);
        assert_eq!(
            store.get_schedule(&sched.id).unwrap().unwrap().state,
            "fired"
        );
        // One-shot must NOT re-arm.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(backend.pending_count(), 0);
        assert_eq!(backend.wake_count(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_tokio_timer_recurring_rearms_automatically() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let health = HealthBoard::default();
        let agent = store
            .create_agent(NewAgent::new("timer-recurring", "codex"))
            .unwrap();
        store.set_agent_running(&agent.id, "/work").unwrap();

        let first_fire = anyagents_core::now_ms() + 80;
        let sched = store
            .create_schedule(NewSchedule {
                agent_id: agent.id.clone(),
                kind: "every".into(),
                spec: "100ms".into(),
                prompt: "recurring pulse".into(),
                interval_ms: Some(100),
                next_fire_at_ms: Some(first_fire),
                watch_baseline: None,
            })
            .unwrap();

        let backend = LocalTokioTimerBackend::new(
            Arc::clone(&store),
            health,
            tokio::runtime::Handle::current(),
        );
        let armed = backend.rehydrate_from_store().unwrap();
        assert_eq!(armed, 1);

        // Wait for first firing.
        let started = std::time::Instant::now();
        loop {
            let msgs = store.inbox(&agent.id, None).unwrap();
            if !msgs.is_empty() {
                store.mark_delivered_up_to(&agent.id, msgs[0].seq).unwrap();
                break;
            }
            assert!(started.elapsed() < Duration::from_secs(2));
            tokio::time::sleep(Duration::from_millis(15)).await;
        }

        // Wait for automatic re-arm and second firing.
        loop {
            let msgs = store.inbox(&agent.id, None).unwrap();
            if msgs.len() >= 2 {
                break;
            }
            assert!(
                started.elapsed() < Duration::from_secs(2),
                "recurring timer did not re-arm and fire a second time"
            );
            tokio::time::sleep(Duration::from_millis(15)).await;
        }

        assert_eq!(
            store.get_schedule(&sched.id).unwrap().unwrap().state,
            "active"
        );
        assert!(backend.wake_count() >= 2);
        backend.cancel(&sched.id, None).unwrap();
        assert_eq!(backend.pending_count(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_tokio_timer_cancel_prevents_firing_and_idle_has_zero_wakes() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let health = HealthBoard::default();
        let agent = store
            .create_agent(NewAgent::new("timer-cancel", "codex"))
            .unwrap();
        store.set_agent_running(&agent.id, "/work").unwrap();

        let backend = LocalTokioTimerBackend::new(
            Arc::clone(&store),
            health,
            tokio::runtime::Handle::current(),
        );
        // Zero schedules -> rehydrate arms 0 and triggers 0 wakes when idle.
        assert_eq!(backend.rehydrate_from_store().unwrap(), 0);
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(backend.pending_count(), 0);
        assert_eq!(backend.wake_count(), 0);

        let fire_at = anyagents_core::now_ms() + 150;
        let sched = store
            .create_schedule(NewSchedule {
                agent_id: agent.id.clone(),
                kind: "at".into(),
                spec: "150ms".into(),
                prompt: "should never fire".into(),
                interval_ms: None,
                next_fire_at_ms: Some(fire_at),
                watch_baseline: None,
            })
            .unwrap();
        backend.schedule_one_shot(&sched.id, fire_at).unwrap();
        assert_eq!(backend.pending_count(), 1);

        // Cancel immediately before deadline.
        store.cancel_schedule(Some(&agent.id), &sched.id).unwrap();
        backend.cancel(&sched.id, None).unwrap();
        assert_eq!(backend.pending_count(), 0);

        tokio::time::sleep(Duration::from_millis(250)).await;
        assert!(store.inbox(&agent.id, None).unwrap().is_empty());
        assert_eq!(backend.wake_count(), 0);
    }
}
