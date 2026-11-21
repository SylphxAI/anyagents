//! Local persistent-coordinator [`TimerBackend`] backed by exact per-schedule
//! `tokio::time::sleep_until` tasks with zero 60s periodic polling and zero
//! idle wakeups.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use anyagents_core::Store;
use anyagents_supervisor::HealthBoard;
use tokio::task::{AbortHandle, JoinHandle};

use super::{TimerBackend, fire_schedule_by_id};

struct LocalTimerInner {
    store: Arc<Store>,
    health: HealthBoard,
    handle: tokio::runtime::Handle,
    tasks: Mutex<HashMap<String, (u64, AbortHandle)>>,
    next_gen: AtomicU64,
    wake_count: AtomicU64,
    now_ms: Box<dyn Fn() -> i64 + Send + Sync>,
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
        Self::with_clock(store, health, handle, anyagents_core::now_ms)
    }

    fn with_clock(
        store: Arc<Store>,
        health: HealthBoard,
        handle: tokio::runtime::Handle,
        now_ms: impl Fn() -> i64 + Send + Sync + 'static,
    ) -> Self {
        Self {
            inner: Arc::new(LocalTimerInner {
                store,
                health,
                handle,
                tasks: Mutex::new(HashMap::new()),
                next_gen: AtomicU64::new(1),
                wake_count: AtomicU64::new(0),
                now_ms: Box::new(now_ms),
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

    fn arm(&self, schedule_id: &str, fire_at_ms: i64) -> Result<JoinHandle<()>, String> {
        let id = schedule_id.trim().to_string();
        if id.is_empty() {
            return Err("schedule_id cannot be empty".to_string());
        }
        // An already-due task can run on another runtime worker immediately.
        // Hold the registration lock until its handle is published.
        let mut guard = self.inner.tasks.lock().unwrap_or_else(|e| e.into_inner());
        let generation = self.inner.next_gen.fetch_add(1, Ordering::Relaxed);
        let weak: Weak<LocalTimerInner> = Arc::downgrade(&self.inner);
        let task_id = id.clone();

        let join = self.inner.handle.spawn(async move {
            loop {
                let Some(inner) = weak.upgrade() else {
                    return;
                };
                let now = (inner.now_ms)();
                drop(inner);
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
                match guard.get(&task_id) {
                    Some((g, _)) if *g == generation => {
                        guard.remove(&task_id);
                    }
                    _ => return, // Cancelled or replaced before claiming this wake.
                }
            }
            inner.wake_count.fetch_add(1, Ordering::SeqCst);

            let backend = LocalTokioTimerBackend {
                inner: Arc::clone(&inner),
            };
            let store = Arc::clone(&inner.store);
            let health = inner.health.clone();
            let sid = task_id.clone();
            if crate::server::serverless_maintenance::is_system_timer_id(&sid) {
                let tb: Arc<dyn TimerBackend> = Arc::new(backend);
                if let Err(err) =
                    crate::server::serverless_maintenance::fire_system_timer(store, tb, &sid).await
                {
                    eprintln!("anyagents-timer: local system timer failed for {sid}: {err}");
                }
            } else {
                let fired_at_ms = (inner.now_ms)();
                let _ = tokio::task::spawn_blocking(move || {
                    if let Err(err) = fire_schedule_by_id(
                        &store,
                        &health,
                        &backend,
                        &sid,
                        Some(fire_at_ms),
                        fired_at_ms,
                    ) {
                        eprintln!("anyagents-timer: local fire failed for {sid}: {err}");
                    }
                })
                .await;
            }
        });

        if let Some((_, previous)) = guard.insert(id, (generation, join.abort_handle())) {
            previous.abort();
        }
        Ok(join)
    }
}

impl TimerBackend for LocalTokioTimerBackend {
    fn backend_kind(&self) -> &'static str {
        "local_tokio"
    }

    fn schedule_one_shot(
        &self,
        schedule_id: &str,
        fire_at_ms: i64,
    ) -> Result<Option<String>, String> {
        drop(self.arm(schedule_id, fire_at_ms)?);
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
    use anyagents_core::{Agent, AgentSchedule, NewAgent, NewSchedule};
    use tokio::time::{Instant, advance, timeout};

    const EPOCH: i64 = 1_800_000_000_000;

    fn fixture() -> (tempfile::TempDir, Arc<Store>, Agent, LocalTokioTimerBackend) {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let agent = store
            .create_agent(NewAgent::new("timer-test", "test"))
            .unwrap();
        store.set_agent_running(&agent.id, "/work").unwrap();
        let start = Instant::now();
        let backend = LocalTokioTimerBackend::with_clock(
            Arc::clone(&store),
            HealthBoard::default(),
            tokio::runtime::Handle::current(),
            move || EPOCH + start.elapsed().as_millis() as i64,
        );
        (dir, store, agent, backend)
    }

    fn schedule(store: &Store, agent: &Agent, due: i64, interval: Option<i64>) -> AgentSchedule {
        store
            .create_schedule(NewSchedule {
                agent_id: agent.id.clone(),
                kind: if interval.is_some() { "every" } else { "at" }.into(),
                spec: "120ms".into(),
                prompt: "timer wake".into(),
                interval_ms: interval,
                next_fire_at_ms: Some(due),
                watch_baseline: None,
            })
            .unwrap()
    }

    #[tokio::test(start_paused = true)]
    async fn local_tokio_timer_sub_60s_one_shot_fires_precisely_without_rearm() {
        let (_dir, store, agent, backend) = fixture();
        let due = EPOCH + 120;
        let sched = schedule(&store, &agent, due, None);
        let task = backend.arm(&sched.id, due).unwrap();
        assert_eq!(backend.pending_count(), 1);

        advance(Duration::from_millis(119)).await;
        assert_eq!(backend.wake_count(), 0);
        assert!(store.inbox(&agent.id, None).unwrap().is_empty());

        // Allow millisecond timer-wheel rounding, not wall-clock scheduler latency.
        timeout(Duration::from_millis(5), task)
            .await
            .unwrap()
            .unwrap();
        let fired = store.get_schedule(&sched.id).unwrap().unwrap();
        assert_eq!(fired.state, "fired");
        assert!((due..=due + 4).contains(&fired.last_fired_at_ms.unwrap()));
        assert_eq!(store.inbox(&agent.id, None).unwrap().len(), 1);
        advance(Duration::from_secs(60)).await;
        assert_eq!(backend.pending_count(), 0);
        assert_eq!(backend.wake_count(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn local_tokio_timer_recurring_rearms_automatically() {
        let (_dir, store, agent, backend) = fixture();
        let sched = schedule(&store, &agent, EPOCH + 80, Some(120));
        assert_eq!(backend.rehydrate_from_store().unwrap(), 1);

        for expected in 1..=2 {
            timeout(Duration::from_secs(1), async {
                loop {
                    if store.inbox(&agent.id, None).unwrap().len() >= expected
                        && backend.pending_count() == 1
                        && backend.wake_count() == expected as u64
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .unwrap();
            if expected == 1 {
                let first = &store.inbox(&agent.id, None).unwrap()[0];
                store.mark_delivered_up_to(&agent.id, first.seq).unwrap();
            }
        }
        assert_eq!(
            store.get_schedule(&sched.id).unwrap().unwrap().state,
            "active"
        );
        backend.cancel(&sched.id, None).unwrap();
        assert_eq!(backend.pending_count(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn local_tokio_timer_cancel_prevents_firing_and_idle_has_zero_wakes() {
        let (_dir, store, agent, backend) = fixture();
        assert_eq!(backend.rehydrate_from_store().unwrap(), 0);
        advance(Duration::from_secs(60)).await;
        assert_eq!(backend.pending_count(), 0);
        assert_eq!(backend.wake_count(), 0);

        let due = EPOCH + 60_150;
        let sched = schedule(&store, &agent, due, None);
        let task = backend.arm(&sched.id, due).unwrap();
        tokio::task::yield_now().await;
        assert_eq!(backend.pending_count(), 1);
        // Leave the schedule active so store cancellation cannot mask a backend wake.
        backend.cancel(&sched.id, None).unwrap();
        assert!(task.await.unwrap_err().is_cancelled());
        advance(Duration::from_secs(60)).await;
        assert_eq!(backend.pending_count(), 0);
        assert_eq!(backend.wake_count(), 0);
        assert!(store.inbox(&agent.id, None).unwrap().is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn overdue_timers_finish_without_leaving_registered_handles() {
        let (_dir, _store, _agent, backend) = fixture();
        for n in 0..16 {
            backend
                .arm(&format!("missing-{n}"), EPOCH)
                .unwrap()
                .await
                .unwrap();
        }
        assert_eq!(backend.wake_count(), 16);
        assert_eq!(backend.pending_count(), 0);
    }
}
