//! Rehydrate user timers without flooding the remote scheduler or asking it to
//! fire every overdue callback at the same instant.

use std::sync::{Arc, Mutex};

use anyagents_core::{AgentId, Store, now_ms};
use futures::{StreamExt, stream};

use crate::timer::{REMOTE_TIMER_CONCURRENCY, TimerBackend};

use super::{
    DEFAULT_CLOUD_POLL_DELAY_MS, arm_system_timer_inner, binding_is_active, cloud_poll_timer_id,
    lease_renew_fire_at_ms, lease_renew_timer_id, sandbox_stale_threshold_ms,
    turn_timeout_timer_id,
};

pub(super) const OVERDUE_SCHEDULE_CATCHUP_DELAY_MS: i64 = 1_000;
const CATCHUP_SPACING_MS: i64 = 125;

#[derive(Default)]
pub(super) struct Rearmed {
    pub future: usize,
    pub catchup: usize,
    pub system: usize,
}

enum Entry {
    User(String),
    Turn(String),
    Lease(AgentId),
}

enum Armed {
    Future,
    Catchup,
    System,
}

impl Entry {
    fn key(&self) -> &str {
        match self {
            Self::User(id) | Self::Turn(id) => id,
            Self::Lease(id) => id.as_str(),
        }
    }
}

fn list_entries(store: &Store) -> Vec<Entry> {
    let mut entries = Vec::new();
    let mut catchup = Vec::new();
    // Prefer nearest future user deadlines to backlog catch-up work.
    match store.list_schedules(None, true) {
        Ok(mut schedules) => {
            let now = now_ms();
            schedules.sort_unstable_by_key(|s| {
                s.next_fire_at_ms
                    .filter(|&due| due > now)
                    .unwrap_or(i64::MAX)
            });
            for s in schedules {
                let queue = if s.next_fire_at_ms.is_some_and(|due| due > now) {
                    &mut entries
                } else {
                    &mut catchup
                };
                queue.push(Entry::User(s.id));
            }
        }
        Err(err) => eprintln!("failed to list active schedules for startup re-arm: {err}"),
    }
    match store.list_live_agents() {
        Ok(agents) => {
            for agent in agents {
                match store.remote_turn_store().list_active_for_agent(&agent.id) {
                    Ok(turns) => {
                        entries.extend(turns.into_iter().map(|turn| Entry::Turn(turn.turn_id)))
                    }
                    Err(err) => eprintln!(
                        "failed to list active turns for {} at startup: {err}",
                        agent.id
                    ),
                }
            }
        }
        Err(err) => eprintln!("failed to list agents for startup timer re-arm: {err}"),
    }
    match store.sandbox_binding_store().list_active_ready() {
        Ok(bindings) => entries.extend(
            bindings
                .into_iter()
                .map(|binding| Entry::Lease(binding.agent_id)),
        ),
        Err(err) => eprintln!("failed to list ready leases for startup timer re-arm: {err}"),
    }
    entries.extend(catchup);
    entries
}

pub(super) async fn rearm(store: Arc<Store>, backend: Arc<dyn TimerBackend>) -> Rearmed {
    let mut report = Rearmed::default();
    if backend.is_noop() {
        return report;
    }
    let list_store = Arc::clone(&store);
    let entries = match tokio::task::spawn_blocking(move || list_entries(&list_store)).await {
        Ok(entries) => entries,
        Err(err) => {
            eprintln!("failed to join startup timer census: {err}");
            return report;
        }
    };
    let last_catchup = Arc::new(Mutex::new(0));
    let remote = backend.is_remote();
    let slots = if remote { REMOTE_TIMER_CONCURRENCY } else { 1 };
    // Census keys are unique; each schedule is processed by one worker.
    let mut arms = stream::iter(entries)
        .map(|entry| {
            let store = Arc::clone(&store);
            let backend = Arc::clone(&backend);
            let last_catchup = Arc::clone(&last_catchup);
            async move {
                let id = entry.key().to_owned();
                let result = tokio::task::spawn_blocking(move || match entry {
                    Entry::User(id) => rearm_user(&store, backend.as_ref(), &id, &last_catchup),
                    other => rearm_system(&store, backend.as_ref(), other, &last_catchup),
                })
                .await;
                (id, result)
            }
        })
        .buffer_unordered(slots);
    while let Some((id, result)) = arms.next().await {
        match result {
            Ok(Ok(Some(Armed::Future))) => report.future += 1,
            Ok(Ok(Some(Armed::Catchup))) => report.catchup += 1,
            Ok(Ok(Some(Armed::System))) => report.system += 1,
            Ok(Ok(None)) => {}
            Ok(Err(err)) => eprintln!("failed to re-arm timer {id} at startup: {err}"),
            Err(err) => eprintln!("failed to join startup re-arm for {id}: {err}"),
        }
    }
    report
}

/// Read current rows after obtaining a slot, not from the census snapshot.
fn rearm_user(
    store: &Store,
    backend: &dyn TimerBackend,
    id: &str,
    last_catchup: &Mutex<i64>,
) -> Result<Option<Armed>, String> {
    let Some(sched) = store.get_schedule(id).map_err(|err| err.to_string())? else {
        return Ok(None);
    };
    if sched.state != "active" {
        return Ok(None);
    }
    match sched.kind.as_str() {
        "at" | "every" if sched.next_fire_at_ms.is_some() => {}
        "watch"
            if !matches!(
                crate::schedule::parse_watch_spec(&sched.spec),
                Ok(crate::schedule::WatchTarget::Webhook { .. })
            ) => {}
        _ => return Ok(None),
    }
    match store.agent(&sched.agent_id) {
        Ok(agent)
            if agent.state == "archived"
                || crate::server::is_explicitly_archived(store.data_dir(), &agent) =>
        {
            return Ok(None);
        }
        Err(anyagents_core::Error::AgentNotFound(_)) => return Ok(None),
        Err(err) => return Err(format!("read schedule owner: {err}")),
        Ok(_) => {}
    }

    let now = now_ms();
    let due = sched.next_fire_at_ms.filter(|&due| due > now);
    let fire_at = due.unwrap_or_else(|| catchup_at(backend, last_catchup, now));
    let intended_at = sched.next_fire_at_ms.unwrap_or(fire_at);
    if let Some(run_id) = backend.restore_slot(&sched.id, fire_at, intended_at)? {
        // A cancellation or a callback may have advanced the row while the
        // POST was in flight. Never overwrite a newer slot with this receipt.
        if !store
            .set_schedule_external_timer_id_for_slot(&sched.id, sched.next_fire_at_ms, &run_id)
            .map_err(|err| format!("persist timer receipt: {err}"))?
        {
            backend.cancel(&sched.id, Some(&run_id))?;
            return Ok(None);
        }
    }
    Ok(Some(if due.is_some() || sched.next_fire_at_ms.is_none() {
        Armed::Future
    } else {
        Armed::Catchup
    }))
}

fn catchup_at(backend: &dyn TimerBackend, last_catchup: &Mutex<i64>, now: i64) -> i64 {
    let earliest = now.saturating_add(OVERDUE_SCHEDULE_CATCHUP_DELAY_MS);
    if !backend.is_remote() {
        return earliest;
    }
    let mut latest = last_catchup.lock().unwrap_or_else(|err| err.into_inner());
    *latest = earliest.max(latest.saturating_add(CATCHUP_SPACING_MS));
    *latest
}

fn rearm_system(
    store: &Store,
    backend: &dyn TimerBackend,
    entry: Entry,
    last_catchup: &Mutex<i64>,
) -> Result<Option<Armed>, String> {
    let now = now_ms();
    let (owner, id, deadline) = match entry {
        Entry::Turn(turn_id) => {
            let Some(turn) = store
                .remote_turn_store()
                .get_turn(&turn_id)
                .map_err(|err| err.to_string())?
            else {
                return Ok(None);
            };
            if turn.is_terminal() {
                return Ok(None);
            }
            let (id, due) = if turn.harness_kind == "sandbox" {
                (
                    turn_timeout_timer_id(&turn_id),
                    turn.last_event_at_ms
                        .saturating_add(sandbox_stale_threshold_ms()),
                )
            } else {
                (
                    cloud_poll_timer_id(&turn_id),
                    turn.next_poll_at_ms
                        .unwrap_or_else(|| now.saturating_add(DEFAULT_CLOUD_POLL_DELAY_MS)),
                )
            };
            (turn.agent_id, id, due)
        }
        Entry::Lease(agent_id) => {
            let Some(binding) = store
                .sandbox_binding_store()
                .get(&agent_id)
                .map_err(|err| err.to_string())?
            else {
                return Ok(None);
            };
            if binding.lease_state != "ready" || !binding_is_active(store, &binding) {
                return Ok(None);
            }
            let due = lease_renew_fire_at_ms(binding.lease_expires_at_ms, now);
            (agent_id.clone(), lease_renew_timer_id(&agent_id), due)
        }
        Entry::User(_) => unreachable!(),
    };
    let deadline = if backend.is_remote() && deadline <= now_ms() {
        catchup_at(backend, last_catchup, now_ms())
    } else {
        deadline
    };
    arm_system_timer_inner(store, backend, &owner, &id, deadline, true)?;
    Ok(Some(Armed::System))
}

#[cfg(test)]
mod tests {
    use anyagents_core::{NewAgent, NewSchedule};
    use std::collections::{HashMap, HashSet};
    use std::sync::{
        Condvar,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;
    use tokio::sync::mpsc;

    use super::*;

    struct GatedBackend {
        started: mpsc::UnboundedSender<(String, i64, i64)>,
        released: Mutex<bool>,
        gate: Condvar,
        in_flight: AtomicUsize,
        peak: AtomicUsize,
        cancelled: Mutex<Vec<String>>,
        fail: String,
    }

    impl GatedBackend {
        fn release(&self) {
            *self.released.lock().unwrap() = true;
            self.gate.notify_all();
        }
    }

    // Release waiting workers even when a regression assertion panics.
    struct ReleaseGate(Arc<GatedBackend>);
    impl Drop for ReleaseGate {
        fn drop(&mut self) {
            self.0.release();
        }
    }

    impl TimerBackend for GatedBackend {
        fn is_remote(&self) -> bool {
            true
        }

        fn schedule_one_shot(&self, id: &str, deadline: i64) -> Result<Option<String>, String> {
            self.restore_slot(id, deadline, deadline)
        }

        fn restore_slot(
            &self,
            id: &str,
            deadline: i64,
            intended: i64,
        ) -> Result<Option<String>, String> {
            let active = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(active, Ordering::SeqCst);
            self.started
                .send((id.to_owned(), deadline, intended))
                .unwrap();
            drop(
                self.gate
                    .wait_while(self.released.lock().unwrap(), |open| !*open)
                    .unwrap(),
            );
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            if id == self.fail {
                return Err("scheduler unavailable".into());
            }
            Ok(Some(format!("receipt-{id}")))
        }

        fn cancel(&self, id: &str, receipt: Option<&str>) -> Result<(), String> {
            assert_eq!(receipt, Some(format!("receipt-{id}").as_str()));
            self.cancelled.lock().unwrap().push(id.to_owned());
            Ok(())
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn startup_rearms_every_eligible_schedule_with_bounded_parallelism() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let agent = store
            .create_agent(NewAgent::new("startup-test", "test"))
            .unwrap();
        let make_schedule = |due| {
            store
                .create_schedule(NewSchedule {
                    agent_id: agent.id.clone(),
                    kind: "at".into(),
                    spec: "startup test".into(),
                    prompt: "startup test".into(),
                    interval_ms: None,
                    next_fire_at_ms: Some(due),
                    watch_baseline: None,
                })
                .unwrap()
                .id
        };
        let future = now_ms() + 60_000;
        let fail = make_schedule(future);
        let active: Vec<_> = (0..2 * REMOTE_TIMER_CONCURRENCY)
            .map(|_| make_schedule(1))
            .collect();
        let (started, mut observed) = mpsc::unbounded_channel();
        let backend = Arc::new(GatedBackend {
            started,
            released: Mutex::new(false),
            gate: Condvar::new(),
            in_flight: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            cancelled: Mutex::new(Vec::new()),
            fail: fail.clone(),
        });
        let _release = ReleaseGate(backend.clone());
        let reconcile = tokio::spawn(super::super::startup_reconcile_serverless(
            store.clone(),
            backend.clone(),
        ));
        let mut first_batch = HashSet::new();
        let mut seen = HashMap::new();
        for _ in 0..REMOTE_TIMER_CONCURRENCY {
            let (id, deadline, intended) =
                tokio::time::timeout(Duration::from_secs(20), observed.recv())
                    .await
                    .unwrap()
                    .unwrap();
            assert_eq!(intended, if id == fail { future } else { 1 });
            first_batch.insert(id.clone());
            assert!(seen.insert(id, deadline).is_none());
        }
        assert!(first_batch.contains(&fail));
        let cancelled = first_batch.iter().find(|id| **id != fail).unwrap().clone();
        store.cancel_schedule(Some(&agent.id), &cancelled).unwrap();
        backend.release();

        let report = reconcile.await.unwrap().unwrap();
        while let Ok((id, deadline, intended)) = observed.try_recv() {
            assert_eq!(intended, 1);
            assert!(seen.insert(id, deadline).is_none(), "duplicate startup arm");
        }
        assert_eq!(report.schedules_rearmed, 0);
        assert_eq!(report.overdue_schedules_rearmed, active.len() - 1);
        assert_eq!(
            backend.peak.load(Ordering::SeqCst),
            REMOTE_TIMER_CONCURRENCY
        );
        assert_eq!(backend.in_flight.load(Ordering::SeqCst), 0);
        assert_eq!(
            *backend.cancelled.lock().unwrap(),
            std::slice::from_ref(&cancelled)
        );
        assert_eq!(seen.remove(&fail), Some(future));
        assert_eq!(
            seen.keys().cloned().collect::<HashSet<_>>(),
            active.iter().cloned().collect()
        );
        let mut deadlines: Vec<_> = seen.into_values().collect();
        deadlines.sort_unstable();
        assert!(
            deadlines
                .windows(2)
                .all(|pair| pair[1] - pair[0] >= CATCHUP_SPACING_MS)
        );
        for id in active.into_iter().chain([fail.clone()]) {
            let row = store.get_schedule(&id).unwrap().unwrap();
            if id == cancelled || id == fail {
                assert_eq!(row.external_timer_id, None);
            } else {
                assert_eq!(row.next_fire_at_ms, Some(1));
                assert_eq!(row.external_timer_id, Some(format!("receipt-{id}")));
            }
        }
    }
}
