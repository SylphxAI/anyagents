use std::collections::HashMap;
use std::sync::Mutex;

use anyagents_core::{Agent, NewAgent, NewSchedule};

use super::*;

const DUE: i64 = 1_800_000_000_000;

pub(crate) fn due_schedule(store: &Store, agent: &Agent, kind: &str, due: i64) -> AgentSchedule {
    let (spec, baseline) = if kind == "watch" {
        let path = store.data_dir().join("signal");
        std::fs::write(&path, "ready").unwrap();
        (format!("file:{}", path.display()), Some("missing".into()))
    } else {
        ("1s".to_string(), None)
    };
    store
        .create_schedule(NewSchedule {
            agent_id: agent.id.clone(),
            kind: kind.into(),
            spec,
            prompt: "scheduled wake".into(),
            interval_ms: (kind == "every").then_some(1_000),
            next_fire_at_ms: (kind != "watch").then_some(due),
            watch_baseline: baseline,
        })
        .unwrap()
}

#[derive(Default)]
struct Scheduler {
    attempts: Mutex<Vec<(i64, i64)>>,
    fail: Mutex<usize>,
    pending: Mutex<HashMap<String, (i64, i64)>>,
}

impl TimerBackend for Scheduler {
    fn is_remote(&self) -> bool {
        true
    }

    fn schedule_one_shot(&self, id: &str, delivery_at: i64) -> Result<Option<String>, String> {
        self.schedule_slot(id, delivery_at, None, delivery_at)
    }

    fn schedule_slot(
        &self,
        id: &str,
        delivery_at: i64,
        _expected: Option<i64>,
        intended: i64,
    ) -> Result<Option<String>, String> {
        self.attempts.lock().unwrap().push((delivery_at, intended));
        let run_id = deterministic_run_id(&format!("{id}:{intended}"), delivery_at);
        self.pending
            .lock()
            .unwrap()
            .insert(run_id.clone(), (delivery_at, intended));
        // Model acceptance followed by a transport error, not just rejection.
        let mut fail = self.fail.lock().unwrap();
        if *fail > 0 {
            *fail -= 1;
            return Err("temporary transport failure".into());
        }
        Ok(Some(run_id))
    }

    fn cancel(&self, _id: &str, receipt: Option<&str>) -> Result<(), String> {
        self.pending.lock().unwrap().remove(receipt.unwrap());
        Ok(())
    }
}

#[test]
fn busy_delivery_preserves_slot_and_duplicate_replacement_cannot_claim_the_next_tick() {
    for kind in ["at", "every", "watch"] {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let owner = store
            .create_agent(NewAgent::new("fixture", "test"))
            .unwrap();
        let sched = due_schedule(&store, &owner, kind, DUE);
        let backend = Scheduler::default();
        let fire = |intended, now| {
            fire_schedule_by_id(
                &store,
                &HealthBoard::default(),
                &backend,
                &sched.id,
                Some(intended),
                now,
            )
            .unwrap()
        };
        let lock = store.try_lock_agent_lifecycle(&owner.id).unwrap();
        for now in [DUE, DUE + 1] {
            let busy = fire(DUE, now);
            assert!(busy.is_retryable() && !busy.fired, "{kind}: {busy:?}");
            assert_eq!(busy.reason.as_deref(), Some("lifecycle_busy"));
        }
        let row = store.get_schedule(&sched.id).unwrap().unwrap();
        assert_eq!(row.state, "active");
        assert_eq!(row.next_fire_at_ms, sched.next_fire_at_ms);
        assert_eq!(row.watch_baseline, sched.watch_baseline);
        assert!(store.inbox(&owner.id, None).unwrap().is_empty());
        let (delivery_at, intended) = {
            let pending = backend.pending.lock().unwrap();
            assert_eq!(
                pending.len(),
                1,
                "duplicate submissions must have identical identities"
            );
            *pending.values().next().unwrap()
        };
        assert!((DUE + 5_000..=DUE + 10_000).contains(&delivery_at));
        assert_eq!(intended, DUE);
        drop(lock);

        // Exercise each winning route. For `every` the delayed callback arrives
        // after multiple later due times, with the first message already read.
        let (first_at, replay_at) = if kind == "every" {
            (DUE + 100, delivery_at)
        } else {
            (delivery_at, delivery_at + 1)
        };
        assert!(fire(intended, first_at).fired, "{kind}");
        let inbox = store.inbox(&owner.id, None).unwrap();
        store.mark_delivered_up_to(&owner.id, inbox[0].seq).unwrap();
        assert!(!fire(DUE, replay_at).fired, "{kind}");
        assert_eq!(store.inbox(&owner.id, None).unwrap().len(), 1, "{kind}");
    }
}

#[test]
fn post_claim_retry_reuses_the_run_identity_without_replaying_the_wake() {
    for coalesced in [false, true] {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let owner = store
            .create_agent(NewAgent::new("fixture", "test"))
            .unwrap();
        let sched = due_schedule(&store, &owner, "every", DUE);
        let backend = Scheduler::default();
        let fire = |intended, now| {
            fire_schedule_by_id(
                &store,
                &HealthBoard::default(),
                &backend,
                &sched.id,
                Some(intended),
                now,
            )
            .unwrap()
        };
        let due = if coalesced {
            assert!(fire(DUE, DUE).fired);
            backend.attempts.lock().unwrap().clear();
            backend.pending.lock().unwrap().clear();
            DUE + 1_000
        } else {
            DUE
        };
        *backend.fail.lock().unwrap() = 1;
        let fired = fire(due, due);
        assert_eq!(fired.fired, !coalesced);
        let next = due + 1_000;
        assert_eq!(fired.next_run_ms, Some(next));
        assert_eq!(
            backend.attempts.lock().unwrap().as_slice(),
            &[(next, next); 2]
        );
        assert_eq!(backend.pending.lock().unwrap().len(), 1);
        let inbox = store.inbox(&owner.id, None).unwrap();
        assert_eq!(inbox.len(), 1);
        store.mark_delivered_up_to(&owner.id, inbox[0].seq).unwrap();
        assert_eq!(fire(due, next).reason.as_deref(), Some("stale_slot"));
        assert!(fire(next, next).fired);
        assert_eq!(store.inbox(&owner.id, None).unwrap().len(), 2);
    }
}

#[test]
fn stale_watch_callback_repairs_a_failed_probe_arm_without_rechecking_the_predicate() {
    let dir = crate::test_tempdir();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let owner = store
        .create_agent(NewAgent::new("fixture", "test"))
        .unwrap();
    let sched = due_schedule(&store, &owner, "watch", DUE);
    let path = store.data_dir().join("signal");
    std::fs::remove_file(&path).unwrap();
    let backend = Scheduler::default();
    let fire = |intended, now| {
        fire_schedule_by_id(
            &store,
            &HealthBoard::default(),
            &backend,
            &sched.id,
            Some(intended),
            now,
        )
        .unwrap()
    };
    *backend.fail.lock().unwrap() = 3;
    let first = fire(DUE, DUE);
    assert_eq!(first.reason.as_deref(), Some("rearm_failed"));
    let next = first.next_run_ms.unwrap();
    assert!(next > DUE);
    assert!(
        store
            .get_schedule(&sched.id)
            .unwrap()
            .unwrap()
            .external_timer_id
            .is_none()
    );
    std::fs::write(path, "ready").unwrap();
    let retry = fire(DUE, next + 1);
    assert_eq!(retry.reason.as_deref(), Some("stale_slot"));
    assert!(store.inbox(&owner.id, None).unwrap().is_empty());
    let (delivery_at, intended) = *backend.attempts.lock().unwrap().last().unwrap();
    assert_eq!(intended, next);
    assert!(delivery_at > next + 1);
    assert!(fire(intended, delivery_at).fired);
    assert_eq!(store.inbox(&owner.id, None).unwrap().len(), 1);
}
