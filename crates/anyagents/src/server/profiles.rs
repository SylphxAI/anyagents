//! `GET /v1/profiles` from a snapshot.
//!
//! Listing the provider profiles syncs seat credentials into the pool, probes
//! every provider's usage API and rebalances bindings: seconds of disk and
//! network work, which a request used to wait for. The server now keeps the
//! last listing and answers from it at once, each entry marked with the age of
//! its usage reading. A request that finds the snapshot older than
//! [`REFRESH_AFTER`] starts one refresh in the background; concurrent requests
//! share that refresh rather than starting their own. Only a request with no
//! usable snapshot (none yet, or one a change of profiles invalidated) waits,
//! for one refresh, bounded by the per-provider probe timeouts.

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyagents_core::{Store, now_ms};

use crate::create::ProviderProfileEntry;

/// A snapshot at least this old is refreshed when a request reads it.
const REFRESH_AFTER: Duration = Duration::from_secs(30);

type Refresh = dyn Fn(&Store, &Path) -> Vec<ProviderProfileEntry> + Send + Sync;

/// The listing one refresh produced.
struct Snapshot {
    entries: Vec<ProviderProfileEntry>,
    taken_at_ms: i64,
    /// The `wanted` generation the refresh started under.
    generation: u64,
}

#[derive(Default)]
struct State {
    snapshot: Option<Arc<Snapshot>>,
    /// A refresh is running; a request never starts a second one.
    running: bool,
    /// The oldest generation a request may be answered from. A change of
    /// profiles bumps it, so no request is answered from before the change.
    wanted: u64,
    /// How many refreshes failed, and the generation and reason of the last.
    failures: u64,
    failed: Option<(u64, String)>,
}

pub(crate) struct ProfileSnapshots {
    state: Mutex<State>,
    /// Bumped after every refresh, so waiting requests look again.
    published: tokio::sync::watch::Sender<u64>,
    refresh: Arc<Refresh>,
}

impl Default for ProfileSnapshots {
    fn default() -> Self {
        Self::with_refresh(Arc::new(|store: &Store, data: &Path| {
            crate::create::list_provider_profiles_fast(store, data)
        }))
    }
}

/// One entry as the route answers it: the listing, and how old its usage is.
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct ServedProfile {
    #[serde(flatten)]
    pub entry: ProviderProfileEntry,
    /// Milliseconds since the snapshot this entry comes from was taken.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage_age_ms: Option<i64>,
}

impl ProfileSnapshots {
    fn with_refresh(refresh: Arc<Refresh>) -> Self {
        Self {
            state: Mutex::new(State::default()),
            published: tokio::sync::watch::channel(0).0,
            refresh,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The listing, from the snapshot when there is a usable one. `fresh`
    /// asks for a listing made after this call.
    pub(crate) async fn list(
        self: &Arc<Self>,
        store: &Arc<Store>,
        fresh: bool,
    ) -> Result<Vec<ServedProfile>, String> {
        if fresh {
            self.invalidate(store);
        }
        // Subscribe before looking, so a refresh that ends in between is seen.
        let mut published = self.published.subscribe();
        let (wanted, failures) = {
            let state = self.lock();
            (state.wanted, state.failures)
        };
        loop {
            {
                let mut state = self.lock();
                if let Some(snapshot) = state.snapshot.clone()
                    && snapshot.generation >= wanted
                {
                    let age = now_ms().saturating_sub(snapshot.taken_at_ms).max(0);
                    if age >= REFRESH_AFTER.as_millis() as i64 {
                        self.start(&mut state, store);
                    }
                    return Ok(served(&snapshot, age));
                }
                // A refresh this request waited for failed; the next request
                // tries again.
                if state.failures > failures
                    && let Some((generation, why)) = &state.failed
                    && *generation >= wanted
                {
                    return Err(why.clone());
                }
                self.start(&mut state, store);
            }
            if published.changed().await.is_err() {
                return Err("the profile listing stopped".into());
            }
        }
    }

    /// Profiles changed: no request is answered from an older listing, and a
    /// new one starts now.
    pub(crate) fn invalidate(self: &Arc<Self>, store: &Arc<Store>) {
        let mut state = self.lock();
        state.wanted += 1;
        self.start(&mut state, store);
    }

    /// Start a refresh unless one is running; a running refresh goes round
    /// again when it ends behind `wanted`.
    fn start(self: &Arc<Self>, state: &mut State, store: &Arc<Store>) {
        if state.running {
            return;
        }
        state.running = true;
        let this = Arc::clone(self);
        let store = Arc::clone(store);
        tokio::task::spawn_blocking(move || this.refresh_until_current(&store));
    }

    fn refresh_until_current(&self, store: &Store) {
        loop {
            let generation = self.lock().wanted;
            let listed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                (self.refresh)(store, store.data_dir())
            }));
            let mut state = self.lock();
            match listed {
                Ok(entries) => {
                    state.snapshot = Some(Arc::new(Snapshot {
                        entries,
                        taken_at_ms: now_ms(),
                        generation,
                    }));
                }
                Err(_) => {
                    state.failures += 1;
                    state.failed = Some((generation, "the profile listing failed".into()));
                }
            }
            let behind = state.wanted > generation;
            if !behind {
                state.running = false;
            }
            drop(state);
            self.published.send_modify(|count| *count += 1);
            if !behind {
                return;
            }
        }
    }
}

fn served(snapshot: &Snapshot, age: i64) -> Vec<ServedProfile> {
    snapshot
        .entries
        .iter()
        .map(|entry| ServedProfile {
            entry: entry.clone(),
            usage_age_ms: Some(age),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    fn entry(usage: &str) -> ProviderProfileEntry {
        ProviderProfileEntry {
            provider: "openai".into(),
            profile: "default".into(),
            account: None,
            kind: "subscription".into(),
            compatible_harnesses: vec!["codex".into()],
            assigned_agents: Vec::new(),
            subagent_count: 0,
            usage: Some(usage.into()),
        }
    }

    /// A refresh that counts its runs and waits at a gate the test opens, in
    /// place of the probes: nothing here touches the network.
    struct Gate {
        runs: AtomicUsize,
        open: std::sync::Mutex<bool>,
        opened: std::sync::Condvar,
    }

    impl Gate {
        fn new(open: bool) -> Arc<Self> {
            Arc::new(Self {
                runs: AtomicUsize::new(0),
                open: std::sync::Mutex::new(open),
                opened: std::sync::Condvar::new(),
            })
        }

        fn set(&self, open: bool) {
            *self.open.lock().unwrap() = open;
            self.opened.notify_all();
        }

        fn snapshots(self: &Arc<Self>) -> Arc<ProfileSnapshots> {
            let gate = Arc::clone(self);
            Arc::new(ProfileSnapshots::with_refresh(Arc::new(move |_, _| {
                let run = gate.runs.fetch_add(1, Ordering::SeqCst) + 1;
                let mut open = gate.open.lock().unwrap();
                while !*open {
                    open = gate.opened.wait(open).unwrap();
                }
                vec![entry(&format!("run {run}"))]
            })))
        }
    }

    fn store() -> (tempfile::TempDir, Arc<Store>) {
        let dir = crate::test_tempdir();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        (dir, store)
    }

    fn usage(listed: &[ServedProfile]) -> &str {
        listed[0].entry.usage.as_deref().unwrap()
    }

    /// The first request waits for one refresh; later requests answer from the
    /// snapshot at once, even while a refresh is held, and concurrent requests
    /// share one refresh.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn requests_answer_from_the_snapshot_and_share_one_refresh() {
        let (_dir, store) = store();
        let gate = Gate::new(true);
        let snapshots = gate.snapshots();

        let cold = futures::future::join_all((0..4).map(|_| {
            let snapshots = Arc::clone(&snapshots);
            let store = Arc::clone(&store);
            async move { snapshots.list(&store, false).await.unwrap() }
        }))
        .await;
        assert_eq!(
            gate.runs.load(Ordering::SeqCst),
            1,
            "one refresh served all"
        );
        for listed in &cold {
            assert_eq!(usage(listed), "run 1");
            assert!(listed[0].usage_age_ms.is_some());
        }

        // An old snapshot is served at once and refreshed behind the answer,
        // however long that refresh takes.
        gate.set(false);
        {
            let mut state = snapshots.lock();
            let old = state.snapshot.take().unwrap();
            state.snapshot = Some(Arc::new(Snapshot {
                entries: old.entries.clone(),
                taken_at_ms: old.taken_at_ms - REFRESH_AFTER.as_millis() as i64,
                generation: old.generation,
            }));
        }
        for _ in 0..3 {
            let listed =
                tokio::time::timeout(Duration::from_secs(2), snapshots.list(&store, false))
                    .await
                    .expect("a snapshot answers without waiting for its refresh")
                    .unwrap();
            assert_eq!(usage(&listed), "run 1");
            assert!(listed[0].usage_age_ms.unwrap() >= REFRESH_AFTER.as_millis() as i64);
        }
        while gate.runs.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
        gate.set(true);
        loop {
            let listed = snapshots.list(&store, false).await.unwrap();
            if usage(&listed) == "run 2" {
                assert!(listed[0].usage_age_ms.unwrap() < REFRESH_AFTER.as_millis() as i64);
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            gate.runs.load(Ordering::SeqCst),
            2,
            "one background refresh"
        );
    }

    /// After a change of profiles, or when asked for a fresh listing, no
    /// request is answered from before it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_change_is_never_answered_from_an_older_listing() {
        let (_dir, store) = store();
        let gate = Gate::new(true);
        let snapshots = gate.snapshots();
        assert_eq!(
            usage(&snapshots.list(&store, false).await.unwrap()),
            "run 1"
        );
        snapshots.invalidate(&store);
        assert_eq!(
            usage(&snapshots.list(&store, false).await.unwrap()),
            "run 2"
        );
        assert_eq!(usage(&snapshots.list(&store, true).await.unwrap()), "run 3");
        assert_eq!(
            usage(&snapshots.list(&store, false).await.unwrap()),
            "run 3"
        );

        // A change during a refresh makes that refresh go round once more.
        gate.set(false);
        let waiting = {
            let snapshots = Arc::clone(&snapshots);
            let store = Arc::clone(&store);
            tokio::spawn(async move { snapshots.list(&store, true).await.unwrap() })
        };
        while gate.runs.load(Ordering::SeqCst) < 4 {
            tokio::task::yield_now().await;
        }
        snapshots.invalidate(&store);
        gate.set(true);
        // The waiting request began before that change: either listing is
        // newer than its call. A request after the change gets the second.
        let answered = waiting.await.unwrap();
        assert!(matches!(usage(&answered), "run 4" | "run 5"));
        assert_eq!(
            usage(&snapshots.list(&store, false).await.unwrap()),
            "run 5"
        );
        assert_eq!(gate.runs.load(Ordering::SeqCst), 5);
    }

    /// The route's entries are the listing's fields plus the age; a client
    /// reads an older server's answer, which has no age, the same way.
    #[test]
    fn a_served_entry_is_the_listing_plus_its_age() {
        let served = ServedProfile {
            entry: entry("5h: 80% left"),
            usage_age_ms: Some(1_500),
        };
        let json = serde_json::to_value(&served).unwrap();
        assert_eq!(json["provider"], "openai");
        assert_eq!(json["usage"], "5h: 80% left");
        assert_eq!(json["usage_age_ms"], 1_500);
        let older = serde_json::to_value(entry("5h: 80% left")).unwrap();
        let read: ServedProfile = serde_json::from_value(older).unwrap();
        assert_eq!(read.usage_age_ms, None);
        assert_eq!(read.entry.usage.as_deref(), Some("5h: 80% left"));
    }

    /// The listing the server refreshes in the background never writes into an
    /// agent whose creation is in flight or failed: a rollback that removed
    /// its tree stays removed, and the agent is still counted on its profile.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_listing_never_writes_into_an_agent_being_created() {
        let (dir, store) = store();
        let data = dir.path();
        std::fs::create_dir_all(data.join("profiles/openai/default")).unwrap();
        std::fs::write(
            data.join("profiles/openai/default/profile.json"),
            r#"{"provider":"openai","kind":"subscription","compatible_harnesses":["codex"]}"#,
        )
        .unwrap();
        let spawning = store
            .create_agent(anyagents_core::NewAgent::new("spawning", "codex"))
            .unwrap();
        assert_eq!(spawning.state, "created");
        let running = store
            .create_agent(anyagents_core::NewAgent::new("running", "codex"))
            .unwrap();
        store.set_agent_state(&running.id, "running").unwrap();
        let running_cfg = anyagents_core::agent_config_dir(data, &running.id);
        std::fs::create_dir_all(&running_cfg).unwrap();

        let snapshots = Arc::new(ProfileSnapshots::default());
        let listed = snapshots.list(&store, true).await.unwrap();
        crate::create::sync_fresher_bound_agent_credentials_in(&store, data);

        assert!(!data.join("agents").join(spawning.id.as_str()).exists());
        assert_eq!(
            std::fs::read_to_string(running_cfg.join(".anyagents-profile")).unwrap(),
            "openai/default\n"
        );
        let pool = listed
            .iter()
            .find(|served| served.entry.provider == "openai" && served.entry.profile == "default")
            .unwrap();
        assert_eq!(pool.entry.assigned_agents, vec!["running", "spawning"]);
    }

    /// A refresh that panics answers its waiting requests with an error and
    /// leaves the next request free to try again.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failed_refresh_answers_and_the_next_request_retries() {
        let (_dir, store) = store();
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&calls);
        let snapshots = Arc::new(ProfileSnapshots::with_refresh(Arc::new(move |_, _| {
            if counted.fetch_add(1, Ordering::SeqCst) == 0 {
                panic!("probe exploded");
            }
            vec![entry("recovered")]
        })));
        assert!(snapshots.list(&store, false).await.is_err());
        assert_eq!(
            usage(&snapshots.list(&store, false).await.unwrap()),
            "recovered"
        );
    }
}
