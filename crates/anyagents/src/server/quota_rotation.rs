//! Profile rotation for agents a view shows quota-held.
//!
//! Recording a profile's reset and rotating an exhausted profile read vendor
//! usage APIs and sync credentials, so they never run inside a request. A view
//! that sees an active quota hold reports it here, and one worker thread acts
//! on it when something can change the answer, never on a cadence:
//! - the hold starts, or its reset moves;
//! - a profile of the hold's provider becomes available: a held agent's reset
//!   or a profile's recorded reset passes, a held agent's profile recovers or
//!   moves, or a profile's recorded reset is cleared;
//! - a profile of the provider is added.
//!
//! Between events the worker sleeps until the earliest time an answer can
//! change. Recorded resets are `profiles/<provider>/<profile>/` markers. On
//! Linux an inotify watch on `profiles/` reports clears and additions made by
//! any process, such as a CLI sign-in or the profile listing. The rotation's
//! own check cooldown defers an event that falls inside it to the moment it
//! ends; it never drops one.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::Duration;

use anyagents_core::{AgentId, Store, agent_config_dir, now_ms};

/// The window in which `try_rotate_exhausted_profile` answers a held agent
/// from its last check (`.anyagents-last-quota-check-ms`) instead of probing.
const COOLDOWN_MS: i64 = 120_000;

/// A profile's recorded reset, written while an agent bound to it is held.
const RESET_MARKER: &str = ".anyagents-quota-reset-ms";

/// The latest hold a view reported for one agent.
#[derive(Clone, Copy)]
struct Reported {
    reset_ms: i64,
    seen_ms: i64,
}

#[derive(Default)]
struct Inbox {
    reported: HashMap<AgentId, Reported>,
    /// Agents whose hold is new, or whose reset moved, since the worker looked.
    fresh: HashSet<AgentId>,
    /// Providers with a changed profile set or a cleared reset.
    changed: HashSet<String>,
    /// Every provider may have changed.
    all_changed: bool,
}

impl Inbox {
    /// Record a view's report; whether the worker has something new to do.
    fn report(&mut self, agent_id: &AgentId, reset_ms: i64, now: i64) -> bool {
        let previous = self.reported.insert(
            agent_id.clone(),
            Reported {
                reset_ms,
                seen_ms: now,
            },
        );
        let fresh = previous.is_none_or(|previous| previous.reset_ms != reset_ms);
        if fresh {
            self.fresh.insert(agent_id.clone());
        }
        fresh
    }

    fn is_empty(&self) -> bool {
        self.fresh.is_empty() && self.changed.is_empty() && !self.all_changed
    }
}

#[derive(Default)]
struct Shared {
    inbox: Mutex<Inbox>,
    ready: Condvar,
}

impl Shared {
    fn inbox(&self) -> MutexGuard<'_, Inbox> {
        self.inbox.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

static SHARED: OnceLock<Shared> = OnceLock::new();

/// Report that a view sees `agent_id` held until `reset_ms`. Only a new hold
/// or a moved reset wakes the worker. A no-op where no worker runs (offline
/// reads, tests).
pub(crate) fn queue(agent_id: &AgentId, reset_ms: i64) {
    let Some(shared) = SHARED.get() else {
        return;
    };
    if shared.inbox().report(agent_id, reset_ms, now_ms()) {
        shared.ready.notify_one();
    }
}

/// A profile of `provider` was added or recovered; `None` when the provider
/// is unknown. Held agents of that provider are reconsidered at once.
pub(crate) fn profiles_changed(provider: Option<&str>) {
    let Some(shared) = SHARED.get() else {
        return;
    };
    {
        let mut inbox = shared.inbox();
        match provider {
            Some(provider) => {
                inbox.changed.insert(provider.to_owned());
            }
            None => inbox.all_changed = true,
        }
    }
    shared.ready.notify_one();
}

/// Start the worker once per process. It sleeps until an event or the
/// earliest time a held agent's answer can change.
pub(crate) fn start(store: Arc<Store>) {
    if SHARED.set(Shared::default()).is_err() {
        return;
    }
    #[cfg(target_os = "linux")]
    {
        let profiles = store.data_dir().join("profiles");
        let spawned = std::thread::Builder::new()
            .name("anyagents-quota-watch".into())
            .spawn(move || {
                if let Err(err) = watch::run(&profiles, profiles_changed) {
                    eprintln!(
                        "quota rotation: profile watch stopped ({err}); additions and clears by other processes wait for the next reset"
                    );
                }
            });
        if let Err(err) = spawned {
            eprintln!("quota rotation profile watch did not start: {err}");
        }
    }
    let spawned = std::thread::Builder::new()
        .name("anyagents-quota-rotation".into())
        .spawn(move || run(&store));
    if let Err(err) = spawned {
        eprintln!("quota rotation worker did not start: {err}");
    }
}

fn run(store: &Store) {
    let shared = SHARED
        .get()
        .expect("shared state is set before the worker starts");
    let data = store.data_dir();
    let mut schedule = Schedule::default();
    loop {
        let (fresh, changed) = wait(shared, schedule.next_wake());
        let now = now_ms();
        for agent_id in fresh {
            let Some(reported) = shared.inbox().reported.get(&agent_id).copied() else {
                continue;
            };
            match store.agent(&agent_id) {
                Ok(agent) => schedule.start(
                    agent_id,
                    reported.reset_ms,
                    crate::create::upstream_provider_of(&agent.harness, agent.model.as_deref()),
                    now,
                ),
                Err(_) => {
                    shared.inbox().reported.remove(&agent_id);
                }
            }
        }
        match changed {
            None => schedule.provider_changed(None, now),
            Some(providers) => {
                for provider in providers {
                    schedule.provider_changed(Some(&provider), now);
                }
            }
        }
        for agent_id in schedule.take_due(now) {
            let Some(hold) = schedule.holds.get(&agent_id).cloned() else {
                continue;
            };
            let seen_ms = shared
                .inbox()
                .reported
                .get(&agent_id)
                .map(|reported| reported.seen_ms);
            let outcome = act(store, &agent_id, &hold, seen_ms, now_ms());
            if matches!(outcome, Outcome::Ended { .. }) {
                shared.inbox().reported.remove(&agent_id);
            }
            schedule.settle(&agent_id, outcome, now_ms());
        }
        schedule.plan(now_ms(), |provider| recorded_resets(data, provider));
    }
}

/// Wait for an event or until `wake_ms`. `None` for the changed providers
/// means every provider.
fn wait(shared: &Shared, wake_ms: Option<i64>) -> (Vec<AgentId>, Option<Vec<String>>) {
    let mut inbox = shared.inbox();
    while inbox.is_empty() {
        let now = now_ms();
        match wake_ms {
            Some(wake) if wake <= now => break,
            Some(wake) => {
                let timeout = Duration::from_millis(u64::try_from(wake - now).unwrap_or(0));
                inbox = shared
                    .ready
                    .wait_timeout(inbox, timeout)
                    .unwrap_or_else(PoisonError::into_inner)
                    .0;
            }
            None => {
                inbox = shared
                    .ready
                    .wait(inbox)
                    .unwrap_or_else(PoisonError::into_inner);
            }
        }
    }
    let fresh = std::mem::take(&mut inbox.fresh).into_iter().collect();
    let changed = if std::mem::take(&mut inbox.all_changed) {
        inbox.changed.clear();
        None
    } else {
        Some(std::mem::take(&mut inbox.changed).into_iter().collect())
    };
    (fresh, changed)
}

/// Future resets recorded on the profiles of `provider`.
fn recorded_resets(data: &Path, provider: &str) -> Vec<i64> {
    crate::create::list_harness_profiles(data, provider)
        .into_iter()
        .filter_map(|(_, dir)| read_ms(&dir.join(RESET_MARKER)))
        .collect()
}

fn read_ms(path: &Path) -> Option<i64> {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|raw| raw.trim().parse().ok())
}

#[derive(Clone)]
struct Hold {
    reset_ms: i64,
    provider: String,
    /// When to act on this hold by itself: at once for a new hold or an
    /// event, or when the rotation's cooldown ends.
    due_ms: Option<i64>,
}

#[derive(Debug, PartialEq)]
enum Outcome {
    /// The hold is over: cleared, moved, archived, or past its reset. `freed`
    /// says a profile of its provider may have become available.
    Ended { freed: bool },
    /// The rotation's cooldown answers until then.
    Deferred(i64),
    /// No profile can take the agent yet.
    Waiting,
}

#[derive(Default)]
struct Schedule {
    holds: HashMap<AgentId, Hold>,
    /// Per provider, the earliest future time one of its profiles becomes
    /// available: a held agent's reset or a recorded profile reset.
    wake: HashMap<String, i64>,
}

impl Schedule {
    /// A hold starts or its reset moves: act at once.
    fn start(&mut self, agent_id: AgentId, reset_ms: i64, provider: String, now: i64) {
        let reset_moves_earlier = self.wake.get(&provider).is_none_or(|&wake| reset_ms < wake);
        if reset_moves_earlier && reset_ms > now {
            self.wake.insert(provider.clone(), reset_ms);
        }
        self.holds.insert(
            agent_id,
            Hold {
                reset_ms,
                provider,
                due_ms: Some(now),
            },
        );
    }

    /// A profile under `profiles/<provider>` (under any provider for `None`)
    /// changed. A hold's provider also draws on its compatible directories.
    fn provider_changed(&mut self, provider: Option<&str>, now: i64) {
        for hold in self.holds.values_mut() {
            if provider.is_none_or(|provider| {
                provider == hold.provider
                    || crate::create::compatible_harnesses_for(&hold.provider).contains(&provider)
            }) {
                hold.due_ms = Some(now);
            }
        }
    }

    fn next_wake(&self) -> Option<i64> {
        self.holds
            .values()
            .filter_map(|hold| hold.due_ms)
            .chain(self.wake.values().copied())
            .min()
    }

    /// Holds to act on now: those due by themselves and every hold of a
    /// provider one of whose profiles became available.
    fn take_due(&mut self, now: i64) -> Vec<AgentId> {
        let woken: HashSet<String> = self
            .wake
            .iter()
            .filter(|&(_, &at)| at <= now)
            .map(|(provider, _)| provider.clone())
            .collect();
        self.wake.retain(|provider, _| !woken.contains(provider));
        let mut due: Vec<AgentId> = self
            .holds
            .iter_mut()
            .filter(|(_, hold)| {
                woken.contains(&hold.provider) || hold.due_ms.is_some_and(|at| at <= now)
            })
            .map(|(agent_id, hold)| {
                hold.due_ms = None;
                agent_id.clone()
            })
            .collect();
        due.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        due
    }

    fn settle(&mut self, agent_id: &AgentId, outcome: Outcome, now: i64) {
        match outcome {
            Outcome::Ended { freed } => {
                if let Some(hold) = self.holds.remove(agent_id)
                    && freed
                {
                    self.provider_changed(Some(&hold.provider), now);
                }
            }
            Outcome::Deferred(at) => {
                if let Some(hold) = self.holds.get_mut(agent_id) {
                    hold.due_ms = Some(at);
                }
            }
            // Events before this act are answered by it; later ones are not.
            Outcome::Waiting => {
                if let Some(hold) = self.holds.get_mut(agent_id) {
                    hold.due_ms = None;
                }
            }
        }
    }

    /// Recompute each provider's next availability from its holds' resets and
    /// its profiles' recorded resets.
    fn plan(&mut self, now: i64, recorded: impl Fn(&str) -> Vec<i64>) {
        let providers: BTreeSet<String> = self
            .holds
            .values()
            .map(|hold| hold.provider.clone())
            .collect();
        self.wake.clear();
        for provider in providers {
            let next = self
                .holds
                .values()
                .filter(|hold| hold.provider == provider)
                .map(|hold| hold.reset_ms)
                .chain(recorded(&provider))
                .filter(|&at| at > now)
                .min();
            if let Some(at) = next {
                self.wake.insert(provider, at);
            }
        }
    }
}

/// Act on one hold: record its reset on its profile, then rotate or clear it
/// unless the rotation's cooldown still answers.
fn act(store: &Store, agent_id: &AgentId, hold: &Hold, seen_ms: Option<i64>, now: i64) -> Outcome {
    let Ok(agent) = store.agent(agent_id) else {
        return Outcome::Ended { freed: false };
    };
    if agent.state == "archived" {
        return Outcome::Ended { freed: false };
    }
    if hold.reset_ms <= now {
        // Its profile is available again; the agent's own block clears on
        // the same deadline.
        return Outcome::Ended { freed: true };
    }
    let data = store.data_dir();
    let cfg = agent_config_dir(data, agent_id);
    // Cleared or rotated after a view last reported it: this hold is over. A
    // view that sees a new one reports it again.
    let cleared_ms = [
        ".anyagents-quota-cleared-ms",
        ".anyagents-profile-rotated-ms",
    ]
    .into_iter()
    .filter_map(|marker| read_ms(&cfg.join(marker)))
    .max()
    .unwrap_or(0);
    if seen_ms.is_none_or(|seen| cleared_ms > seen) {
        return Outcome::Ended { freed: false };
    }
    crate::create::record_profile_quota_reset(data, &agent, hold.reset_ms);
    if let Some(checked) = read_ms(&cfg.join(".anyagents-last-quota-check-ms"))
        && now.saturating_sub(checked) < COOLDOWN_MS
    {
        return Outcome::Deferred(checked + COOLDOWN_MS);
    }
    match crate::create::try_rotate_exhausted_profile(store, data, &agent, true) {
        Some(_) => Outcome::Ended { freed: true },
        None => Outcome::Waiting,
    }
}

/// Reports profile additions and cleared resets under `profiles/`, by any
/// process, to the worker.
#[cfg(target_os = "linux")]
mod watch {
    use std::collections::HashMap;
    use std::ffi::{CString, OsStr};
    use std::io;
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use std::os::unix::ffi::OsStrExt;
    use std::path::{Path, PathBuf};

    #[derive(Clone, Copy, PartialEq)]
    enum Level {
        /// The data directory, until `profiles/` exists.
        Data,
        Profiles,
        Provider,
        Profile,
    }

    struct Watches {
        fd: OwnedFd,
        dirs: HashMap<i32, (PathBuf, Level)>,
    }

    impl Watches {
        /// Watch `dir` and, below `profiles/`, every profile directory in it.
        fn add(&mut self, dir: &Path, level: Level) -> io::Result<()> {
            let mask = match level {
                Level::Data | Level::Profiles | Level::Provider => {
                    libc::IN_CREATE | libc::IN_MOVED_TO
                }
                Level::Profile => libc::IN_DELETE | libc::IN_MOVED_FROM,
            } | libc::IN_ONLYDIR;
            let path = CString::new(dir.as_os_str().as_bytes())?;
            let wd = unsafe { libc::inotify_add_watch(self.fd.as_raw_fd(), path.as_ptr(), mask) };
            if wd < 0 {
                return Err(io::Error::last_os_error());
            }
            self.dirs.insert(wd, (dir.to_path_buf(), level));
            let child = match level {
                Level::Profiles => Level::Provider,
                Level::Provider => Level::Profile,
                Level::Data | Level::Profile => return Ok(()),
            };
            for entry in std::fs::read_dir(dir)?.flatten() {
                if entry.file_type().is_ok_and(|kind| kind.is_dir())
                    && !is_hidden(&entry.file_name())
                {
                    // A directory that vanished meanwhile needs no watch.
                    let _ = self.add(&entry.path(), child);
                }
            }
            Ok(())
        }
    }

    fn is_hidden(name: &OsStr) -> bool {
        name.as_bytes().first() == Some(&b'.')
    }

    fn provider_of(dir: &Path) -> Option<String> {
        dir.file_name().and_then(OsStr::to_str).map(str::to_owned)
    }

    /// Runs until reading the watch fails. A profile directory added under a
    /// provider, a provider added with its profiles, and a removed recorded
    /// reset report that provider; a lost event reports every provider.
    pub(super) fn run(profiles: &Path, changed: fn(Option<&str>)) -> io::Result<()> {
        let fd = unsafe { libc::inotify_init1(libc::IN_CLOEXEC) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        let mut watches = Watches {
            fd: unsafe { OwnedFd::from_raw_fd(fd) },
            dirs: HashMap::new(),
        };
        if profiles.is_dir() {
            watches.add(profiles, Level::Profiles)?;
        } else {
            let data = profiles.parent().unwrap_or(Path::new("."));
            watches.add(data, Level::Data)?;
            if profiles.is_dir() {
                watches.add(profiles, Level::Profiles)?;
            }
        }
        let header = std::mem::size_of::<libc::inotify_event>();
        let mut buffer = vec![0u8; 64 * 1024];
        loop {
            let read = unsafe {
                libc::read(
                    watches.fd.as_raw_fd(),
                    buffer.as_mut_ptr().cast(),
                    buffer.len(),
                )
            };
            let Ok(read) = usize::try_from(read) else {
                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(err);
            };
            let mut offset = 0;
            while offset + header <= read {
                let event: libc::inotify_event =
                    unsafe { std::ptr::read_unaligned(buffer.as_ptr().add(offset).cast()) };
                let name_end = (offset + header + event.len as usize).min(read);
                let raw = &buffer[offset + header..name_end];
                let name = OsStr::from_bytes(
                    &raw[..raw.iter().position(|&b| b == 0).unwrap_or(raw.len())],
                );
                offset = name_end;
                if event.mask & libc::IN_Q_OVERFLOW != 0 {
                    changed(None);
                    continue;
                }
                if event.mask & libc::IN_IGNORED != 0 {
                    watches.dirs.remove(&event.wd);
                    continue;
                }
                let Some((dir, level)) = watches.dirs.get(&event.wd).cloned() else {
                    continue;
                };
                let created_dir = event.mask & libc::IN_ISDIR != 0
                    && event.mask & (libc::IN_CREATE | libc::IN_MOVED_TO) != 0
                    && !is_hidden(name);
                match level {
                    Level::Data if created_dir && name == "profiles" => {
                        let _ = watches.add(&dir.join(name), Level::Profiles);
                        changed(None);
                    }
                    Level::Profiles if created_dir => {
                        let _ = watches.add(&dir.join(name), Level::Provider);
                        changed(name.to_str());
                    }
                    Level::Provider if created_dir => {
                        let _ = watches.add(&dir.join(name), Level::Profile);
                        changed(provider_of(&dir).as_deref());
                    }
                    Level::Profile if name == super::RESET_MARKER => {
                        changed(dir.parent().and_then(provider_of).as_deref());
                    }
                    _ => {}
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR: i64 = 3_600_000;
    const DAY: i64 = 24 * HOUR;

    fn waiting(schedule: &mut Schedule, agent_id: &AgentId, now: i64) {
        schedule.settle(agent_id, Outcome::Waiting, now);
    }

    #[test]
    fn a_view_wakes_the_worker_only_for_a_new_hold_or_a_moved_reset() {
        let mut inbox = Inbox::default();
        let id = AgentId::generate();
        assert!(inbox.report(&id, 1_000, 10), "a new hold");
        inbox.fresh.clear();
        assert!(!inbox.report(&id, 1_000, 20), "the same hold");
        assert_eq!(inbox.reported[&id].seen_ms, 20, "still seen");
        assert!(inbox.is_empty());
        assert!(inbox.report(&id, 2_000, 30), "a moved reset");
        assert!(!inbox.is_empty());
    }

    /// A hold whose reset is days away is acted on once, then not again until
    /// something can change the answer: no fixed recheck cadence.
    #[test]
    fn a_waiting_hold_sleeps_until_its_providers_next_availability() {
        let now = 1_000_000;
        let mut schedule = Schedule::default();
        let id = AgentId::generate();
        schedule.start(id.clone(), now + 3 * DAY, "openai".into(), now);
        assert_eq!(
            schedule.take_due(now),
            std::slice::from_ref(&id),
            "a new hold acts at once"
        );
        waiting(&mut schedule, &id, now);
        schedule.plan(now, |_| Vec::new());
        assert_eq!(schedule.next_wake(), Some(now + 3 * DAY));
        assert!(schedule.take_due(now + 2 * DAY).is_empty());

        // Another profile of the provider recorded an earlier reset.
        schedule.plan(now, |provider| {
            assert_eq!(provider, "openai");
            vec![now - HOUR, now + 5 * HOUR]
        });
        assert_eq!(schedule.next_wake(), Some(now + 5 * HOUR));
        assert_eq!(schedule.take_due(now + 5 * HOUR), std::slice::from_ref(&id));
    }

    #[test]
    fn a_provider_event_acts_on_that_providers_holds_only() {
        let now = 1_000_000;
        let mut schedule = Schedule::default();
        let (openai, google) = (AgentId::generate(), AgentId::generate());
        schedule.start(openai.clone(), now + DAY, "openai".into(), now);
        schedule.start(google.clone(), now + DAY, "google".into(), now);
        schedule.take_due(now);
        waiting(&mut schedule, &openai, now);
        waiting(&mut schedule, &google, now);
        schedule.plan(now, |_| Vec::new());
        schedule.provider_changed(Some("openai"), now + HOUR);
        assert_eq!(schedule.take_due(now + HOUR), std::slice::from_ref(&openai));
        waiting(&mut schedule, &openai, now + HOUR);
        schedule.provider_changed(Some("codex"), now + HOUR);
        assert_eq!(
            schedule.take_due(now + HOUR),
            std::slice::from_ref(&openai),
            "a compatible profile directory"
        );
        schedule.provider_changed(None, now + 2 * HOUR);
        let due: HashSet<AgentId> = schedule.take_due(now + 2 * HOUR).into_iter().collect();
        assert_eq!(due, HashSet::from([google, openai]));
    }

    /// An event inside the rotation's cooldown is deferred to its end, never
    /// dropped.
    #[test]
    fn an_event_inside_the_cooldown_is_deferred_not_dropped() {
        let now = 1_000_000;
        let mut schedule = Schedule::default();
        let id = AgentId::generate();
        schedule.start(id.clone(), now + DAY, "openai".into(), now);
        schedule.take_due(now);
        schedule.settle(&id, Outcome::Deferred(now + COOLDOWN_MS), now);
        schedule.plan(now, |_| Vec::new());
        assert_eq!(schedule.next_wake(), Some(now + COOLDOWN_MS));
        assert!(schedule.take_due(now + COOLDOWN_MS - 1).is_empty());
        assert_eq!(schedule.take_due(now + COOLDOWN_MS), [id]);
    }

    /// A hold that ends by recovering or moving, or by its reset passing,
    /// frees a profile: the provider's other holds are reconsidered at once.
    #[test]
    fn a_freed_profile_reconsiders_the_providers_other_holds() {
        let now = 1_000_000;
        let mut schedule = Schedule::default();
        let (early, late, other) = (
            AgentId::generate(),
            AgentId::generate(),
            AgentId::generate(),
        );
        schedule.start(early.clone(), now + HOUR, "openai".into(), now);
        schedule.start(late.clone(), now + DAY, "openai".into(), now);
        schedule.start(other.clone(), now + HOUR, "google".into(), now);
        for id in schedule.take_due(now) {
            waiting(&mut schedule, &id, now);
        }
        schedule.plan(now, |_| Vec::new());
        assert_eq!(schedule.next_wake(), Some(now + HOUR));
        let due = schedule.take_due(now + HOUR);
        assert_eq!(due.len(), 3, "both providers have a reset passing");
        schedule.settle(&early, Outcome::Ended { freed: true }, now + HOUR);
        schedule.settle(&other, Outcome::Ended { freed: true }, now + HOUR);
        assert_eq!(schedule.take_due(now + HOUR), std::slice::from_ref(&late));
        waiting(&mut schedule, &late, now + HOUR);
        schedule.plan(now + HOUR, |_| Vec::new());
        assert_eq!(schedule.next_wake(), Some(now + DAY));
        assert_eq!(schedule.holds.len(), 1);
    }

    /// A new hold with an earlier reset moves the provider's wake for holds
    /// already waiting.
    #[test]
    fn a_new_earlier_reset_moves_the_providers_wake() {
        let now = 1_000_000;
        let mut schedule = Schedule::default();
        let (waiting_id, new_id) = (AgentId::generate(), AgentId::generate());
        schedule.start(waiting_id.clone(), now + DAY, "openai".into(), now);
        schedule.take_due(now);
        waiting(&mut schedule, &waiting_id, now);
        schedule.plan(now, |_| Vec::new());
        schedule.start(new_id.clone(), now + HOUR, "openai".into(), now + 1);
        assert_eq!(schedule.take_due(now + 1), std::slice::from_ref(&new_id));
        waiting(&mut schedule, &new_id, now + 1);
        assert_eq!(schedule.next_wake(), Some(now + HOUR));
        let due: HashSet<AgentId> = schedule.take_due(now + HOUR).into_iter().collect();
        assert_eq!(due, HashSet::from([waiting_id, new_id]));
    }

    #[test]
    fn the_worker_wakes_for_an_event_or_at_the_next_wake() {
        let shared = Shared::default();
        let id = AgentId::generate();
        let start = now_ms();
        let (fresh, changed) = wait(&shared, Some(start + 30));
        assert!(fresh.is_empty());
        assert_eq!(changed, Some(Vec::new()));
        assert!(now_ms() >= start + 30, "slept until the wake");
        std::thread::scope(|scope| {
            scope.spawn(|| {
                std::thread::sleep(Duration::from_millis(20));
                shared.inbox().report(&id, start + DAY, now_ms());
                shared.ready.notify_one();
            });
            let (fresh, _) = wait(&shared, None);
            assert_eq!(fresh, std::slice::from_ref(&id));
        });
        shared.inbox().all_changed = true;
        shared.inbox().changed.insert("openai".into());
        assert_eq!(wait(&shared, None).1, None, "every provider changed");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn the_watch_reports_added_profiles_and_cleared_resets() {
        use std::sync::mpsc;
        static EVENTS: Mutex<Option<mpsc::Sender<Option<String>>>> = Mutex::new(None);
        fn record(provider: Option<&str>) {
            if let Some(sender) = EVENTS.lock().unwrap().as_ref() {
                let _ = sender.send(provider.map(str::to_owned));
            }
        }
        let (sender, events) = mpsc::channel();
        *EVENTS.lock().unwrap() = Some(sender);
        let dir = crate::test_tempdir();
        let profiles = dir.path().join("profiles");
        std::fs::create_dir_all(profiles.join("openai/default")).unwrap();
        std::fs::write(profiles.join("openai/default").join(RESET_MARKER), "1\n").unwrap();
        let watched = profiles.clone();
        std::thread::spawn(move || watch::run(&watched, record));
        let next = || events.recv_timeout(Duration::from_secs(5)).unwrap();
        // The watch is armed once a probe event comes through.
        loop {
            std::fs::create_dir(profiles.join("openai/probe")).unwrap();
            if events.recv_timeout(Duration::from_millis(200)).is_ok() {
                break;
            }
            std::fs::remove_dir(profiles.join("openai/probe")).unwrap();
        }
        while events.recv_timeout(Duration::from_millis(200)).is_ok() {}
        // A recorded reset written or a staging directory made is no event.
        std::fs::write(profiles.join("openai/default").join(RESET_MARKER), "2\n").unwrap();
        std::fs::create_dir(profiles.join("openai/.signin-staging-1")).unwrap();
        std::fs::remove_file(profiles.join("openai/default").join(RESET_MARKER)).unwrap();
        assert_eq!(next(), Some("openai".into()), "a cleared reset");
        std::fs::rename(
            profiles.join("openai/.signin-staging-1"),
            profiles.join("openai/second"),
        )
        .unwrap();
        assert_eq!(next(), Some("openai".into()), "a signed-in profile");
        std::fs::create_dir_all(profiles.join("google/default")).unwrap();
        assert_eq!(next(), Some("google".into()), "a new provider");
        // Its profile may report once more, depending on when it was watched.
        while let Ok(event) = events.recv_timeout(Duration::from_millis(200)) {
            assert_eq!(event, Some("google".into()));
        }
        std::fs::write(profiles.join("google/default").join(RESET_MARKER), "3\n").unwrap();
        std::fs::remove_file(profiles.join("google/default").join(RESET_MARKER)).unwrap();
        assert_eq!(
            next(),
            Some("google".into()),
            "a profile of a new provider is watched"
        );
        assert!(events.recv_timeout(Duration::from_millis(200)).is_err());
        *EVENTS.lock().unwrap() = None;
    }
}
