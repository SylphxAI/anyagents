//! The Telegram poll: one long-polling thread per channel.
//!
//! Each bot has its own thread, so a bot with nothing to say never delays
//! another bot's update. A supervisor thread keeps that set in step with the
//! store: a channel added while the server runs gets its thread, and a channel
//! removed has its thread stopped. Dropping the poller stops every thread and
//! waits for the call each one is in.
//!
//! Nothing here wakes on a clock to look for work. The supervisor and every
//! idle bot wait on one signal, raised when a Telegram binding changes in this
//! process, when an agent bound to a bot changes its lifecycle (the store's
//! agent wake), when a local receipt is left pending, and when the poller
//! stops. A change no event announces, such as another process writing the
//! store directly, is seen within [`SAFETY_NET`].
//!
//! A long poll cannot be cancelled from this process: it ends when Telegram
//! answers, or when another process calls the same bot. Shutdown therefore ends
//! every poll rather than abandoning it: it sends `getUpdates` to each bot with
//! the shortest timeout and no offset, which makes Telegram release the poll the
//! bot holds and answer both calls at once. The held poll comes back with any
//! update it carried, the last offset is written, and only then does the server
//! stop. A bot that does not answer within the bound keeps the stopping process
//! in its poller join: a poller left behind would be a second poller for the
//! same bot beside the next server's, so the handoff is never declared finished
//! early.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyagents_core::{AgentId, Channel, ChannelId, FileCredentialStore, Store, Wake};

use super::channel::{
    CHANGES, LONG_POLL_S, Signal, changed, delete_webhook, ensure_webhook, poll_channel_once,
    reconcile_channel_groups,
};

/// The longest the supervisor or an idle bot waits with no event. It only
/// bounds how late a change no event announced is seen.
const SAFETY_NET: Duration = Duration::from_secs(60);

/// While a local receipt is still pending, its projection is retried this
/// often.
const RECEIPT_RETRY: Duration = Duration::from_secs(2);

/// A failed poll or registration waits this long before it is tried again.
const RETRY_AFTER_FAILURE: Duration = Duration::from_secs(2);

/// A bot pushed by webhook reconciles its groups this often.
const WEBHOOK_RECONCILE_EVERY: Duration = Duration::from_secs(30);

/// The timeout the shutdown's own call carries, so the held long poll and this
/// one both come back now that Telegram has answered them.
const RELEASE_POLL_S: u64 = 1;

/// A running poller. Dropping it stops every thread and waits for them.
pub struct Poller {
    stop: Arc<AtomicBool>,
    supervisors: Mutex<Vec<JoinHandle<()>>>,
}

/// One channel's own polling thread.
struct Bot {
    stop: Arc<AtomicBool>,
    handle: JoinHandle<()>,
}

fn should_replace_worker(id: &str, wanted: &[String], bot: &Bot) -> bool {
    !wanted.iter().any(|wanted| wanted == id) || bot.handle.is_finished()
}

impl Poller {
    pub fn start(store: Arc<Store>, data_dir: std::path::PathBuf) -> Result<Self, String> {
        let stop = Arc::new(AtomicBool::new(false));
        let bots: Arc<Mutex<HashMap<String, Bot>>> = Arc::new(Mutex::new(HashMap::new()));
        let bound: Arc<Mutex<HashSet<AgentId>>> = Arc::default();
        forward_lifecycle_wakes(&store, Arc::clone(&bound), Arc::clone(&stop), &CHANGES)?;
        let running = Arc::clone(&bots);
        let flag = Arc::clone(&stop);
        let handle = std::thread::Builder::new()
            .name("anyagents-telegram".into())
            .spawn(move || supervise(store, data_dir, flag, running, bound, &CHANGES))
            .map_err(|err| err.to_string())?;
        Ok(Self {
            stop,
            supervisors: Mutex::new(vec![handle]),
        })
    }

    /// Signal every polling thread to stop so that when [`release_every_bot`]
    /// interrupts an in-flight long poll, the thread commits its offset and
    /// exits instead of starting a new 30-second poll.
    pub fn signal_stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
        changed();
    }

    /// Stop every bot's poll and wait for its last offset to be written. The
    /// server releases every held poll first (see [`release_every_bot`]), so
    /// each thread's call comes back at once and its state is committed before
    /// the join returns. The join has no timeout: a poller left behind would be
    /// a second poller for the same bot beside the next server's.
    pub fn stop(&self) {
        self.signal_stop();
        let handles = lock(&self.supervisors).drain(..).collect::<Vec<_>>();
        for handle in handles {
            let _ = handle.join();
        }
    }
}

impl Drop for Poller {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Raise the signal when an agent bound to a Telegram channel changes its
/// lifecycle: the store wakes subscribers on every such change, made in this
/// process or announced by another one on the wake socket.
fn forward_lifecycle_wakes(
    store: &Store,
    bound: Arc<Mutex<HashSet<AgentId>>>,
    stop: Arc<AtomicBool>,
    signal: &'static Signal,
) -> Result<(), String> {
    let wakes = store.subscribe();
    std::thread::Builder::new()
        .name("anyagents-telegram-wake".into())
        .spawn(move || {
            while !stop.load(Ordering::SeqCst) {
                match wakes.recv_timeout(SAFETY_NET) {
                    Ok(Wake::Agent { agent_id }) if lock(&bound).contains(&agent_id) => {
                        signal.raise()
                    }
                    Ok(_) | Err(mpsc::RecvTimeoutError::Timeout) => {}
                    Err(mpsc::RecvTimeoutError::Disconnected) => return,
                }
            }
        })
        .map(drop)
        .map_err(|err| err.to_string())
}

/// Keep one polling thread alive for every Telegram channel in the store.
fn supervise(
    store: Arc<Store>,
    data_dir: std::path::PathBuf,
    stop: Arc<AtomicBool>,
    bots: Arc<Mutex<HashMap<String, Bot>>>,
    bound: Arc<Mutex<HashSet<AgentId>>>,
    signal: &'static Signal,
) {
    while !stop.load(Ordering::SeqCst) {
        let seen = signal.generation();
        let receipts_pending = match super::channel::replay_local_receipts(&store) {
            Ok(_) => false,
            Err(err) => {
                eprintln!("Telegram local receipt replay pending: {err}");
                true
            }
        };
        let retry = if receipts_pending {
            RECEIPT_RETRY
        } else {
            SAFETY_NET
        };
        // The bound agents are published before their lifecycles are read, so
        // a change after that read raises the signal and none falls between.
        let census = telegram_bots(&store).and_then(|(bots, agents)| {
            *lock(&bound) = agents;
            to_poll(&store, bots)
        });
        let channels = match census {
            Ok(channels) => channels,
            Err(err) => {
                eprintln!("Telegram channel census unavailable: {err}");
                signal.pause(RETRY_AFTER_FAILURE, &[&stop]);
                continue;
            }
        };
        let wanted: Vec<String> = channels.iter().map(|c| c.id.to_string()).collect();

        // Stop a channel that is gone: it is not in the store any more.
        let gone: Vec<String> = {
            let running = lock(&bots);
            running
                .keys()
                .filter(|id| {
                    running
                        .get(*id)
                        .is_some_and(|bot| should_replace_worker(id, &wanted, bot))
                })
                .cloned()
                .collect()
        };
        for id in gone {
            if let Some(bot) = lock(&bots).remove(&id) {
                bot.stop.store(true, Ordering::SeqCst);
                signal.raise();
                let _ = bot.handle.join();
            }
        }

        // Start a channel that is new.
        for channel in channels {
            let id = channel.id.to_string();
            if lock(&bots).contains_key(&id) {
                continue;
            }
            let bot_stop = Arc::new(AtomicBool::new(false));
            let flag = Arc::clone(&bot_stop);
            let global_stop = Arc::clone(&stop);
            let store = Arc::clone(&store);
            let dir = data_dir.clone();
            let started = std::thread::Builder::new()
                .name(format!("anyagents-telegram-{id}"))
                .spawn(move || run_channel(store, dir, channel.id, flag, global_stop, signal));
            if let Ok(handle) = started {
                lock(&bots).insert(
                    id,
                    Bot {
                        stop: bot_stop,
                        handle,
                    },
                );
            }
        }

        // Wait for the next change, but wake at once when the server is
        // stopping.
        signal.wait(seen, retry, &[&stop]);
    }

    // Shutdown: every bot's poll ends and its last offset is written before its
    // thread returns. The stop was released through Telegram already, so this
    // join returns as soon as that call has been stored.
    let running = std::mem::take(&mut *lock(&bots));
    for (_, bot) in running {
        bot.stop.store(true, Ordering::SeqCst);
        signal.raise();
        let _ = bot.handle.join();
    }
}

fn configured_webhook_base() -> Option<String> {
    std::env::var("ANYAGENTS_WEBHOOK_URL")
        .ok()
        .map(|v| v.trim().trim_end_matches('/').to_string())
        .filter(|v| !v.is_empty())
}

static WEBHOOK_READY_CACHE: Mutex<Option<(String, Instant, bool)>> = Mutex::new(None);

fn webhook_origin_ready(base: &str) -> bool {
    if std::env::var_os("ANYAGENTS_TELEGRAM_API").is_some() {
        return true;
    }
    let normalized = base.trim_end_matches('/').to_string();
    {
        let cached = lock(&WEBHOOK_READY_CACHE);
        if let Some((ref cached_base, checked_at, ok)) = *cached {
            let ttl = if ok {
                Duration::from_secs(30)
            } else {
                Duration::from_secs(5)
            };
            if cached_base == &normalized && checked_at.elapsed() < ttl {
                return ok;
            }
        }
    }
    let url = format!("{normalized}/v1/webhooks/healthz");
    let agent = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(3)))
        .http_status_as_error(false)
        .max_redirects(0)
        .max_redirects_will_error(false)
        .build()
        .new_agent();
    let ok = agent
        .get(&url)
        .call()
        .map(|resp| resp.status().as_u16() == 200)
        .unwrap_or(false);
    *lock(&WEBHOOK_READY_CACHE) = Some((normalized, Instant::now(), ok));
    ok
}

/// One channel's own loop: register its webhook when a live webhook origin is
/// configured, otherwise long-poll it.
fn run_channel(
    store: Arc<Store>,
    data_dir: std::path::PathBuf,
    id: ChannelId,
    stop: Arc<AtomicBool>,
    global_stop: Arc<AtomicBool>,
    signal: &'static Signal,
) {
    let stops = [&*stop, &*global_stop];
    let secrets = FileCredentialStore::new(&data_dir);
    let mut webhook_registered_for: Option<String> = None;
    let mut groups_reconciled_at: Option<Instant> = None;
    while !stop.load(Ordering::SeqCst) && !global_stop.load(Ordering::SeqCst) {
        let seen = signal.generation();
        // The channel is re-read each pass: its saved offset and chats are what
        // this thread's own writes produced, so a stale copy would write an old
        // state back over them.
        let channel = match store.channel(&id) {
            Ok(channel) => channel,
            Err(err) => {
                eprintln!("Telegram channel read unavailable: {err}");
                signal.pause(RETRY_AFTER_FAILURE, &stops);
                continue;
            }
        };
        let Some(channel) = channel else {
            return;
        };
        let active = match channel_has_running_agent(&store, &channel) {
            Ok(active) => active,
            Err(err) => {
                eprintln!("Telegram agent lifecycle unavailable: {err}");
                signal.pause(RETRY_AFTER_FAILURE, &stops);
                continue;
            }
        };
        if !active {
            if webhook_registered_for.take().is_some() {
                let _ = delete_webhook(&secrets, &channel);
            }
            // Asleep until a bound agent's lifecycle or the binding changes.
            signal.wait(seen, SAFETY_NET, &stops);
            continue;
        }
        if let Some(base) = configured_webhook_base()
            && webhook_origin_ready(&base)
        {
            if webhook_registered_for.as_deref() != Some(base.as_str()) {
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    ensure_webhook(&store, &secrets, &channel, &base)
                }));
                if matches!(outcome, Ok(Ok(()))) {
                    webhook_registered_for = Some(base);
                } else {
                    signal.pause(RETRY_AFTER_FAILURE, &stops);
                    continue;
                }
            }
            // A lifecycle change wakes this loop early so an archived or
            // paused agent's webhook goes at once; the groups themselves are
            // reconciled on their own cadence.
            if groups_reconciled_at.is_none_or(|at| at.elapsed() >= WEBHOOK_RECONCILE_EVERY) {
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    reconcile_channel_groups(&store, &secrets, &channel);
                }));
                groups_reconciled_at = Some(Instant::now());
            }
            let since = groups_reconciled_at.map_or(Duration::ZERO, |at| at.elapsed());
            signal.wait(seen, WEBHOOK_RECONCILE_EVERY.saturating_sub(since), &stops);
            continue;
        }
        webhook_registered_for = None;
        groups_reconciled_at = None;
        if channel.agents.is_empty() {
            // A bot no agent holds yet has nobody to poll for: wait for the
            // binding instead of asking again at once.
            signal.wait(seen, SAFETY_NET, &stops);
            continue;
        }
        // The timeout is capped so a shutdown never waits out a full long poll:
        // Telegram answers at once when the timeout is short.
        let timeout = if stop.load(Ordering::SeqCst) || global_stop.load(Ordering::SeqCst) {
            RELEASE_POLL_S
        } else {
            LONG_POLL_S
        };
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            poll_channel_once(&store, &secrets, &channel, timeout)
        }));
        if let Ok(Err(ref err)) = outcome
            && err.to_string().contains("Conflict: webhook is active")
        {
            let _ = delete_webhook(&secrets, &channel);
        }
        // A failed poll waits a moment; a good one asks again at once.
        if !matches!(outcome, Ok(Ok(()))) {
            signal.pause(RETRY_AFTER_FAILURE, &stops);
        }
    }
    if webhook_registered_for.is_some()
        && let Ok(Some(channel)) = store.channel(&id)
    {
        let _ = delete_webhook(&secrets, &channel);
    }
}

/// End every bot poll that is waiting on Telegram, and wait for each bot to
/// write its last offset. Telegram releases a held long poll when the next call
/// to that bot arrives, so each bot gets one call with the shortest timeout; the
/// held poll and this one are then both answered. An error the bot itself
/// reports (a competing poller, a dead token) is passed on: it is not hidden.
/// An update the bot had already routed before the stop is stored on the way
/// out, so the release also waits for the last offset to be committed.
pub fn release_every_bot(
    store: &Store,
    data_dir: &std::path::Path,
    within: Duration,
) -> Result<(), String> {
    if let Some(base) = configured_webhook_base()
        && webhook_origin_ready(&base)
    {
        return Ok(());
    }
    let channels = channels_to_poll(store).map_err(|err| err.to_string())?;
    let (done_tx, done_rx) = mpsc::channel();
    let mut waiting = 0;
    for channel in channels {
        if !channel_has_running_agent(store, &channel).map_err(|err| err.to_string())? {
            continue;
        }
        let secrets = FileCredentialStore::new(data_dir);
        let done = done_tx.clone();
        let id = channel.id.to_string();
        // One pass per bot, on its own thread: every bot is released at the
        // same time, not one after another.
        let data_dir = data_dir.to_path_buf();
        std::thread::spawn(move || {
            let store = match Store::open(data_dir.join("state.db")) {
                Ok(store) => store,
                Err(err) => {
                    let _ = done.send((id, Err(err.to_string())));
                    return;
                }
            };
            let result = poll_channel_once(&store, &secrets, &channel, RELEASE_POLL_S);
            let _ = done.send((id, result.map_err(|err| err.to_string())));
        });
        waiting += 1;
    }
    drop(done_tx);
    let deadline = Instant::now() + within;
    while waiting > 0 {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return Err(format!(
                "{waiting} bot(s) did not release their poll within {}s",
                within.as_secs()
            ));
        }
        match done_rx.recv_timeout(left) {
            Ok((_, Err(reason))) => return Err(reason),
            Ok((_, Ok(()))) => waiting -= 1,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
    Ok(())
}

fn channel_has_running_agent(
    store: &Store,
    channel: &Channel,
) -> Result<bool, anyagents_core::Error> {
    channel_has_eligible_agent(store, channel, false)
}

fn channel_has_eligible_agent(
    store: &Store,
    channel: &Channel,
    webhook: bool,
) -> Result<bool, anyagents_core::Error> {
    let mut active = channel.agents.is_empty();
    // A partial lifecycle population cannot authorize removal of any worker.
    for id in &channel.agents {
        let agent = store.agent(id)?;
        active |= agent.state != "archived" && (webhook || agent.state != "paused");
    }
    Ok(active)
}

/// Every Telegram bot with a credential, and every agent bound to one: a
/// lifecycle change of any of them can change which bots are polled.
fn telegram_bots(store: &Store) -> Result<(Vec<Channel>, HashSet<AgentId>), anyagents_core::Error> {
    let bots: Vec<Channel> = store
        .list_channels()?
        .into_iter()
        .filter(|channel| channel.kind == "telegram" && channel.credential_ref.is_some())
        .collect();
    let agents = bots
        .iter()
        .flat_map(|channel| channel.agents.iter().cloned())
        .collect();
    Ok((bots, agents))
}

/// The bots to poll now. A census is authoritative only when every required
/// read succeeds.
fn to_poll(store: &Store, bots: Vec<Channel>) -> Result<Vec<Channel>, anyagents_core::Error> {
    let webhook = configured_webhook_base().is_some();
    let mut channels = Vec::new();
    for channel in bots {
        if channel_has_eligible_agent(store, &channel, webhook)? {
            channels.push(channel);
        }
    }
    Ok(channels)
}

fn channels_to_poll(store: &Store) -> Result<Vec<Channel>, anyagents_core::Error> {
    to_poll(store, telegram_bots(store)?.0)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poison| poison.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> (tempfile::TempDir, Store, Channel, anyagents_core::Agent) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(anyagents_core::NewAgent::new("helper", "codex"))
            .unwrap();
        let channel = store
            .create_channel(anyagents_core::NewChannel {
                kind: "telegram".into(),
                identity: None,
                config: "{}".into(),
                credential_ref: Some("channels/helper".into()),
            })
            .unwrap();
        store.attach_channel(&channel.id, &agent.id).unwrap();
        let channel = store.list_channels().unwrap().remove(0);
        (dir, store, channel, agent)
    }
    #[test]
    fn failed_census_is_not_an_empty_population() {
        let (dir, store, _, _) = fixture();
        assert_eq!(channels_to_poll(&store).unwrap().len(), 1);
        let conn = rusqlite::Connection::open(dir.path().join("state.db")).unwrap();
        conn.execute_batch("ALTER TABLE channels RENAME TO hidden_channels")
            .unwrap();
        assert!(channels_to_poll(&store).is_err());
        conn.execute_batch("ALTER TABLE hidden_channels RENAME TO channels")
            .unwrap();
        assert_eq!(channels_to_poll(&store).unwrap().len(), 1);
        conn.execute_batch("DELETE FROM channel_agents; DELETE FROM channels")
            .unwrap();
        assert!(channels_to_poll(&store).unwrap().is_empty());
    }
    #[test]
    fn lifecycle_read_error_cannot_confirm_inactivity() {
        let (dir, store, channel, agent) = fixture();
        assert!(channel_has_running_agent(&store, &channel).unwrap());
        store.set_agent_state(&agent.id, "paused").unwrap();
        assert!(!channel_has_running_agent(&store, &channel).unwrap());
        store.set_agent_state(&agent.id, "running").unwrap();
        let conn = rusqlite::Connection::open(dir.path().join("state.db")).unwrap();
        conn.execute_batch("ALTER TABLE agents RENAME TO hidden_agents")
            .unwrap();
        assert!(channel_has_running_agent(&store, &channel).is_err());
        assert!(channels_to_poll(&store).is_err());
    }
    #[test]
    fn completed_worker_is_replaced_and_live_wanted_worker_is_preserved() {
        let (tx, rx) = std::sync::mpsc::channel();
        let bot = Bot {
            stop: Arc::new(AtomicBool::new(false)),
            handle: std::thread::spawn(move || {
                rx.recv().unwrap();
            }),
        };
        let wanted = vec!["channel".to_string()];
        assert!(!should_replace_worker("channel", &wanted, &bot));
        assert!(should_replace_worker("removed", &wanted, &bot));
        tx.send(()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        while !bot.handle.is_finished() && Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert!(should_replace_worker("channel", &wanted, &bot));
        bot.handle.join().unwrap();
    }

    fn private_signal() -> &'static Signal {
        Box::leak(Box::new(Signal::new()))
    }

    fn unbound_bot(store: &Store, name: &str) -> Channel {
        store
            .create_channel(anyagents_core::NewChannel {
                kind: "telegram".into(),
                identity: None,
                config: "{}".into(),
                credential_ref: Some(format!("channels/{name}")),
            })
            .unwrap()
    }

    fn wait_until(what: &str, done: impl Fn() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !done() {
            assert!(Instant::now() < deadline, "{what}");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn a_wait_ends_on_a_raise_on_a_stop_or_at_its_timeout() {
        let signal = private_signal();
        let stop = AtomicBool::new(false);
        let started = Instant::now();
        signal.wait(signal.generation(), Duration::from_millis(50), &[&stop]);
        assert!(started.elapsed() >= Duration::from_millis(50));

        let seen = signal.generation();
        std::thread::scope(|scope| {
            scope.spawn(|| {
                std::thread::sleep(Duration::from_millis(20));
                signal.raise();
            });
            let started = Instant::now();
            signal.wait(seen, Duration::from_secs(30), &[&stop]);
            assert!(started.elapsed() < Duration::from_secs(5));
        });

        std::thread::scope(|scope| {
            scope.spawn(|| {
                std::thread::sleep(Duration::from_millis(20));
                stop.store(true, Ordering::SeqCst);
                signal.raise();
            });
            let started = Instant::now();
            signal.pause(Duration::from_secs(30), &[&stop]);
            assert!(started.elapsed() < Duration::from_secs(5));
        });
    }

    #[test]
    fn the_supervisor_looks_at_the_store_again_only_when_signalled() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        // Bots no agent holds yet: their threads start and then wait, so the
        // test makes no Telegram call.
        let first = unbound_bot(&store, "first");
        let signal = private_signal();
        let stop = Arc::new(AtomicBool::new(false));
        let bots: Arc<Mutex<HashMap<String, Bot>>> = Arc::default();
        let supervisor = {
            let (store, stop, bots) = (Arc::clone(&store), Arc::clone(&stop), Arc::clone(&bots));
            let dir = dir.path().to_path_buf();
            std::thread::spawn(move || supervise(store, dir, stop, bots, Arc::default(), signal))
        };
        wait_until("the first bot never started", || {
            lock(&bots).contains_key(first.id.as_str())
        });

        // A channel written without a signal is not seen by any clock.
        let second = unbound_bot(&store, "second");
        std::thread::sleep(Duration::from_millis(500));
        assert_eq!(lock(&bots).len(), 1);

        signal.raise();
        wait_until("the signalled bot never started", || {
            lock(&bots).contains_key(second.id.as_str())
        });

        stop.store(true, Ordering::SeqCst);
        signal.raise();
        supervisor.join().unwrap();
        assert!(lock(&bots).is_empty());
    }

    #[test]
    fn only_a_bound_agents_lifecycle_raises_the_signal() {
        let (_dir, store, _, bound_agent) = fixture();
        let other = store
            .create_agent(anyagents_core::NewAgent::new("other", "codex"))
            .unwrap();
        // Running before the forwarder subscribes, so mail changes no state.
        store.set_agent_state(&bound_agent.id, "running").unwrap();
        let signal = private_signal();
        let stop = Arc::new(AtomicBool::new(false));
        let bound = Arc::new(Mutex::new(HashSet::from([bound_agent.id.clone()])));
        forward_lifecycle_wakes(&store, bound, Arc::clone(&stop), signal).unwrap();

        let seen = signal.generation();
        store.set_agent_state(&other.id, "paused").unwrap();
        store
            .receive(&bound_agent.id, anyagents_core::Incoming::new("mail"))
            .unwrap();
        signal.wait(seen, Duration::from_millis(300), &[]);
        assert_eq!(signal.generation(), seen);

        store.set_agent_state(&bound_agent.id, "paused").unwrap();
        signal.wait(seen, Duration::from_secs(5), &[]);
        assert_eq!(signal.generation(), seen + 1);
        stop.store(true, Ordering::SeqCst);
    }
}
