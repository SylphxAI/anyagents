//! The Telegram poll: one long-polling thread per channel.
//!
//! Each bot has its own thread, so a bot with nothing to say never delays
//! another bot's update. A supervisor thread keeps that set in step with the
//! store: a channel added while the server runs gets its thread, and a channel
//! removed has its thread stopped. Dropping the poller stops every thread and
//! waits for the call each one is in.
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

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyagents_core::{Channel, FileCredentialStore, Store};

use super::channel::{LONG_POLL_S, delete_webhook, ensure_webhook, poll_channel_once};

/// How often the supervisor looks for channels that were added or removed.
const RECONCILE_EVERY: Duration = Duration::from_secs(2);

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

impl Poller {
    pub fn start(store: Arc<Store>, data_dir: std::path::PathBuf) -> Result<Self, String> {
        let stop = Arc::new(AtomicBool::new(false));
        let bots: Arc<Mutex<HashMap<String, Bot>>> = Arc::new(Mutex::new(HashMap::new()));
        let running = Arc::clone(&bots);
        let flag = Arc::clone(&stop);
        let handle = std::thread::Builder::new()
            .name("anyagents-telegram".into())
            .spawn(move || supervise(store, data_dir, flag, running))
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
    }

    /// Stop every bot's poll and wait for its last offset to be written. The
    /// server releases every held poll first (see [`release_every_bot`]), so
    /// each thread's call comes back at once and its state is committed before
    /// the join returns. The join has no timeout: a poller left behind would be
    /// a second poller for the same bot beside the next server's.
    pub fn stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
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

/// Keep one polling thread alive for every Telegram channel in the store.
fn supervise(
    store: Arc<Store>,
    data_dir: std::path::PathBuf,
    stop: Arc<AtomicBool>,
    bots: Arc<Mutex<HashMap<String, Bot>>>,
) {
    while !stop.load(Ordering::SeqCst) {
        let channels = channels_to_poll(&store);
        let wanted: Vec<String> = channels.iter().map(|c| c.id.to_string()).collect();

        // Stop a channel that is gone: it is not in the store any more.
        let gone: Vec<String> = {
            let running = lock(&bots);
            running
                .keys()
                .filter(|id| !wanted.contains(id))
                .cloned()
                .collect()
        };
        for id in gone {
            if let Some(bot) = lock(&bots).remove(&id) {
                bot.stop.store(true, Ordering::SeqCst);
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
            let channel_id = channel.id.to_string();
            let started = std::thread::Builder::new()
                .name(format!("anyagents-telegram-{id}"))
                .spawn(move || run_channel(store, dir, channel_id, flag, global_stop));
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

        // Wait a moment, but wake at once when the server is stopping.
        let deadline = Instant::now() + RECONCILE_EVERY;
        while Instant::now() < deadline && !stop.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    // Shutdown: every bot's poll ends and its last offset is written before its
    // thread returns. The stop was released through Telegram already, so this
    // join returns as soon as that call has been stored.
    let running = std::mem::take(&mut *lock(&bots));
    for (_, bot) in running {
        bot.stop.store(true, Ordering::SeqCst);
        let _ = bot.handle.join();
    }
}

fn configured_webhook_base() -> Option<String> {
    std::env::var("ANYAGENTS_WEBHOOK_URL")
        .ok()
        .map(|v| v.trim().trim_end_matches('/').to_string())
        .filter(|v| !v.is_empty())
}

fn webhook_origin_ready(base: &str) -> bool {
    if std::env::var_os("ANYAGENTS_TELEGRAM_API").is_some() {
        return true;
    }
    let url = format!("{}/v1/webhooks/healthz", base.trim_end_matches('/'));
    let agent = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(3)))
        .http_status_as_error(false)
        .max_redirects(0)
        .max_redirects_will_error(false)
        .build()
        .new_agent();
    agent
        .get(&url)
        .call()
        .map(|resp| resp.status().as_u16() == 200)
        .unwrap_or(false)
}

/// One channel's own loop: register its webhook when a live webhook origin is
/// configured, otherwise long-poll it.
fn run_channel(
    store: Arc<Store>,
    data_dir: std::path::PathBuf,
    id: String,
    stop: Arc<AtomicBool>,
    global_stop: Arc<AtomicBool>,
) {
    let secrets = FileCredentialStore::new(&data_dir);
    let mut webhook_registered_for: Option<String> = None;
    while !stop.load(Ordering::SeqCst) && !global_stop.load(Ordering::SeqCst) {
        // The channel is re-read each pass: its saved offset and chats are what
        // this thread's own writes produced, so a stale copy would write an old
        // state back over them.
        let channel = store
            .list_channels()
            .unwrap_or_default()
            .into_iter()
            .find(|channel| channel.id.to_string() == id);
        let Some(channel) = channel else {
            return;
        };
        if let Some(base) = configured_webhook_base()
            && webhook_origin_ready(&base)
        {
            if webhook_registered_for.as_deref() != Some(base.as_str()) {
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    ensure_webhook(&store, &secrets, &channel, &base)
                }));
                if matches!(outcome, Ok(Ok(()))) {
                    webhook_registered_for = Some(base);
                }
            }
            sleep_a_moment(&stop, &global_stop);
            continue;
        }
        webhook_registered_for = None;
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
            sleep_a_moment(&stop, &global_stop);
        }
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
    let channels = channels_to_poll(store);
    let (done_tx, done_rx) = mpsc::channel();
    let mut waiting = 0;
    for channel in channels {
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
        match done_rx.recv_timeout(left.min(Duration::from_millis(200))) {
            Ok((_, Err(reason))) => return Err(reason),
            Ok((_, Ok(()))) => waiting -= 1,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
    Ok(())
}

/// The channels this server should be polling: a Telegram channel with a
/// credential, which a bot cannot be polled without.
fn channels_to_poll(store: &Store) -> Vec<Channel> {
    store
        .list_channels()
        .unwrap_or_default()
        .into_iter()
        .filter(|channel| channel.kind == "telegram" && channel.credential_ref.is_some())
        .collect()
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poison| poison.into_inner())
}

fn sleep_a_moment(flag: &AtomicBool, global_stop: &AtomicBool) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        if flag.load(Ordering::SeqCst) || global_stop.load(Ordering::SeqCst) {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}
