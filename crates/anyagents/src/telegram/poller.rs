//! The Telegram poll: one long-polling thread per channel.
//!
//! Each bot has its own thread, so a bot with nothing to say never delays
//! another bot's update. A supervisor thread keeps that set in step with the
//! store: a channel added while the server runs gets its thread, and a channel
//! removed has its thread stopped. Dropping the poller stops every thread and
//! waits for the call each one is in.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyagents_core::{Channel, Store};

use super::channel::{LONG_POLL_S, poll_channel_once};
use anyagents_core::FileCredentialStore;

/// The poll timeout used once a shutdown has been asked for, so the call in
/// hand comes back promptly instead of holding the join for a full long poll.
const SHUTDOWN_POLL_S: u64 = 1;

/// How often the supervisor looks for channels that were added or removed.
const RECONCILE_EVERY: Duration = Duration::from_secs(2);

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

    /// Ask every thread to stop and wait for them. Called at server shutdown.
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
            let store = Arc::clone(&store);
            let dir = data_dir.clone();
            let channel_id = channel.id.to_string();
            let started = std::thread::Builder::new()
                .name(format!("anyagents-telegram-{id}"))
                .spawn(move || run_channel(store, dir, channel_id, flag));
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

    // Shutdown: every channel's thread stops and is waited for.
    let running = std::mem::take(&mut *lock(&bots));
    for (_, bot) in running {
        bot.stop.store(true, Ordering::SeqCst);
        let _ = bot.handle.join();
    }
}

/// One channel's own loop: long-poll it, and never another channel's.
fn run_channel(store: Arc<Store>, data_dir: std::path::PathBuf, id: String, stop: Arc<AtomicBool>) {
    let secrets = FileCredentialStore::new(&data_dir);
    while !stop.load(Ordering::SeqCst) {
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
        // The timeout is capped so a shutdown never waits out a full long poll:
        // Telegram answers at once when the timeout is short.
        let timeout = if stop.load(Ordering::SeqCst) {
            SHUTDOWN_POLL_S
        } else {
            LONG_POLL_S
        };
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            poll_channel_once(&store, &secrets, &channel, timeout)
        }));
        // A failed poll waits a moment; a good one asks again at once.
        if !matches!(outcome, Ok(Ok(()))) {
            sleep_a_moment(&stop);
        }
    }
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

fn sleep_a_moment(flag: &AtomicBool) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        if flag.load(Ordering::SeqCst) {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}
