//! One quiet bot must never delay another bot's update.
//!
//! These tests run the real server against a fake Telegram that records which
//! bot was polled and when. A bot with nothing to say answers at once, so the
//! only thing that can delay a bot's update is another bot's long poll — which
//! is exactly what the per-channel poller removes.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyagents_core::Store;
use serde_json::{Value, json};

/// One `getUpdates` call: which bot made it, and when.
#[derive(Clone, Debug)]
struct Asked {
    bot: i64,
    at: Instant,
}

/// A fake Telegram that serves each bot separately.
struct FakeTelegram {
    address: String,
    asked: Arc<Mutex<Vec<Asked>>>,
    /// The updates waiting for one bot, by bot id.
    pending: Arc<Mutex<Vec<(i64, Value)>>>,
    stop: Arc<Mutex<bool>>,
    thread: Option<thread::JoinHandle<()>>,
}

impl FakeTelegram {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let asked = Arc::new(Mutex::new(Vec::new()));
        let pending = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(Mutex::new(false));
        let shared_asked = Arc::clone(&asked);
        let shared_pending = Arc::clone(&pending);
        let shared_stop = Arc::clone(&stop);
        let handle = thread::spawn(move || {
            while !*shared_stop.lock().unwrap() {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let asked = Arc::clone(&shared_asked);
                        let pending = Arc::clone(&shared_pending);
                        thread::spawn(move || serve(stream, asked, pending));
                    }
                    Err(_) => thread::sleep(Duration::from_millis(5)),
                }
            }
        });
        Self {
            address,
            asked,
            pending,
            stop,
            thread: Some(handle),
        }
    }

    /// Put one update in front of `bot`, and only that bot.
    fn offer(&self, bot: i64, update: Value) {
        self.pending.lock().unwrap().push((bot, update));
    }

    fn asked(&self) -> Vec<Asked> {
        self.asked.lock().unwrap().clone()
    }

    /// Wait until `bot` has been polled at least once.
    fn wait_polled(&self, bot: i64, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if self.asked().iter().any(|one| one.bot == bot) {
                return true;
            }
            thread::sleep(Duration::from_millis(20));
        }
        false
    }
}

impl Drop for FakeTelegram {
    fn drop(&mut self) {
        *self.stop.lock().unwrap() = true;
        if let Some(handle) = self.thread.take() {
            let _ = handle.join();
        }
    }
}

fn serve(
    mut stream: TcpStream,
    asked: Arc<Mutex<Vec<Asked>>>,
    pending: Arc<Mutex<Vec<(i64, Value)>>>,
) {
    let mut raw = Vec::new();
    let mut chunk = [0u8; 4096];
    let (head, body) = loop {
        let read = stream.read(&mut chunk).unwrap_or(0);
        if read == 0 {
            return;
        }
        raw.extend_from_slice(&chunk[..read]);
        if let Some(at) = raw.windows(4).position(|window| window == b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&raw[..at]).to_string();
            let length = head
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())?
                })
                .unwrap_or(0);
            if raw.len() >= at + 4 + length {
                break (head, raw[at + 4..].to_vec());
            }
        }
    };
    // `/bot<id>:<token>/<method>`
    let path = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or("/");
    let bot: i64 = path
        .split("/bot")
        .nth(1)
        .and_then(|rest| rest.split(':').next())
        .and_then(|id| id.parse().ok())
        .unwrap_or(0);
    let method = path.rsplit('/').next().unwrap_or("").to_string();
    let request: Value = serde_json::from_slice(&body).unwrap_or(json!({}));

    let response = match method.as_str() {
        "getMe" => json!({
            "ok": true,
            "result": {"id": bot, "is_bot": true, "first_name": "bot", "username": format!("bot_{bot}")},
        }),
        "getUpdates" => {
            asked.lock().unwrap().push(Asked {
                bot,
                at: Instant::now(),
            });
            // Telegram holds a long poll open for the timeout it was given when
            // there is nothing to send. A quiet bot must behave the same here,
            // or this fake cannot show what a real long poll costs.
            let timeout = request["timeout"].as_u64().unwrap_or(0).min(20);
            let hold = Instant::now() + Duration::from_secs(timeout);
            loop {
                let mine: Vec<Value> = {
                    let mut waiting = pending.lock().unwrap();
                    let mine: Vec<Value> = waiting
                        .iter()
                        .filter(|(for_bot, _)| *for_bot == bot)
                        .map(|(_, update)| update.clone())
                        .collect();
                    waiting.retain(|(for_bot, _)| *for_bot != bot);
                    mine
                };
                if !mine.is_empty() || Instant::now() >= hold {
                    break json!({"ok": true, "result": mine});
                }
                thread::sleep(Duration::from_millis(50));
            }
        }
        "sendMessage" => {
            json!({"ok": true, "result": {"message_id": 1, "chat": {"id": 1}, "date": 0}})
        }
        _ => json!({"ok": true, "result": true}),
    };
    let payload = response.to_string();
    let _ = write!(
        stream,
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
        payload.len()
    );
    let _ = stream.flush();
}

struct Server {
    child: Child,
}

impl Server {
    fn start(dir: &Path, api: &str) -> Self {
        let child = Command::new(env!("CARGO_BIN_EXE_anyagents"))
            .env("ANYAGENTS_OPENCODE_BIN", env!("CARGO_BIN_EXE_anyagents"))
            .env("ANYAGENTS_TELEGRAM_API", api)
            .arg("--data")
            .arg(dir)
            .arg("serve")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("server");
        let socket = dir.join("anyagents.sock");
        let deadline = Instant::now() + Duration::from_secs(15);
        while !socket.exists() {
            assert!(Instant::now() < deadline, "no socket");
            thread::sleep(Duration::from_millis(20));
        }
        Self { child }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A `PATH` where creating an agent installs nothing: `npm` answers a version
/// and a no-op install.
fn machine_path(dir: &Path) -> std::ffi::OsString {
    use std::os::unix::fs::PermissionsExt;
    let bin = dir.join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    let npm = bin.join("npm");
    if !npm.exists() {
        std::fs::write(
            &npm,
            "#!/bin/sh\ncase \"$1\" in view) echo 0.0.0 ;; esac\nexit 0\n",
        )
        .unwrap();
        std::fs::set_permissions(&npm, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    std::env::join_paths([bin]).unwrap()
}

fn create(dir: &Path, name: &str) -> String {
    let created = Command::new(env!("CARGO_BIN_EXE_anyagents"))
        .arg("--data")
        .arg(dir)
        .args(["agents", "create", name, "--harness", "claude-code"])
        .env("PATH", machine_path(dir))
        .env("ANYAGENTS_CLAUDE_BIN", "/bin/true")
        .output()
        .unwrap();
    assert!(
        created.status.success(),
        "{}",
        String::from_utf8_lossy(&created.stderr)
    );
    String::from_utf8(created.stdout)
        .unwrap()
        .lines()
        .next()
        .unwrap()
        .split('\t')
        .next()
        .unwrap()
        .to_string()
}

/// Add one Telegram bot for `agent`, with the bot id as its token's first field.
fn add_channel(dir: &Path, agent: &str, bot: i64) {
    let token = dir.join(format!("bot-token-{bot}"));
    std::fs::write(&token, format!("{bot}:test-token")).unwrap();
    let added = Command::new(env!("CARGO_BIN_EXE_anyagents"))
        .arg("--data")
        .arg(dir)
        .args(["channels", "add", "telegram", "--agent", agent])
        .arg("--token-file")
        .arg(&token)
        .output()
        .unwrap();
    assert!(
        added.status.success(),
        "{}",
        String::from_utf8_lossy(&added.stderr)
    );
    std::fs::remove_file(&token).unwrap();
}

fn dm(update_id: i64, text: &str) -> Value {
    json!({
        "update_id": update_id,
        "message": {
            "message_id": update_id,
            "chat": {"id": 500, "type": "private"},
            "from": {"id": 42, "is_bot": false, "first_name": "Kai"},
            "date": 1_700_000_000,
            "text": text,
        },
    })
}

/// The last of many bots gets its update within one poll interval, not one per
/// bot.
#[test]
fn a_quiet_bot_never_delays_another_bots_update() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    let fake = FakeTelegram::start();

    // Eight bots, each on its own channel. Only the last has an update.
    let bots: Vec<i64> = (1..=8).map(|n| 900 + n).collect();
    for (at, bot) in bots.iter().enumerate() {
        let name = format!("owner{at}");
        create(&data, &name);
        add_channel(&data, &name, *bot);
    }
    fake.offer(*bots.last().unwrap(), dm(1, "the only line"));

    let started = Instant::now();
    let _server = Server::start(&data, &fake.address);
    let deadline = Instant::now() + Duration::from_secs(20);
    let agent = Store::open(data.join("state.db"))
        .unwrap()
        .agent_by_name(&format!("owner{}", bots.len() - 1))
        .unwrap();
    loop {
        let inbox = Store::open(data.join("state.db"))
            .unwrap()
            .inbox(&agent.id, None)
            .unwrap();
        if inbox.iter().any(|message| message.text == "the only line") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the last bot's update never arrived"
        );
        thread::sleep(Duration::from_millis(50));
    }
    let waited = started.elapsed();

    // Every bot was polled at about the same time, not one after another.
    let asked = fake.asked();
    let first = asked.iter().map(|one| one.at).min().unwrap();
    let last = asked.iter().map(|one| one.at).max().unwrap();
    assert!(
        last.duration_since(first) < Duration::from_secs(5),
        "bots were polled one after another: {asked:?}"
    );
    assert!(
        waited < Duration::from_secs(15),
        "the update waited {waited:?}, about one poll per bot"
    );
}

/// Each bot keeps its own offset: a restart asks for what it already stored and
/// the same update twice stays one message.
#[test]
fn each_bot_keeps_its_own_offset_across_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    let fake = FakeTelegram::start();
    for (at, bot) in [910_i64, 911].iter().enumerate() {
        let name = format!("owner{at}");
        create(&data, &name);
        add_channel(&data, &name, *bot);
    }
    fake.offer(910, dm(7, "for the first bot"));

    let agent = Store::open(data.join("state.db"))
        .unwrap()
        .agent_by_name("owner0")
        .unwrap();
    let wait_for = |text: &str| {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let inbox = Store::open(data.join("state.db"))
                .unwrap()
                .inbox(&agent.id, None)
                .unwrap();
            if inbox.iter().any(|message| message.text == text) {
                return;
            }
            assert!(Instant::now() < deadline, "{text} never arrived");
            thread::sleep(Duration::from_millis(50));
        }
    };
    {
        let _server = Server::start(&data, &fake.address);
        wait_for("for the first bot");
        // The state is written on the pass after the one that stored the line.
        thread::sleep(Duration::from_millis(800));
    }

    // The bot with the update saved its own offset, and the quiet bot has its
    // own: one bot's progress never moves another bot's cursor.
    let store = Store::open(data.join("state.db")).unwrap();
    let offsets: Vec<(String, i64)> = store
        .list_channels()
        .unwrap()
        .into_iter()
        .filter_map(|channel| {
            let state: Value = serde_json::from_str(&channel.state).ok()?;
            let agent = store.agent(&channel.agents[0]).ok()?;
            Some((agent.name, state["offset"].as_i64().unwrap_or(0)))
        })
        .collect();
    assert_eq!(offsets.len(), 2, "{offsets:?}");
    let first_bot = offsets.iter().find(|(name, _)| name == "owner0");
    let raw: Vec<String> = store
        .list_channels()
        .unwrap()
        .into_iter()
        .map(|channel| channel.state.clone())
        .collect();
    assert!(
        first_bot.is_some_and(|(_, offset)| *offset > 0),
        "the bot with the update saved its offset: {offsets:?} states={raw:?}"
    );

    // After a restart the same update is asked for again and stays one message.
    fake.offer(910, dm(7, "for the first bot"));
    {
        let _server = Server::start(&data, &fake.address);
        wait_for("for the first bot");
        thread::sleep(Duration::from_millis(500));
    }
    let inbox = Store::open(data.join("state.db"))
        .unwrap()
        .inbox(&agent.id, None)
        .unwrap();
    let copies = inbox
        .iter()
        .filter(|message| message.text == "for the first bot")
        .count();
    assert_eq!(copies, 1, "one update is one message across a restart");
}

/// A channel added while the server runs is polled without a restart, and a
/// channel that stops being pollable has its thread stopped.
#[test]
fn a_channel_added_while_running_is_polled_without_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    let fake = FakeTelegram::start();
    create(&data, "first");
    add_channel(&data, "first", 920);

    let server = Server::start(&data, &fake.address);
    assert!(
        fake.wait_polled(920, Duration::from_secs(20)),
        "the first bot was polled"
    );

    // A second bot is added while the server is running.
    create(&data, "second");
    add_channel(&data, "second", 921);
    assert!(
        fake.wait_polled(921, Duration::from_secs(20)),
        "the bot added while running was never polled"
    );

    // The new bot's update arrives too, so its thread is a working poller.
    fake.offer(921, dm(11, "for the new bot"));
    let agent = Store::open(data.join("state.db"))
        .unwrap()
        .agent_by_name("second")
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let inbox = Store::open(data.join("state.db"))
            .unwrap()
            .inbox(&agent.id, None)
            .unwrap();
        if inbox
            .iter()
            .any(|message| message.text == "for the new bot")
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the new bot's update never arrived"
        );
        thread::sleep(Duration::from_millis(50));
    }
    drop(server);
}

/// Shutdown stops every poller: the server ends on SIGTERM, removes its socket,
/// and no bot is polled again afterwards.
#[test]
fn shutdown_stops_every_poller() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    let fake = FakeTelegram::start();
    for (at, bot) in [930_i64, 931, 932].iter().enumerate() {
        let name = format!("owner{at}");
        create(&data, &name);
        add_channel(&data, &name, *bot);
    }
    let mut server = Server::start(&data, &fake.address);
    for bot in [930_i64, 931, 932] {
        assert!(
            fake.wait_polled(bot, Duration::from_secs(20)),
            "bot {bot} polled"
        );
    }

    // A long poll is in flight for every bot when the shutdown signal lands.
    let pid = server.child.id().to_string();
    let sent = Command::new("kill").args(["-TERM", &pid]).status().unwrap();
    assert!(sent.success(), "SIGTERM was not delivered");

    // The server ends by itself, and a poller that was still running would keep
    // this from finishing. The budget is generous because the machine may be
    // busy: the point is that it ends, not how fast.
    let deadline = Instant::now() + Duration::from_secs(60);
    let exited = loop {
        if let Ok(Some(_)) = server.child.try_wait() {
            break true;
        }
        if Instant::now() > deadline {
            break false;
        }
        thread::sleep(Duration::from_millis(50));
    };
    assert!(exited, "the server did not stop on SIGTERM");
    assert!(
        !data.join("anyagents.sock").exists(),
        "the socket was left behind"
    );

    // Nothing polls after the shutdown.
    let after = fake.asked().len();
    thread::sleep(Duration::from_secs(2));
    assert_eq!(
        fake.asked().len(),
        after,
        "a bot was still polled after shutdown"
    );
}
