//! The server against a fake Telegram Bot API: a DM and a forum topic message
//! land in the agent's mailbox with their chat, topic and message id, and the
//! agent's replies arrive in the same chat and the same topic. The fake also
//! sends one update twice, as `getUpdates` can after a restart, and the mailbox
//! keeps it once.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use anyagents_core::{CredentialStore, FileCredentialStore, Store};
use serde_json::{Value, json};

/// One `sendMessage` the fake server received.
#[derive(Clone, Debug, PartialEq)]
struct Sent {
    chat_id: i64,
    text: String,
    reply_to: Option<i64>,
    thread: Option<i64>,
}

/// A fake Bot API on loopback, standing in for `api.telegram.org`.
struct FakeTelegram {
    address: String,
    sent: Arc<Mutex<Vec<Sent>>>,
    updates: Arc<Mutex<Vec<Value>>>,
    served: Arc<Mutex<usize>>,
    stop: Arc<Mutex<bool>>,
    thread: Option<JoinHandle<()>>,
}

impl FakeTelegram {
    fn start(updates: Vec<Value>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let sent = Arc::new(Mutex::new(Vec::new()));
        let updates = Arc::new(Mutex::new(updates));
        let pending = Arc::clone(&updates);
        let served = Arc::new(Mutex::new(0));
        let stop = Arc::new(Mutex::new(false));
        let worker = Worker {
            listener,
            pending: Arc::clone(&pending),
            sent: Arc::clone(&sent),
            served: Arc::clone(&served),
            stop: Arc::clone(&stop),
        };
        let handle = thread::spawn(move || worker.run());
        Self {
            address,
            sent,
            updates,
            served,
            stop,
            thread: Some(handle),
        }
    }

    fn sent(&self) -> Vec<Sent> {
        self.sent.lock().unwrap().clone()
    }

    /// How many updates are still waiting to be asked for.
    fn waiting(&self) -> usize {
        lock(&self.updates).len()
    }

    /// Whether a `getUpdates` has been served yet.
    fn asked_for_updates(&self) -> bool {
        *lock(&self.served) > 0
    }
}

/// The fake's listening side, on its own thread.
struct Worker {
    listener: TcpListener,
    pending: Arc<Mutex<Vec<Value>>>,
    sent: Arc<Mutex<Vec<Sent>>>,
    served: Arc<Mutex<usize>>,
    stop: Arc<Mutex<bool>>,
}

impl Worker {
    fn run(self) {
        self.listener.set_nonblocking(true).unwrap();
        loop {
            if *lock(&self.stop) {
                return;
            }
            match self.listener.accept() {
                Ok((stream, _)) => {
                    stream
                        .set_read_timeout(Some(Duration::from_secs(5)))
                        .unwrap();
                    answer(stream, &self.pending, &self.sent, &self.served);
                }
                Err(_) => thread::sleep(Duration::from_millis(10)),
            }
        }
    }
}

impl Drop for FakeTelegram {
    fn drop(&mut self) {
        *lock(&self.stop) = true;
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poison| poison.into_inner())
}

fn answer(
    mut stream: TcpStream,
    pending: &Arc<Mutex<Vec<Value>>>,
    sent: &Arc<Mutex<Vec<Sent>>>,
    served: &Arc<Mutex<usize>>,
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
    let method = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or("/")
        .rsplit('/')
        .next()
        .unwrap_or("")
        .to_string();
    let request: Value = serde_json::from_slice(&body).unwrap_or(json!({}));
    let response = match method.as_str() {
        "getMe" => json!({
            "ok": true,
            "result": {"id": 777, "is_bot": true, "first_name": "Ada", "username": "ada_bot"},
        }),
        "getUpdates" => {
            let mut pending = pending.lock().unwrap();
            let mut served = served.lock().unwrap();
            // The first call hands over everything and, as Telegram may after
            // a restart, the same update twice.
            let batch = if *served == 0 {
                let mut batch = pending.clone();
                if let Some(first) = pending.first().cloned() {
                    batch.push(first);
                }
                *served = 1;
                batch
            } else {
                pending.clone()
            };
            pending.clear();
            json!({"ok": true, "result": batch})
        }
        "sendMessage" => {
            sent.lock().unwrap().push(Sent {
                chat_id: request["chat_id"].as_i64().unwrap(),
                text: request["text"].as_str().unwrap_or_default().to_string(),
                reply_to: request["reply_to_message_id"].as_i64(),
                thread: request["message_thread_id"].as_i64(),
            });
            let id = 900 + sent.lock().unwrap().len() as i64;
            json!({"ok": true, "result": {"message_id": id, "chat": {"id": 1}, "date": 0}})
        }
        other => json!({"ok": false, "error_code": 400, "description": format!("unknown {other}")}),
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
    /// Start the server on `data`, calling a fake Telegram at `api`.
    fn start(dir: &Path, api: &str) -> Self {
        let child = Command::new(env!("CARGO_BIN_EXE_anyagents"))
            .env("ANYAGENTS_OPENCODE_BIN", env!("CARGO_BIN_EXE_anyagents"))
            .env("ANYAGENTS_FAKE_OPENCODE", "1")
            .env("ANYAGENTS_FAKE_LOG", dir.join("fake.log"))
            .env("ANYAGENTS_TELEGRAM_API", api)
            .arg("--data")
            .arg(dir)
            .arg("serve")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("server");
        let socket = dir.join("anyagents.sock");
        let deadline = Instant::now() + Duration::from_secs(10);
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

fn create(dir: &Path, name: &str) -> String {
    let created = Command::new(env!("CARGO_BIN_EXE_anyagents"))
        .arg("--data")
        .arg(dir)
        .args(["agents", "create", name, "--harness", "opencode", "--model"])
        .arg("opencode/big-pickle")
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

fn add_channel(dir: &Path, agent: &str, token: &str) -> String {
    let token_file = dir.join("bot-token");
    std::fs::write(&token_file, token).unwrap();
    let added = Command::new(env!("CARGO_BIN_EXE_anyagents"))
        .arg("--data")
        .arg(dir)
        .args(["channels", "add", "telegram", "--agent", agent])
        .arg("--token-file")
        .arg(&token_file)
        .output()
        .unwrap();
    std::fs::remove_file(&token_file).unwrap();
    assert!(
        added.status.success(),
        "{}",
        String::from_utf8_lossy(&added.stderr)
    );
    let stdout = String::from_utf8(added.stdout).unwrap();
    stdout
        .lines()
        .next()
        .unwrap()
        .split('\t')
        .next()
        .unwrap()
        .to_string()
}

fn post(socket: &Path, request: &Value) -> Value {
    let request = request.to_string();
    let mut stream = std::os::unix::net::UnixStream::connect(socket).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    stream
        .write_all(
            format!(
                "POST /v1/send HTTP/1.1\r\nHost: local\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{request}",
                request.len()
            )
            .as_bytes(),
        )
        .unwrap();
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).unwrap();
    let text = String::from_utf8_lossy(&buf);
    let json_start = text.find("\r\n\r\n").map(|at| at + 4).expect(&text);
    serde_json::from_str(&text[json_start..]).expect(&text)
}

fn dm(update_id: i64, chat: i64, message_id: i64, text: &str) -> Value {
    json!({
        "update_id": update_id,
        "message": {
            "message_id": message_id,
            "chat": {"id": chat, "type": "private"},
            "from": {"id": 42, "is_bot": false, "first_name": "Kai", "username": "kai"},
            "date": 1_700_000_000,
            "text": text,
        },
    })
}

fn topic(update_id: i64, chat: i64, message_id: i64, thread: i64, text: &str) -> Value {
    json!({
        "update_id": update_id,
        "message": {
            "message_id": message_id,
            "message_thread_id": thread,
            "is_topic_message": true,
            "chat": {"id": chat, "type": "supergroup", "is_forum": true},
            "from": {"id": 43, "is_bot": false, "first_name": "Mo"},
            "date": 1_700_000_001,
            "text": text,
        },
    })
}

fn wait_for(mut ready: impl FnMut() -> bool, why: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !ready() {
        assert!(Instant::now() < deadline, "{why}");
        thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn a_dm_and_a_topic_reach_the_mailbox_and_answers_go_back_there() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    let fake = FakeTelegram::start(vec![
        dm(10, 100, 5, "hello from a dm"),
        topic(11, -1007, 55, 3, "and a topic"),
    ]);

    let agent_name = "ada";
    let agent = create(&data, agent_name);
    add_channel(&data, agent_name, "777:fake-token-not-real");

    // The token is kept by reference, and the channel row names its bot instead.
    let secrets = FileCredentialStore::new(&data);
    let store = Store::open(data.join("state.db")).unwrap();
    let channel = store.list_channels().unwrap().remove(0);
    let channel_id = channel.id.as_str().to_string();
    assert_eq!(
        channel.credential_ref.as_deref(),
        Some(format!("channels/{channel_id}").as_str())
    );
    assert!(!channel.state.contains("fake-token"), "{}", channel.state);

    let server = Server::start(&data, &fake.address);
    let agent_id = anyagents_core::AgentId::parse(&agent).unwrap();
    wait_for(
        || {
            let store = Store::open(data.join("state.db")).unwrap();
            let inbox = store.inbox(&agent_id, None).unwrap();
            inbox.len() >= 2
        },
        "the two messages never reached the mailbox",
    );

    let store = Store::open(data.join("state.db")).unwrap();
    let inbox = store.inbox(&agent_id, None).unwrap();
    assert_eq!(
        inbox.len(),
        2,
        "one update delivered twice is still one: {inbox:?}"
    );
    assert_eq!(inbox[0].text, "hello from a dm");
    assert_eq!(inbox[0].reply_to.as_deref(), Some("telegram:chat:100"));
    assert_eq!(inbox[0].sender.as_deref(), Some("telegram:user:42:5"));
    assert_eq!(inbox[0].source_key.as_deref(), Some("bot:10"));
    assert_eq!(inbox[1].text, "and a topic");
    assert_eq!(
        inbox[1].reply_to.as_deref(),
        Some("telegram:chat:-1007:3"),
        "a topic message is answered in its topic"
    );

    // The bot confirms itself once and remembers what it has stored.
    let channel = store.list_channels().unwrap().remove(0);
    let state: Value = serde_json::from_str(&channel.state).unwrap();
    assert_eq!(state["bot"], "777:ada_bot");
    assert_eq!(state["offset"], 12);
    let chats = state["chats"].as_array().unwrap();
    assert!(chats.iter().any(|link| link["chat_id"] == 100));
    assert!(chats.iter().any(|link| link["chat_id"] == -1007));

    // The agent answers the DM and the topic through its own bot.
    let socket = data.join("anyagents.sock");
    let to_dm = post(
        &socket,
        &json!({
            "sender": "agent:ada",
            "to": ["telegram:chat:100"],
            "text": "pong in the dm",
            "reply_to": "5",
        }),
    );
    assert_eq!(to_dm["deliveries"][0]["id"], "901", "{to_dm}");
    let to_topic = post(
        &socket,
        &json!({
            "sender": "agent:ada",
            "to": ["telegram:chat:-1007:3"],
            "text": "pong in the topic",
            "reply_to": "55",
        }),
    );
    assert_eq!(to_topic["deliveries"][0]["id"], "902", "{to_topic}");

    let sent = fake.sent();
    assert_eq!(sent.len(), 2, "{sent:?}");
    assert_eq!(sent[0], Sent {
        chat_id: 100,
        text: "pong in the dm".into(),
        reply_to: Some(5),
        thread: None,
    });
    assert_eq!(sent[1], Sent {
        chat_id: -1007,
        text: "pong in the topic".into(),
        reply_to: Some(55),
        thread: Some(3),
    });
    drop(server);

    // The token is only in its own 0600 file, never in the channel row.
    let secret = secrets
        .read(&format!("channels/{channel_id}"))
        .unwrap();
    assert_eq!(secret.as_str().unwrap(), "777:fake-token-not-real");
}

#[test]
fn a_bad_topic_id_is_refused_before_anything_is_posted() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    let fake = FakeTelegram::start(Vec::new());
    create(&data, "ada");
    add_channel(&data, "ada", "777:fake-token-not-real");
    let server = Server::start(&data, &fake.address);
    let socket = data.join("anyagents.sock");
    let refused = post(
        &socket,
        &json!({
            "sender": "agent:ada",
            "to": ["telegram:chat:100:two"],
            "text": "hi",
        }),
    );
    assert!(refused["error"].is_string(), "{refused}");
    assert!(fake.sent().is_empty());
    wait_for(
        || fake.asked_for_updates(),
        "the bot was never polled",
    );
    assert_eq!(fake.waiting(), 0, "the bot did not ask for its update");
    drop(server);
}
