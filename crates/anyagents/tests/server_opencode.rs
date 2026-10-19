//! The server on an OpenCode agent: create, one send, one answer on the mailbox.
//!
//! The vendor is `anyagents fake-opencode`, which is this binary in its
//! test-double mode: it answers a prompt with text and no result line, as a
//! streaming vendor does, and records its argv and stdin.

use std::io::{Read, Write};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyagents_core::Store;
use serde_json::{Value, json};

/// A server child that is killed when this value drops.
struct Server {
    child: Child,
}

impl Server {
    fn start(dir: &Path) -> Self {
        let child = Command::new(env!("CARGO_BIN_EXE_anyagents"))
            .env("ANYAGENTS_OPENCODE_BIN", env!("CARGO_BIN_EXE_anyagents"))
            .env("ANYAGENTS_FAKE_OPENCODE", "1")
            .env("ANYAGENTS_FAKE_LOG", dir.join("fake.log"))
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
            std::thread::sleep(Duration::from_millis(20));
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

fn create(dir: &Path, name: &str) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_anyagents"))
        .arg("--data")
        .arg(dir)
        .args(["agents", "create", name, "--harness", "opencode", "--model"])
        .arg("opencode/big-pickle")
        .output()
        .unwrap()
}

fn call(socket: &Path, request: &str) -> Value {
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

#[test]
fn an_opencode_agent_answers_one_line_and_the_answer_is_on_its_mailbox() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();

    let server = Server::start(&data);
    let created = create(&data, "alpha");
    assert!(
        created.status.success(),
        "{}",
        String::from_utf8_lossy(&created.stderr)
    );
    let id = String::from_utf8(created.stdout)
        .unwrap()
        .lines()
        .next()
        .unwrap()
        .split('\t')
        .next()
        .unwrap()
        .to_string();
    let config = data
        .join("agents")
        .join(&id)
        .join("config")
        .join("opencode.json");
    let parsed: Value = serde_json::from_slice(&std::fs::read(&config).unwrap()).unwrap();
    assert_eq!(parsed["model"], "opencode/big-pickle");
    assert_eq!(parsed["mcp"]["anyagents"]["type"], "local");

    let socket = data.join("anyagents.sock");
    let sent = call(
        &socket,
        &json!({"to": ["alpha"], "text": "ping", "sender": "agent:owner"}).to_string(),
    );
    let sent_text = sent.to_string();
    let sent_id = sent["deliveries"][0]["id"].as_str().expect(&sent_text);

    let store = Store::open(data.join("state.db")).unwrap();
    let agent = store.agent_by_name("alpha").unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    let reply = loop {
        let inbox = store.inbox(&agent.id, None).unwrap();
        assert!(Instant::now() < deadline, "no reply: {inbox:?}");
        if let Some(reply) = inbox.iter().find(|message| message.reply_to.is_some()) {
            break reply.clone();
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    assert_eq!(reply.sender.as_deref(), Some("agent:alpha"));
    assert_eq!(reply.reply_to.as_deref(), Some(sent_id));
    assert!(reply.text.contains("pong from opencode"), "{}", reply.text);
    // The ask is done once its answer is recorded.
    let deadline = Instant::now() + Duration::from_secs(30);
    let asked = sent["deliveries"][0]["seq"].as_i64().unwrap();
    while store.processed_cursor(&agent.id).unwrap() < asked {
        assert!(Instant::now() < deadline, "messages not done");
        std::thread::sleep(Duration::from_millis(50));
    }

    let log = std::fs::read_to_string(data.join("fake.log")).unwrap_or_default();
    assert!(log.contains("argv:fake-opencode"), "{log}");
    assert!(log.contains("anyagents-message:"), "{log}");
    // The answer is a reply, not a prompt: it is never handed back.
    assert!(!log.contains("pong from opencode\\n\""), "{log}");
    drop(server);
}
