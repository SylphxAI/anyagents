//! The server on an OpenCode agent: create, one send, one answer on the mailbox.
//!
//! The vendor is the scripted `opencode` the adapter's tests use: it holds a
//! service and answers `opencode api` calls, as the real CLI does.

use std::io::{Read, Write};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyagents_core::Store;

/// The scripted `opencode` this crate builds beside the server binary.
fn scripted_opencode() -> std::path::PathBuf {
    let exe = std::path::Path::new(env!("CARGO_BIN_EXE_anyagents"));
    let dir = exe.parent().expect("the test binary has a directory");
    let candidate = dir.join("scripted_opencode");
    if candidate.is_file() {
        return candidate;
    }
    panic!("no scripted opencode at {}", candidate.display());
}
use serde_json::{Value, json};

/// Speak the least an MCP client needs to the agent's own tool server: start
/// it, initialize, and send one reply to the ask. This is how an agent answers,
/// exactly as its model would through the `send` tool.
fn mcp_send_once(mut child: Child) -> std::io::Result<std::process::Output> {
    use std::io::BufRead;
    let mut stdin = child.stdin.take().expect("stdin");
    let mut stdout = std::io::BufReader::new(child.stdout.take().expect("stdout"));
    let mut line = String::new();
    writeln!(
        stdin,
        r#"{{"jsonrpc":"2.0","id":1,"method":"initialize","params":{{"protocolVersion":"2025-06-18","capabilities":{{}},"clientInfo":{{"name":"opencode-test","version":"0"}}}}}}"#
    )?;
    stdout.read_line(&mut line)?;
    writeln!(
        stdin,
        r#"{{"jsonrpc":"2.0","method":"notifications/initialized"}}"#
    )?;
    writeln!(
        stdin,
        r#"{{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{{"name":"send","arguments":{{"to":["agent:owner"],"text":"pong from opencode"}}}}}}"#
    )?;
    line.clear();
    stdout.read_line(&mut line)?;
    drop(stdin);
    child.wait_with_output()
}

/// A server child that is killed when this value drops.
struct Server {
    child: Child,
}

impl Server {
    fn start(dir: &Path) -> Self {
        let child = Command::new(env!("CARGO_BIN_EXE_anyagents"))
            // The scripted CLI, which speaks `serve` and `api` as the real one does.
            .env("ANYAGENTS_OPENCODE_BIN", scripted_opencode())
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
    let store = Store::open(data.join("state.db")).unwrap();
    let agent = store.agent_by_name("alpha").unwrap();
    // The ask reaches the session as one steer prompt carrying its mailbox id,
    // and the ask is delivered once the prompt API has taken it.
    let deadline = Instant::now() + Duration::from_secs(30);
    let asked = sent["deliveries"][0]["seq"].as_i64().unwrap();
    while store.delivered_cursor(&agent.id).unwrap() < asked {
        assert!(Instant::now() < deadline, "messages not delivered");
        std::thread::sleep(Duration::from_millis(50));
    }
    // The agent replies with its own MCP `send`, not through anything the
    // session printed: the adapter scrapes no answer and writes no reply row.
    // To a second agent, so the reply is the switchboard's routing of the
    // agent's own tool call.
    let other = create(&data, "owner");
    assert!(other.status.success(), "{other:?}");
    let answered = Command::new(env!("CARGO_BIN_EXE_anyagents"))
        .args(["--data", data.to_str().unwrap(), "mcp", "--agent", "alpha"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .and_then(mcp_send_once)
        .expect("the agent's tool server runs");
    assert!(answered.status.success(), "{answered:?}");
    // The reply is the agent's own `send`, routed by the switchboard: the
    // owner's mailbox holds it, and nothing was read from the session.
    let owner = store.agent_by_name("owner").unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    let reply = loop {
        let inbox = store.inbox(&owner.id, None).unwrap();
        assert!(Instant::now() < deadline, "no reply: {inbox:?}");
        if let Some(reply) = inbox.iter().find(|message| message.sender.is_some()) {
            break reply.clone();
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    assert_eq!(reply.sender.as_deref(), Some("agent:alpha"));
    assert!(reply.text.contains("pong from opencode"), "{}", reply.text);
    // The agent's own inbox holds the ask and no answer row.
    assert!(
        store
            .inbox(&agent.id, None)
            .unwrap()
            .iter()
            .all(|message| message.reply_to.is_none())
    );
    let log = std::fs::read_to_string(data.join("fake.log")).unwrap_or_default();
    // The line reached the service as a steer prompt on its real endpoint,
    // carrying its mailbox id. The answer never went through the session: it is
    // the agent's own `send`, so no prompt carries it.
    assert!(log.contains("api:POST /api/session/"), "{log}");
    assert!(log.contains("\"delivery\":\"steer\""), "{log}");
    assert!(log.contains("anyagents-message:"), "{log}");
    assert!(!log.contains("pong from opencode"), "{log}");
    drop(server);
}
