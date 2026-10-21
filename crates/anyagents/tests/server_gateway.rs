//! The server on an API-key agent: the gateway starts with the server, the
//! harness is pointed at it with its own token, and a turn the harness sends
//! reaches the stored provider and comes back.
//!
//! Nothing here is a real vendor or provider. The harness is `anyagents
//! fake-claude`, this binary's test double, and the provider is a loopback
//! server that answers one Anthropic Messages request and records what it saw.

mod support;

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyagents_core::Store;

/// A server child that is killed when this value drops.
struct Server {
    child: Child,
}

impl Server {
    fn start(dir: &Path) -> Self {
        let child = Command::new(env!("CARGO_BIN_EXE_anyagents"))
            .env("ANYAGENTS_CLAUDE_BIN", env!("CARGO_BIN_EXE_anyagents"))
            .env("ANYAGENTS_FAKE_CLAUDE", "1")
            .env("PATH", machine_path(dir))
            .env("ANYAGENTS_FAKE_MODE", "hold")
            .env("ANYAGENTS_FAKE_LOG", dir.join("fake.log"))
            .arg("--data")
            .arg(dir)
            .arg("serve")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("server");
        let socket = dir.join("anyagents.sock");
        // The file can outlive the server that made it, so readiness is a
        // connection that succeeds, not a path that exists.
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if std::os::unix::net::UnixStream::connect(&socket).is_ok() {
                break;
            }
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

/// One answer from the provider, and what the request carried.
struct Provider {
    base: String,
    seen: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
}

fn provider() -> Provider {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let recorded = std::sync::Arc::clone(&seen);
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            serve_one(stream, &recorded);
        }
    });
    Provider { base, seen }
}

fn serve_one(mut stream: std::net::TcpStream, seen: &std::sync::Mutex<Vec<String>>) {
    let mut request = Vec::new();
    let mut chunk = [0u8; 4096];
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => request.extend_from_slice(&chunk[..n]),
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(_) => break,
        }
        let text = String::from_utf8_lossy(&request).to_string();
        if let Some(head) = text.find("\r\n\r\n") {
            let length: usize = text[..head]
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .map(|value| value.trim().parse().unwrap_or(0))
                })
                .unwrap_or(0);
            if request.len() >= head + 4 + length {
                break;
            }
        }
        if Instant::now() > deadline {
            break;
        }
    }
    let text = String::from_utf8_lossy(&request).to_string();
    seen.lock().unwrap().push(text.clone());
    let body = r#"{"id":"msg_1","type":"message","role":"assistant","model":"claude","content":[{"type":"text","text":"hello from the provider"}],"stop_reason":"end_turn","usage":{"input_tokens":3,"output_tokens":2}}"#;
    let answer = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(answer.as_bytes());
    let _ = stream.flush();
}

/// A `PATH` carrying the test double as the `claude` the machine already has,
/// so nothing is downloaded for an install on this machine.
fn machine_path(dir: &Path) -> std::ffi::OsString {
    let bin = dir.join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    let launcher = bin.join("claude");
    if !launcher.exists() {
        std::os::unix::fs::symlink(env!("CARGO_BIN_EXE_anyagents"), &launcher).unwrap();
    }
    let npm = bin.join("npm");
    if !npm.exists() {
        // Creating an agent installs its harness here; this test runs none of
        // it, so `view` answers a version and an install does nothing.
        std::fs::write(
            &npm,
            "#!/bin/sh\ncase \"$1\" in view) echo 0.0.0 ;; esac\nexit 0\n",
        )
        .unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&npm, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    std::env::join_paths([bin]).unwrap()
}

fn bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_anyagents"))
}

fn create(dir: &Path, name: &str) -> String {
    // The server at `dir` is running, and it installs the harness the moment the
    // agent is created: same data directory, same `PATH` it was given.
    let socket = dir.join("anyagents.sock");
    // The API, not the CLI: the CLI would install into a fresh process without
    // the server's installer, and the server has already answered.
    let body = serde_json::json!({
        "name": name,
        "harness": "claude-code",
        "model": "claude",
    })
    .to_string();
    let answer = post(&socket, "/v1/agents", &body);
    let start = answer.find("\r\n\r\n").map(|at| at + 4).expect(&answer);
    let created: serde_json::Value = serde_json::from_str(&answer[start..]).expect(&answer);
    created["id"]
        .as_str()
        .unwrap_or_else(|| panic!("{answer}"))
        .to_string()
}

/// One POST over the server's unix socket, answered in full.
fn post(socket: &Path, path: &str, body: &str) -> String {
    let mut stream = std::os::unix::net::UnixStream::connect(socket).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(20)))
        .unwrap();
    stream
        .write_all(
            format!(
                "POST {path} HTTP/1.1\r\nHost: local\r\n{}content-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                support::header(),
                body.len()
            )
            .as_bytes(),
        )
        .unwrap();
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).unwrap();
    String::from_utf8_lossy(&buf).to_string()
}

fn agent_dir(data: &Path, id: &str) -> PathBuf {
    data.join("agents").join(id)
}

#[test]
fn an_api_key_agent_gets_the_gateway_and_its_turn_reaches_the_provider() {
    let _env = support::lock();
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    support::operator(&data);
    let provider = provider();
    let server = Server::start(&data);

    let id = create(&data, "alpha");
    let key_file = dir.path().join("key");
    std::fs::write(&key_file, "sk-provider-secret\n").unwrap();
    let stored = bin()
        .arg("--data")
        .arg(&data)
        .args([
            "model",
            "alpha",
            "--base-url",
            &provider.base,
            "--model",
            "claude",
        ])
        .arg("--key-file")
        .arg(&key_file)
        .output()
        .unwrap();
    assert!(
        stored.status.success(),
        "stderr={} stdout={}",
        String::from_utf8_lossy(&stored.stderr),
        String::from_utf8_lossy(&stored.stdout)
    );

    let printed = format!(
        "{}{}",
        String::from_utf8_lossy(&stored.stdout),
        String::from_utf8_lossy(&stored.stderr)
    );
    assert!(!printed.contains("sk-provider-secret"), "{printed}");
    let stored_key = std::fs::read_to_string(agent_dir(&data, &id).join("provider.json")).unwrap();
    assert!(stored_key.contains("sk-provider-secret"));

    // A running server takes the key at once: its route answers, and the agent
    // is told the gateway from its next start. The server records that origin
    // as it starts, which can land just after this process reads, so it is
    // waited for rather than assumed.
    let deadline = Instant::now() + Duration::from_secs(10);
    let launch = loop {
        let launch = Store::open(data.join("state.db"))
            .unwrap()
            .agent_by_name("alpha")
            .unwrap()
            .launch_env;
        if !launch.is_empty() || Instant::now() > deadline {
            break launch;
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    assert!(
        launch
            .iter()
            .any(|(key, value)| key == "ANTHROPIC_BASE_URL"
                && value.starts_with("http://127.0.0.1:")),
        "{launch:?}"
    );
    drop(server);
    let _server = Server::start(&data);

    // The restarted server binds a new port and repoints every agent at it, so
    // the origin is read from the running server, not from before the restart.
    let store = Store::open(data.join("state.db")).unwrap();
    let agent = store.agent_by_name("alpha").unwrap();
    assert!(
        agent
            .launch_env
            .iter()
            .all(|(_, value)| !value.contains("sk-provider")),
        "the provider key never reaches the agent's environment"
    );
    // A direct request with this agent's token reaches the stored provider.
    let token = std::fs::read_to_string(agent_dir(&data, &id).join("gateway-token.txt"))
        .unwrap()
        .trim()
        .to_string();
    let origin = store
        .agent_by_name("alpha")
        .unwrap()
        .launch_env
        .iter()
        .find(|(key, _)| key == "ANTHROPIC_BASE_URL")
        .map(|(_, value)| value.clone())
        .expect("the agent is pointed at the gateway");
    let body = serde_json::json!({
        "model": "claude",
        "max_tokens": 16,
        "messages": [{"role": "user", "content": "ping"}],
    })
    .to_string();
    let answer = http(
        &origin,
        &format!(
            "POST /v1/messages HTTP/1.1\r\nHost: local\r\ncontent-type: application/json\r\nAuthorization: Bearer {token}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        ),
    );
    assert!(answer.contains("200 OK"), "{answer}");
    assert!(answer.contains("hello from the provider"), "{answer}");
    let unknown = http(
        &origin,
        &format!(
            "POST /v1/messages HTTP/1.1\r\nHost: local\r\ncontent-type: application/json\r\nAuthorization: Bearer not-a-token\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        ),
    );
    assert!(unknown.contains("401"), "{unknown}");

    // The provider saw that request, with the stored key, and not the agent's token.
    let handled = provider.seen.lock().unwrap().join("\n");
    assert!(
        handled.contains("sk-provider-secret"),
        "the stored key is forwarded"
    );
    assert!(
        !handled.contains(&token),
        "the harness token is not passed on"
    );

    // The harness runs under the gateway's origin and this agent's token, never
    // the provider key.
    assert!(
        launch
            .iter()
            .all(|(_, value)| !value.contains("sk-provider")),
        "{launch:?}"
    );
}

fn http(base: &str, request: &str) -> String {
    use std::net::TcpStream;
    let address = base.trim_start_matches("http://");
    let mut stream = TcpStream::connect(address).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    stream.write_all(request.as_bytes()).unwrap();
    let mut buf = Vec::new();
    let _ = stream.read_to_end(&mut buf);
    String::from_utf8_lossy(&buf).to_string()
}
