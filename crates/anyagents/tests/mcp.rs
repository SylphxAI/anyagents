use std::io::Read;
use std::os::unix::fs::PermissionsExt;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rmcp::model::{CallToolRequestParams, CallToolResult, ContentBlock};
use rmcp::transport::{ConfigureCommandExt, TokioChildProcess};
use rmcp::{ServiceExt, serde_json};
use serde::Deserialize;

fn bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_anyagents"))
}

/// A `PATH` where creating an agent installs nothing: `npm` answers a version
/// and a no-op install, and `claude` is this binary.
fn machine_path(dir: &std::path::Path) -> std::ffi::OsString {
    use std::os::unix::fs::PermissionsExt;
    let bin = dir.join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    let launcher = bin.join("claude");
    if !launcher.exists() {
        std::os::unix::fs::symlink(env!("CARGO_BIN_EXE_anyagents"), &launcher).unwrap();
    }
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

#[derive(Deserialize)]
struct AgentsResponse {
    agents: Vec<AgentView>,
}

#[derive(Deserialize)]
struct AgentView {
    name: String,
    harness: String,
    state: String,
    process: String,
    delivery_mode: String,
    steer_support: String,
}

#[derive(Deserialize)]
struct SendResponse {
    deliveries: Vec<Delivery>,
}

#[derive(Deserialize)]
struct Delivery {
    to: String,
    seq: i64,
    duplicate: bool,
}

#[derive(Deserialize)]
struct InboxResponse {
    messages: Vec<MessageView>,
}

#[derive(Deserialize)]
struct MessageView {
    seq: i64,
    text: String,
    sender: Option<String>,
    ack: String,
}

struct Serve {
    child: Child,
}

impl Drop for Serve {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn start_server(data: &std::path::Path) -> Serve {
    let mut child = bin()
        .arg("--data")
        .arg(data)
        .arg("serve")
        .env("ANYAGENTS_CLAUDE_BIN", "/bin/true")
        .env("ANYAGENTS_CODEX_BIN", "/bin/true")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let pipe = child.stderr.take().unwrap();
    let stderr = Arc::new(Mutex::new(String::new()));
    let captured = Arc::clone(&stderr);
    std::thread::spawn(move || {
        let mut buf = String::new();
        let mut pipe = pipe;
        let _ = pipe.read_to_string(&mut buf);
        *captured.lock().unwrap() = buf;
    });
    let socket = data.join("anyagents.sock");
    let started = Instant::now();
    loop {
        if socket.exists() {
            let mode = socket.metadata().unwrap().permissions().mode() & 0o777;
            if mode == 0o600 {
                return Serve { child };
            }
        }
        if let Some(status) = child.try_wait().unwrap() {
            panic!("server exited {status}: {}", stderr.lock().unwrap());
        }
        if started.elapsed() > Duration::from_secs(5) {
            panic!("server did not listen: {}", stderr.lock().unwrap());
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// An agent's id, from the one directory its config lives in.
fn created_id(dir: &std::path::Path, name: &str) -> String {
    let out = bin()
        .arg("--data")
        .arg(dir)
        .args(["agents", "list"])
        .output()
        .unwrap();
    let text = String::from_utf8(out.stdout).unwrap();
    text.lines()
        .find(|line| line.split('\t').nth(1) == Some(name))
        .and_then(|line| line.split('\t').next())
        .unwrap_or_else(|| panic!("no {name} in {text}"))
        .to_string()
}

#[allow(dead_code)]
fn listed_id(dir: &std::path::Path, name: &str) -> String {
    let out = bin()
        .env("PATH", machine_path(dir))
        .arg("--data")
        .arg(dir)
        .args(["agents", "list"])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let text = String::from_utf8(out.stdout).unwrap();
    text.lines()
        .find(|line| line.split('\t').nth(1) == Some(name))
        .and_then(|line| line.split('\t').next())
        .unwrap_or_else(|| panic!("no {name} in {text}"))
        .to_string()
}

/// A tool server for one agent: its capability is in that agent's own config.
async fn connect(
    data: &std::path::Path,
    agent: &str,
) -> rmcp::service::RunningService<rmcp::RoleClient, ()> {
    let id = created_id(data, agent);
    let config = data.join("agents").join(&id).join("config");

    let transport = TokioChildProcess::new(
        tokio::process::Command::new(env!("CARGO_BIN_EXE_anyagents")).configure(|cmd| {
            // The single-user route: this deployment has no operator token, so
            // its own agent route names the agent. The server still resolves
            // the name itself, and a tool call never sends as anyone else.
            cmd.env("ANYAGENTS_AGENT_ROUTE", data)
                .env("ANYAGENTS_DATA", data)
                .env("ANYAGENTS_AGENT_ID", &id)
                .arg("--data")
                .arg(data)
                .args(["mcp", "--config"])
                .arg(&config);
        }),
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(10), ().serve(transport))
        .await
        .expect("mcp handshake timed out")
        .unwrap()
}

async fn call(
    client: &rmcp::service::RunningService<rmcp::RoleClient, ()>,
    name: &str,
    arguments: serde_json::Value,
) -> CallToolResult {
    let mut params = CallToolRequestParams::new(name.to_string());
    if let Some(arguments) = arguments.as_object().cloned() {
        params = params.with_arguments(arguments);
    }
    tokio::time::timeout(Duration::from_secs(10), client.call_tool(params))
        .await
        .expect("tool call timed out")
        .unwrap()
}

fn ok_json<T: for<'de> Deserialize<'de>>(result: CallToolResult) -> T {
    assert_ne!(result.is_error, Some(true), "{result:?}");
    serde_json::from_value(result.structured_content.expect("structured content")).unwrap()
}

fn error_text(result: &CallToolResult) -> String {
    assert_eq!(result.is_error, Some(true), "{result:?}");
    match result.content.first() {
        Some(ContentBlock::Text(text)) => text.text.clone(),
        other => panic!("expected text error, got {other:?}"),
    }
}

#[tokio::test]
async fn mcp_tools_use_the_server_socket() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("anyagents.sock");
    assert!(
        socket.as_os_str().len() <= 107,
        "socket path is too long for a unix socket"
    );
    for name in ["alpha", "beta"] {
        let created = bin()
            .env("PATH", machine_path(dir.path()))
            .arg("--data")
            .arg(dir.path())
            .args(["agents", "create", name])
            .output()
            .unwrap();
        assert!(
            created.status.success(),
            "{}",
            String::from_utf8_lossy(&created.stderr)
        );
    }

    let server = start_server(dir.path());
    let alpha = connect(dir.path(), "alpha").await;
    let beta = connect(dir.path(), "beta").await;

    let tools = alpha.list_all_tools().await.unwrap();
    let names: Vec<_> = tools.iter().map(|tool| tool.name.to_string()).collect();
    assert!(names.contains(&"send".to_string()), "{names:?}");
    assert!(names.contains(&"inbox".to_string()), "{names:?}");
    assert!(names.contains(&"agents".to_string()), "{names:?}");

    let listed: AgentsResponse = ok_json(call(&alpha, "agents", serde_json::json!({})).await);
    assert_eq!(listed.agents.len(), 2);
    let alpha_row = listed
        .agents
        .iter()
        .find(|agent| agent.name == "alpha")
        .expect("alpha");
    assert_eq!(alpha_row.harness, "claude-code");
    assert_eq!(alpha_row.state, "running");
    // Claude Code takes a user line mid-turn, so the catalog steers.
    assert_eq!(alpha_row.delivery_mode, "steer");
    assert_eq!(alpha_row.steer_support, "yes");
    assert!(alpha_row.process == "alive" || alpha_row.process == "down");

    let listed_cli = bin()
        .arg("--data")
        .arg(dir.path())
        .args(["agents", "list"])
        .output()
        .unwrap();
    assert!(listed_cli.status.success());
    let text = String::from_utf8(listed_cli.stdout).unwrap();
    assert!(text.contains("running"), "{text}");
    assert!(text.contains("steer"), "{text}");
    assert!(text.contains("yes"), "{text}");

    let sent: SendResponse = ok_json(
        call(
            &alpha,
            "send",
            serde_json::json!({"to": ["beta", "agent:beta"], "text": "hello"}),
        )
        .await,
    );
    assert_eq!(sent.deliveries.len(), 2);
    assert!(
        sent.deliveries
            .iter()
            .all(|row| row.to == "agent:beta" && !row.duplicate)
    );
    assert_eq!(
        sent.deliveries
            .iter()
            .map(|row| row.seq)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );

    let own: InboxResponse = ok_json(call(&alpha, "inbox", serde_json::json!({})).await);
    assert!(own.messages.is_empty(), "{:?}", own.messages.len());

    let got: InboxResponse = ok_json(call(&beta, "inbox", serde_json::json!({})).await);
    assert_eq!(got.messages.len(), 2);
    assert_eq!(got.messages[0].text, "hello");
    assert_eq!(got.messages[0].sender.as_deref(), Some("agent:alpha"));
    // The mailbox holds it as soon as it is received. Whether the harness has
    // taken it depends on the vendor process, which this test does not run.
    assert!(
        matches!(got.messages[0].ack.as_str(), "received" | "delivered"),
        "{}",
        got.messages[0].ack
    );

    // A room reaches every other agent, and not the sender.
    // A named group reaches its own members.
    for name in ["alpha", "beta"] {
        let joined = bin()
            .arg("--data")
            .arg(dir.path())
            .args(["groups", "join", "crew", name])
            .output()
            .unwrap();
        assert!(
            joined.status.success(),
            "{}",
            String::from_utf8_lossy(&joined.stderr)
        );
    }
    let room: SendResponse = ok_json(
        call(
            &alpha,
            "send",
            serde_json::json!({"to": ["group:crew"], "text": "room line"}),
        )
        .await,
    );
    let to: Vec<&str> = room.deliveries.iter().map(|one| one.to.as_str()).collect();
    assert!(to.contains(&"agent:beta"), "{to:?}");
    assert!(!to.contains(&"agent:alpha"), "{to:?}");
    let in_room: InboxResponse =
        ok_json(call(&beta, "inbox", serde_json::json!({"since": 0})).await);
    assert!(
        in_room.messages.iter().any(|line| line.text == "room line"),
        "{:?}",
        in_room.messages.len()
    );
    let own: InboxResponse = ok_json(call(&alpha, "inbox", serde_json::json!({"since": 0})).await);
    assert!(!own.messages.iter().any(|line| line.text == "room line"));

    let missing = call(
        &alpha,
        "send",
        serde_json::json!({"to": ["beta", "missing"], "text": "nope"}),
    )
    .await;
    assert!(error_text(&missing).contains("agent not found"));
    // beta holds the two addressed lines and the room line; `since` 1 leaves the
    // later two. A failed send to a missing agent wrote nothing.
    let since: InboxResponse = ok_json(call(&beta, "inbox", serde_json::json!({"since": 1})).await);
    assert_eq!(since.messages.len(), 2);
    assert_eq!(since.messages[1].seq, 3);

    let empty = call(
        &alpha,
        "send",
        serde_json::json!({"to": [], "text": "nope"}),
    )
    .await;
    assert!(error_text(&empty).contains("send needs an address"));

    alpha.cancel().await.unwrap();
    beta.cancel().await.unwrap();
    drop(server);

    let inbox = bin()
        .arg("--data")
        .arg(dir.path())
        .args(["inbox", "beta"])
        .output()
        .unwrap();
    assert!(
        inbox.status.success(),
        "{}",
        String::from_utf8_lossy(&inbox.stderr)
    );
    // Two addressed lines and the room line, each readable from the CLI too.
    let listed = String::from_utf8(inbox.stdout).unwrap();
    assert_eq!(listed.lines().count(), 3, "{listed}");
    assert!(listed.contains("room line"), "{listed}");
}
