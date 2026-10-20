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

async fn connect(
    data: &std::path::Path,
    agent: &str,
) -> rmcp::service::RunningService<rmcp::RoleClient, ()> {
    let transport = TokioChildProcess::new(
        tokio::process::Command::new(env!("CARGO_BIN_EXE_anyagents")).configure(|cmd| {
            cmd.arg("--data").arg(data).args(["mcp", "--agent", agent]);
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
    assert_eq!(alpha_row.delivery_mode, "followup");
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
    assert!(text.contains("followup"), "{text}");
    assert!(text.contains("unproven"), "{text}");

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
    assert_eq!(got.messages[0].ack, "received");

    let rejected = call(
        &alpha,
        "send",
        serde_json::json!({"to": ["beta", "group:crew"], "text": "nope"}),
    )
    .await;
    assert!(error_text(&rejected).contains("not implemented yet"));
    let still: InboxResponse = ok_json(call(&beta, "inbox", serde_json::json!({"since": 0})).await);
    assert_eq!(still.messages.len(), 2);

    let missing = call(
        &alpha,
        "send",
        serde_json::json!({"to": ["beta", "missing"], "text": "nope"}),
    )
    .await;
    assert!(error_text(&missing).contains("agent not found"));
    let since: InboxResponse = ok_json(call(&beta, "inbox", serde_json::json!({"since": 1})).await);
    assert_eq!(since.messages.len(), 1);
    assert_eq!(since.messages[0].seq, 2);

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
    let listed = String::from_utf8(inbox.stdout).unwrap();
    assert_eq!(listed.lines().count(), 2, "{listed}");
}
