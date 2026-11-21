mod support;

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
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_anyagents"));
    cmd.env("ANYAGENTS_TESTS", "1")
        .env_remove("ANYAGENTS_AGENT_ROUTE")
        .env_remove("ANYAGENTS_DATA")
        .env_remove("ANYAGENTS_AGENT_ID")
        .env_remove("ANYAGENTS_OWNER_WORKDIR");
    cmd
}

use support::machine_path;

struct McpChild(Child);

impl Drop for McpChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn stdio_exits_when_stdin_closes_during_whoami() {
    use std::io::{BufRead, BufReader};
    use std::os::unix::net::UnixListener;

    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config");
    std::fs::create_dir(&config).unwrap();
    std::fs::write(config.join("mcp-capability"), "fixture-capability").unwrap();
    let listener = UnixListener::bind(dir.path().join("anyagents.sock")).unwrap();
    listener.set_nonblocking(true).unwrap();
    let mut child = McpChild(
        bin()
            .arg("--data")
            .arg(dir.path())
            .arg("mcp")
            .arg("--config")
            .arg(&config)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut conn = loop {
        match listener.accept() {
            Ok((conn, _)) => break conn,
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(
                    child.0.try_wait().unwrap().is_none(),
                    "exited before whoami"
                );
                assert!(Instant::now() < deadline, "MCP never connected for whoami");
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(err) => panic!("accept whoami: {err}"),
        }
    };
    conn.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let mut request = String::new();
    BufReader::new(&mut conn).read_line(&mut request).unwrap();
    assert!(matches!(
        request.trim_end(),
        "GET /v1/whoami HTTP/1.1" | "GET http://localhost/v1/whoami HTTP/1.1"
    ));

    // Leave the RPC stalled even after closing the harness's stdin writer.
    drop(child.0.stdin.take().unwrap());
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            assert!(status.success(), "MCP exited with {status}");
            break;
        }
        assert!(
            Instant::now() < deadline,
            "MCP survived stdin EOF during whoami"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[derive(Deserialize)]
struct AgentsResponse {
    agents: Vec<AgentView>,
}

#[derive(Deserialize)]
struct AgentView {
    id: String,
    name: String,
    #[serde(default)]
    parent: Option<String>,
    harness: String,
    state: String,
    process: String,
    delivery: serde_json::Value,
}

#[derive(Deserialize)]
struct GroupsResponse {
    groups: Vec<GroupView>,
}

#[derive(Deserialize)]
struct GroupView {
    name: String,
    #[serde(default)]
    public: bool,
    members: Vec<String>,
}

#[derive(Deserialize)]
struct SendResponse {
    deliveries: Vec<Delivery>,
    #[serde(default)]
    skipped: Vec<SkippedDelivery>,
}

#[derive(Debug, Deserialize)]
struct SkippedDelivery {
    to: String,
    reason: String,
}

#[derive(Deserialize)]
struct Delivery {
    to: String,
    seq: i64,
    duplicate: bool,
}

#[derive(Deserialize)]
struct ConversationResponse {
    turns: Vec<ConversationTurnView>,
}

#[derive(Debug, Deserialize)]
struct ConversationTurnView {
    direction: String,
    from: String,
    text: String,
}

struct Serve {
    child: Child,
    _keepers: support::Keepers,
}

impl Drop for Serve {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A live stdin-consuming Claude fixture: an exited no-op is a real process
/// failure and must not be hidden by the production error-reporting path.
fn claude_double(data: &std::path::Path) -> std::path::PathBuf {
    let probe = data.join("bin").join("fixture-claude");
    std::fs::write(
        &probe,
        "#!/bin/sh\nprintf '%s\\n' \"$0\" > \"$CLAUDE_CONFIG_DIR/fixture-claude-started\"\nwhile IFS= read -r line; do :; done\n",
    )
    .unwrap();
    std::fs::set_permissions(&probe, std::fs::Permissions::from_mode(0o755)).unwrap();
    probe
}

fn start_server(data: &std::path::Path) -> Serve {
    start_server_with_env(data, &[])
}

fn local_operator_call(
    socket: &std::path::Path,
    method: &str,
    path: &str,
    body: &str,
) -> (String, serde_json::Value) {
    let answer = support::call_raw(socket, method, path, body, "x-anyagents-admin-csrf: 1\r\n");
    let status = answer.lines().next().unwrap_or_default().to_string();
    let at = answer.find("\r\n\r\n").map(|at| at + 4).expect(&answer);
    let value = serde_json::from_str(&answer[at..]).unwrap_or(serde_json::Value::Null);
    (status, value)
}

/// [`start_server`] with extra variables in the server's own environment.
fn start_server_with_env(data: &std::path::Path, env: &[(&str, &str)]) -> Serve {
    let mut command = support::contract_server();
    for (key, value) in env {
        command.env(key, value);
    }
    let mut child = command
        .arg("--data")
        .arg(data)
        .arg("serve")
        .env("PATH", machine_path(data))
        .env("ANYAGENTS_CLAUDE_CODE_BIN", claude_double(data))
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
                return Serve {
                    child,
                    _keepers: support::Keepers::of(data),
                };
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
    connect_with_owner(data, agent, None).await
}

async fn connect_with_owner(
    data: &std::path::Path,
    agent: &str,
    owner: Option<&std::path::Path>,
) -> rmcp::service::RunningService<rmcp::RoleClient, ()> {
    let id = created_id(data, agent);
    let config = data.join("agents").join(&id).join("config");

    let transport = TokioChildProcess::new(
        tokio::process::Command::new(env!("CARGO_BIN_EXE_anyagents")).configure(|cmd| {
            // The single-user route: this deployment has no operator token, so
            // its own agent route names the agent. The server still resolves
            // the name itself, and a tool call never sends as anyone else.
            cmd.env_remove("ANYAGENTS_OWNER_WORKDIR");
            if let Some(owner) = owner {
                cmd.env("ANYAGENTS_OWNER_WORKDIR", owner);
            }
            cmd.env("ANYAGENTS_TESTS", "1")
                .env("ANYAGENTS_AGENT_ROUTE", data)
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

#[tokio::test]
async fn stdio_turn_metadata_scopes_direct_and_nested_parent_sends() {
    use anyagents_core::{Incoming, NewAgent, ReceiveOutcome, Store};
    use serde_json::json;

    let dir = support::physical_tempdir();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let parent = store
        .create_agent(NewAgent::new("lead", "claude-code"))
        .unwrap();
    let mut input = NewAgent::new("worker", "claude-code");
    input.parent_id = Some(parent.id.clone());
    let child = store.create_agent(input).unwrap();
    for agent in [&parent, &child] {
        store.set_agent_state(&agent.id, "paused").unwrap();
    }
    store.set_agent_state(&child.id, "running").unwrap();
    let mut cases = Vec::new();
    for (case, tool, args, tag) in [
        (
            "direct",
            "send",
            json!({"to": ["parent"], "text": "direct send"}),
            true,
        ),
        (
            "untagged",
            "send",
            json!({"to": ["parent"], "text": "unscoped send"}),
            false,
        ),
    ] {
        let ReceiveOutcome::New(opener) = store.receive(&child.id, Incoming::new(case)).unwrap()
        else {
            panic!("fresh opener expected");
        };
        let token = store
            .prepare_task_turn_context(&child.id, opener.id.as_str())
            .unwrap()
            .unwrap();
        cases.push((opener, token, tool, args, tag));
    }
    store.set_agent_state(&child.id, "paused").unwrap();
    let _server = start_server(dir.path());
    let worker = connect(dir.path(), "worker").await;
    let lead = connect(dir.path(), "lead").await;
    for (opener, token, tool, args, tag) in cases {
        store.note_delivered(&child.id, opener.seq).unwrap();
        let mut params =
            CallToolRequestParams::new(tool).with_arguments(args.as_object().unwrap().clone());
        if tag {
            params.meta =
                Some(serde_json::from_value(json!({"io.spiron/turn-context": &token})).unwrap());
        } else {
            // Another authenticated agent cannot mark this child's turn.
            let mut wrong = CallToolRequestParams::new("send").with_arguments(
                json!({"to": ["worker"], "text": "refused"})
                    .as_object()
                    .unwrap()
                    .clone(),
            );
            wrong.meta =
                Some(serde_json::from_value(json!({"io.spiron/turn-context": &token})).unwrap());
            let pending_before = store.inbox(&child.id, None).unwrap().len();
            let response = tokio::time::timeout(Duration::from_secs(10), lead.call_tool(wrong))
                .await
                .unwrap()
                .unwrap();
            assert!(
                response.is_error == Some(true)
                    || response
                        .structured_content
                        .as_ref()
                        .and_then(|value| value["deliveries"].as_array())
                        .is_some_and(Vec::is_empty),
                "wrong author was not refused: {response:?}"
            );
            assert_eq!(store.inbox(&child.id, None).unwrap().len(), pending_before);
        }
        let result = tokio::time::timeout(Duration::from_secs(10), worker.call_tool(params))
            .await
            .unwrap()
            .unwrap();
        let result: serde_json::Value = ok_json(result);
        assert_eq!(
            result["deliveries"].as_array().map(Vec::len),
            Some(1),
            "{result:?}"
        );
        assert!(
            store
                .note_turn_completed_with_output_for(
                    &child.id,
                    opener.seq,
                    Some((opener.id.as_str(), "final answer"))
                )
                .unwrap()
        );
    }
    let rows = store.inbox(&parent.id, None).unwrap();
    for text in ["direct send", "unscoped send"] {
        assert_eq!(
            rows.iter().filter(|row| row.text == text).count(),
            1,
            "{rows:?}"
        );
    }
    let returns: Vec<_> = rows
        .iter()
        .filter(|row| row.text.starts_with("<task_return "))
        .collect();
    assert_eq!(returns.len(), 1, "{rows:?}");
    assert!(returns[0].text.contains(">final answer</task_return>"));
    worker.cancel().await.unwrap();
    lead.cancel().await.unwrap();
}

fn error_text(result: &CallToolResult) -> String {
    assert_eq!(result.is_error, Some(true), "{result:?}");
    match result.content.first() {
        Some(ContentBlock::Text(text)) => text.text.clone(),
        other => panic!("expected text error, got {other:?}"),
    }
}

#[tokio::test]
async fn configured_owner_catalog_controls_mcp_discovery_after_removal() {
    let dir = support::physical_tempdir();
    let owner = dir.path().join("owner");
    let created = bin()
        .env("PATH", machine_path(dir.path()))
        .arg("--data")
        .arg(dir.path())
        .args([
            "agents",
            "create",
            "catalog-reader",
            "--harness",
            "claude-code",
        ])
        .output()
        .unwrap();
    assert!(
        created.status.success(),
        "{}",
        String::from_utf8_lossy(&created.stderr)
    );
    for root in [".agents/agents", "agents-templates", "archetypes"] {
        let root = dir.path().join(root);
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("retired.md"), "Stale role.").unwrap();
        std::fs::write(root.join("worker.md"), "Wrong role.").unwrap();
    }
    let _server = start_server(dir.path());
    let client = connect_with_owner(dir.path(), "catalog-reader", Some(&owner)).await;
    for state in ["missing", "empty", "populated", "removed"] {
        let catalog = owner.join(".agents/agents");
        match state {
            "empty" => std::fs::create_dir_all(&catalog).unwrap(),
            "populated" => std::fs::write(catalog.join("worker.md"), "Current role.").unwrap(),
            "removed" => std::fs::remove_file(catalog.join("worker.md")).unwrap(),
            _ => {}
        }
        let tools = client.list_all_tools().await.unwrap();
        let role_tools: Vec<_> = tools
            .iter()
            .filter(|tool| {
                tool.input_schema
                    .get("properties")
                    .and_then(serde_json::Value::as_object)
                    .is_some_and(|props| props.contains_key("role"))
            })
            .collect();
        assert!(!role_tools.is_empty());
        for tool in role_tools {
            let description = tool.description.as_deref().unwrap_or_default();
            assert!(!description.contains("`retired`"), "{state}: {description}");
            assert!(description.contains("`general`"), "{state}: {description}");
            if state == "populated" {
                assert!(description.contains("`worker`"), "{state}: {description}");
                assert!(
                    description.find("`general`").unwrap() < description.find("`worker`").unwrap(),
                    "{state}: {description}"
                );
            } else {
                assert!(!description.contains("`worker`"), "{state}: {description}");
            }
        }
    }
}

#[tokio::test]
async fn canonical_conversation_raw_range_crosses_mcp_and_authenticated_socket() {
    let dir = support::physical_tempdir();
    let bytes = b"mcprawneedle \0\xff exact binary tail";
    for name in ["range-own", "range-peer"] {
        let created = bin()
            .env("PATH", machine_path(dir.path()))
            .arg("--data")
            .arg(dir.path())
            .args(["agents", "create", name, "--harness", "claude-code"])
            .output()
            .unwrap();
        assert!(
            created.status.success(),
            "{}",
            String::from_utf8_lossy(&created.stderr)
        );
        let root = dir
            .path()
            .join("agents")
            .join(created_id(dir.path(), name))
            .join("memory");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("MEMORY.md"), bytes).unwrap();
    }
    let _server = start_server(dir.path());
    let own = connect(dir.path(), "range-own").await;
    let peer = connect(dir.path(), "range-peer").await;
    let found: serde_json::Value = ok_json(
        call(
            &own,
            "conversation",
            serde_json::json!({"scope":"memory","query":"mcprawneedle"}),
        )
        .await,
    );
    assert_eq!(found["turns"].as_array().unwrap().len(), 1);
    let hit = &found["turns"][0];
    let read: serde_json::Value = ok_json(call(&own, "conversation", serde_json::json!({
        "scope":"memory", "raw_id":hit["id"], "raw_sha256":hit["raw"]["sha256"], "raw_offset":12, "raw_length":8
    })).await);
    let actual: Vec<u8> = serde_json::from_value(read["turns"][0]["raw_bytes"].clone()).unwrap();
    assert_eq!(actual, bytes[12..20]);
    assert_eq!(read["turns"][0]["window"]["length"], 8);
    assert_eq!(read["turns"][0]["raw"]["length"], bytes.len());
    let peer_found: serde_json::Value = ok_json(
        call(
            &peer,
            "conversation",
            serde_json::json!({"scope":"memory","query":"mcprawneedle"}),
        )
        .await,
    );
    let peer_hit = &peer_found["turns"][0];
    let denied = call(&own, "conversation", serde_json::json!({"scope":"memory", "raw_id":peer_hit["id"], "raw_sha256":peer_hit["raw"]["sha256"], "raw_length":8})).await;
    assert!(error_text(&denied).contains("raw document not found in authorized scope"));
    let stale = call(&own, "conversation", serde_json::json!({"scope":"memory", "raw_id":hit["id"], "raw_sha256":"0".repeat(64), "raw_length":8})).await;
    assert!(error_text(&stale).contains("raw document changed since search"));
    own.cancel().await.unwrap();
    peer.cancel().await.unwrap();
}

#[tokio::test]
#[ignore = "intermittent ENOENT in MCP tool calls on macOS CI (push 43264718226 on 5c7e890a, same tree green in queue 43263904677); tracking: https://github.com/SylphxAI/anyagents/issues/216"]
async fn mcp_tools_use_the_server_socket() {
    let dir = support::physical_tempdir();
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
            .args(["agents", "create", name, "--harness", "claude-code"])
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
    assert_eq!(
        names,
        [
            "agents",
            "send",
            "report_bug",
            "inbox",
            "conversation",
            "memory",
            "schedules",
            "groups",
        ]
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>(),
        "expected universal MCP surface on stdio session, got {names:?}"
    );

    let listed: AgentsResponse = ok_json(call(&alpha, "agents", serde_json::json!({})).await);
    assert_eq!(listed.agents.len(), 2);
    let alpha_row = listed
        .agents
        .iter()
        .find(|agent| agent.name == "alpha")
        .expect("alpha");
    assert_eq!(alpha_row.harness, "claude-code");
    assert_eq!(alpha_row.state, "running");
    // The level the Claude Code adapter holds before a process announced itself.
    assert_eq!(alpha_row.delivery["level"], "next-step");
    assert_eq!(alpha_row.delivery["idempotent"], true);
    assert!(
        alpha_row.delivery["proof"]
            .as_str()
            .is_some_and(|p| !p.is_empty())
    );
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

    let own: ConversationResponse =
        ok_json(call(&alpha, "conversation", serde_json::json!({})).await);
    assert!(
        own.turns.iter().all(|t| t.direction == "out"),
        "{:?}",
        own.turns
    );

    let got: ConversationResponse =
        ok_json(call(&beta, "conversation", serde_json::json!({})).await);
    assert_eq!(got.turns.len(), 2);
    assert_eq!(got.turns[0].text, "hello");
    let alpha_id = anyagents_core::Store::open(dir.path().join("state.db"))
        .unwrap()
        .agent_by_name("alpha")
        .unwrap()
        .id;
    assert_eq!(got.turns[0].from, format!("agent:{alpha_id}"));

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
    let in_room: ConversationResponse =
        ok_json(call(&beta, "conversation", serde_json::json!({})).await);
    assert!(
        in_room.turns.iter().any(|line| line.text == "room line"),
        "{:?}",
        in_room.turns.len()
    );
    let searched_conv: ConversationResponse = ok_json(
        call(
            &alpha,
            "conversation",
            serde_json::json!({"query": "room line", "limit": 5}),
        )
        .await,
    );
    assert!(
        searched_conv
            .turns
            .iter()
            .any(|line| line.text == "room line"),
        "{:?}",
        searched_conv.turns
    );
    let searched: ConversationResponse = ok_json(
        call(
            &alpha,
            "search",
            serde_json::json!({"query": "room line", "limit": 5}),
        )
        .await,
    );
    assert!(
        searched.turns.iter().any(|line| line.text == "room line"),
        "{:?}",
        searched.turns
    );

    let missing = call(
        &alpha,
        "send",
        serde_json::json!({"to": ["beta", "missing"], "text": "nope"}),
    )
    .await;
    assert!(error_text(&missing).contains("agent not found"));
    // beta holds the two addressed lines and the room line. A failed send to a
    // missing agent wrote nothing.
    let since: ConversationResponse =
        ok_json(call(&beta, "conversation", serde_json::json!({})).await);
    assert_eq!(since.turns.len(), 3);

    let empty = call(
        &alpha,
        "send",
        serde_json::json!({"to": [], "text": "nope"}),
    )
    .await;
    assert!(error_text(&empty).contains("send needs an address"));

    // Peer messaging and shared groups do not grant private conversation access.
    let peer_conv = call(
        &alpha,
        "conversation",
        serde_json::json!({"agent": "beta", "limit": 2}),
    )
    .await;
    assert!(error_text(&peer_conv).contains("cannot read private conversation"));

    // Pure internal group creation & membership via `groups` MCP tool.
    let joined_group: GroupsResponse = ok_json(
        call(
            &alpha,
            "groups",
            serde_json::json!({"action": "join", "group": "brain-trust"}),
        )
        .await,
    );
    assert_eq!(joined_group.groups[0].name, "brain-trust");
    assert!(
        joined_group.groups[0]
            .members
            .contains(&"alpha".to_string())
    );
    let _: GroupsResponse = ok_json(
        call(
            &alpha,
            "groups",
            serde_json::json!({"action": "join", "group": "brain-trust", "agent": "beta"}),
        )
        .await,
    );
    let all_groups: GroupsResponse =
        ok_json(call(&alpha, "groups", serde_json::json!({"action": "list"})).await);
    assert!(
        all_groups
            .groups
            .iter()
            .any(|g| g.name == "brain-trust" && g.members.len() == 2)
    );

    // Sub-agent spawning (`spawn`), `parent`/`children`/`all` addressing, `archive`, and auto-wake on send.
    let alpha_id = created_id(dir.path(), "alpha");
    let spawned: AgentView = ok_json(
        call(
            &alpha,
            "spawn",
            serde_json::json!({
                "name": "worker-1",
                "prompt": "initial subagent task"
            }),
        )
        .await,
    );
    assert_eq!(spawned.name, "worker-1");
    assert_eq!(spawned.parent.as_deref(), Some(alpha_id.as_str()));
    assert_eq!(spawned.state, "running");

    // Prove the canonical override selected the live fixture through the actual
    // server launch path, rather than an inherited override or PATH fallback.
    let marker = dir
        .path()
        .join("agents")
        .join(&spawned.id)
        .join("config")
        .join("fixture-claude-started");
    let deadline = Instant::now() + support::WAIT;
    while !marker.is_file() {
        if Instant::now() >= deadline {
            let views: serde_json::Value =
                ok_json(call(&alpha, "agents", serde_json::json!({"state":"all"})).await);
            panic!("canonical Claude fixture was not launched: {views}");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(
        std::fs::read_to_string(&marker).unwrap().trim(),
        dir.path().join("bin/fixture-claude").to_str().unwrap()
    );
    let live: AgentsResponse = ok_json(
        call(
            &alpha,
            "agents",
            serde_json::json!({"action":"list", "state":"all"}),
        )
        .await,
    );
    assert_eq!(
        live.agents
            .iter()
            .find(|row| row.id == spawned.id)
            .unwrap()
            .process,
        "alive"
    );

    // Parent can read sub-agent's conversation by `agt_...` ID.
    let worker_conv: ConversationResponse = ok_json(
        call(
            &alpha,
            "conversation",
            serde_json::json!({"agent": &spawned.id}),
        )
        .await,
    );
    assert_eq!(worker_conv.turns.len(), 1);
    assert_eq!(worker_conv.turns[0].text, "initial subagent task");

    // Parent can broadcast to `children`.
    let to_children: SendResponse = ok_json(
        call(
            &alpha,
            "send",
            serde_json::json!({"to": ["children"], "text": "for my children"}),
        )
        .await,
    );
    assert_eq!(to_children.deliveries.len(), 1);
    assert_eq!(to_children.deliveries[0].to, "agent:worker-1");

    // Multi-level hierarchy (`Root -> L1 -> L2`): `worker-1` spawns a child `worker-1-child` (`L2`),
    // and `worker-1-child` can message its direct `L1` parent via `to: ["parent"]` and `to: ["worker-1"]`.
    let worker_1 = connect(dir.path(), "worker-1").await;
    let spawned_l2: AgentView = ok_json(
        call(
            &worker_1,
            "agents",
            serde_json::json!({
                "action": "spawn",
                "name": "worker-1-child",
                "prompt": "nested L2 child"
            }),
        )
        .await,
    );
    assert_eq!(spawned_l2.parent.as_deref(), Some(spawned.id.as_str()));
    let worker_l2 = connect(dir.path(), "worker-1-child").await;
    let l2_to_parent: SendResponse = ok_json(
        call(
            &worker_l2,
            "send",
            serde_json::json!({"to": ["parent"], "text": "L2 reporting to L1 parent"}),
        )
        .await,
    );
    assert_eq!(l2_to_parent.deliveries.len(), 1);
    assert_eq!(l2_to_parent.deliveries[0].to, "agent:worker-1");

    // An ancestor reaches every non-isolated descendant: grandparent `alpha` may wake, steer,
    // pause and archive private L2 sub-agent `worker-1-child`, so it may also message it.
    let grandparent_to_l2: SendResponse = ok_json(
        call(
            &alpha,
            "send",
            serde_json::json!({"to": ["worker-1-child"], "text": "grandparent to L2"}),
        )
        .await,
    );
    assert_eq!(grandparent_to_l2.deliveries[0].to, "agent:worker-1-child");
    // Unrelated root `beta` still cannot reach it, and the refusal names the route.
    let outsider_to_l2_err = error_text(
        &call(
            &beta,
            "send",
            serde_json::json!({"to": ["worker-1-child"], "text": "skipping L1 to L2"}),
        )
        .await,
    );
    assert!(
        outsider_to_l2_err.contains("cannot message sub-agent `worker-1-child` directly")
            && outsider_to_l2_err.contains("Message `agent:worker-1` instead"),
        "unexpected error: {outsider_to_l2_err}"
    );

    // Sub-agent communication encapsulation (`public: false` by default):
    // 1. Spawn a sibling `worker-2` under `alpha` (defaults to `public: false`).
    // 2. Verify `worker-1` cannot DM `worker-2` directly or via `siblings` while `public: false`.
    // 3. Verify Parent `alpha` can opt `worker-2` in via `describe(public: true)`, after which `worker-1` CAN DM `worker-2`.
    // 4. Verify Parent `alpha` can also put `public: false` sub-agents into a shared virtual group (`group:collab`) so they can collaborate without becoming globally `public: true`.
    let spawned_sib: AgentView = ok_json(
        call(
            &alpha,
            "spawn",
            serde_json::json!({
                "name": "worker-2",
                "prompt": "sibling subagent task"
            }),
        )
        .await,
    );
    let p2p_direct_err = error_text(
        &call(
            &worker_1,
            "send",
            serde_json::json!({"to": ["worker-2"], "text": "sibling peer-to-peer attempt"}),
        )
        .await,
    );
    assert!(
        p2p_direct_err.contains("cannot message sub-agent `worker-2` directly")
            && p2p_direct_err.contains("it is your sibling")
            && p2p_direct_err.contains("`to: [\"parent\"]`")
            && p2p_direct_err.contains("scope: \"siblings\""),
        "unexpected error: {p2p_direct_err}"
    );
    let p2p_alias_err = error_text(
        &call(
            &worker_1,
            "send",
            serde_json::json!({"to": ["siblings"], "text": "sibling broadcast attempt"}),
        )
        .await,
    );
    assert!(
        p2p_alias_err.contains("no reachable sibling sub-agents"),
        "unexpected error: {p2p_alias_err}"
    );

    // Parent `alpha` sets `public: true` on `worker-2` -> `worker-1` can now DM `worker-2`!
    let desc_pub: serde_json::Value = ok_json(
        call(
            &alpha,
            "describe",
            serde_json::json!({"agent": "worker-2", "public": true}),
        )
        .await,
    );
    assert_eq!(desc_pub["public"], true);
    let pub_dm: SendResponse = ok_json(
        call(
            &worker_1,
            "send",
            serde_json::json!({"to": ["worker-2"], "text": "allowed after parent set public: true"}),
        )
        .await,
    );
    assert_eq!(pub_dm.deliveries[0].to, "agent:worker-2");

    // Parent `alpha` sets `public: false` again, then adds `worker-1` and `worker-2` to a virtual group `collab`.
    let desc_priv: serde_json::Value = ok_json(
        call(
            &alpha,
            "describe",
            serde_json::json!({"agent": "worker-2", "public": false}),
        )
        .await,
    );
    assert_eq!(desc_priv["public"], false);
    for m in ["worker-1", "worker-2"] {
        let _: serde_json::Value = ok_json(
            call(
                &alpha,
                "groups",
                serde_json::json!({"action": "join", "group": "collab", "agent": m}),
            )
            .await,
        );
    }
    let grp_dm: SendResponse = ok_json(
        call(
            &worker_1,
            "send",
            serde_json::json!({"to": ["worker-2"], "text": "allowed via shared virtual group"}),
        )
        .await,
    );
    assert_eq!(grp_dm.deliveries[0].to, "agent:worker-2");
    for m in ["worker-1", "worker-2"] {
        let _: serde_json::Value = ok_json(
            call(
                &alpha,
                "groups",
                serde_json::json!({"action": "leave", "group": "collab", "agent": m}),
            )
            .await,
        );
    }

    let _: serde_json::Value = ok_json(
        call(
            &alpha,
            "agents",
            serde_json::json!({"action": "delete", "name": &spawned_sib.id}),
        )
        .await,
    );

    let _: AgentView = ok_json(
        call(
            &worker_1,
            "archive",
            serde_json::json!({"agent": &spawned_l2.id}),
        )
        .await,
    );

    // Root `send(to: ["all"])` / `send(to: ["peers"])` is strictly scoped to Root peers (`parent_id.is_none()`),
    // so even while `worker-1` is `running`, `to: ["all"]` delivers ONLY to `agent:beta` and never leaks into sub-agents!
    let to_all_while_worker_running: SendResponse = ok_json(
        call(
            &alpha,
            "send",
            serde_json::json!({"to": ["all"], "text": "root peer broadcast while worker running"}),
        )
        .await,
    );
    let running_all_recipients: Vec<&str> = to_all_while_worker_running
        .deliveries
        .iter()
        .map(|d| d.to.as_str())
        .collect();
    assert_eq!(running_all_recipients, vec!["agent:beta"]);
    assert!(to_all_while_worker_running.skipped.is_empty());

    // Another Root peer (`beta`) cannot directly message `alpha`'s private sub-agent (`worker-1`)!
    let cross_parent_err = error_text(
        &call(
            &beta,
            "send",
            serde_json::json!({"to": ["worker-1"], "text": "intruding into alpha's sub-agent"}),
        )
        .await,
    );
    assert!(
        cross_parent_err.contains("cannot message sub-agent `worker-1` directly")
            && cross_parent_err.contains("agent:alpha"),
        "unexpected error: {cross_parent_err}"
    );

    // Private group creation & access control: `alpha` creates private group `alpha-warroom` with `worker-1` (auto-joining `alpha`),
    // and unrelated Root peer `beta` is refused from joining or posting to `alpha-warroom` until it is made public or `alpha` adds `beta`.
    let warroom: GroupsResponse = ok_json(
        call(
            &alpha,
            "groups",
            serde_json::json!({"action": "join", "group": "alpha-warroom", "agent": &spawned.id}),
        )
        .await,
    );
    assert_eq!(warroom.groups[0].name, "alpha-warroom");
    assert!(!warroom.groups[0].public);
    assert_eq!(warroom.groups[0].members, vec!["alpha", "worker-1"]);

    // The reported public:true join payload must neither authorize a private
    // outsider nor mutate visibility through the real MCP adapter.
    let escalation = error_text(
        &call(&beta, "groups", serde_json::json!({"action":"join","group":"alpha-warroom","agent":"self","public":true})).await,
    );
    assert!(escalation.contains("is private"), "{escalation}");
    let evict = error_text(
        &call(
            &beta,
            "groups",
            serde_json::json!({"action":"leave","group":"alpha-warroom","agent":"worker-1"}),
        )
        .await,
    );
    assert!(evict.contains("removing another member"), "{evict}");
    let unchanged: GroupsResponse = ok_json(
        call(
            &alpha,
            "groups",
            serde_json::json!({"action":"members","group":"alpha-warroom"}),
        )
        .await,
    );
    assert!(!unchanged.groups[0].public);
    assert_eq!(unchanged.groups[0].members, vec!["alpha", "worker-1"]);

    let beta_intrude_group = error_text(
        &call(
            &beta,
            "groups",
            serde_json::json!({"action": "join", "group": "alpha-warroom"}),
        )
        .await,
    );
    assert!(
        beta_intrude_group.contains("is private"),
        "unexpected private group join error: {beta_intrude_group}"
    );
    let beta_post_private_group = error_text(
        &call(
            &beta,
            "send",
            serde_json::json!({"to": ["group:alpha-warroom"], "text": "uninvited post"}),
        )
        .await,
    );
    assert!(
        beta_post_private_group.contains("private group"),
        "unexpected private group post error: {beta_post_private_group}"
    );

    // Custody transfer (`agents` `action = "transfer"`): a non-admin parent cannot transfer a child
    // outside its own subtree (`beta`), while an Admin can transfer across root trees.
    let cross_tree_err = error_text(
        &call(
            &alpha,
            "agents",
            serde_json::json!({
                "action": "transfer",
                "name": &spawned.id,
                "parent": "beta",
                "prompt": "unauthorized cross-tree handoff"
            }),
        )
        .await,
    );
    assert!(
        cross_tree_err.contains("administrative authority required"),
        "unexpected error: {cross_tree_err}"
    );
    // Cross-tree transfer is strictly an operator control-plane operation.
    let sock = dir.path().join("anyagents.sock");
    let (tx_status, transferred_to_beta) = local_operator_call(
        &sock,
        "POST",
        &format!("/v1/agents/{}/transfer", spawned.id),
        &serde_json::json!({
            "parent": "beta",
            "prompt": "handing off worker-1 to beta"
        })
        .to_string(),
    );
    assert!(
        tx_status.contains("200"),
        "{tx_status} {transferred_to_beta}"
    );
    assert_eq!(transferred_to_beta["previous_parent"], "alpha");
    assert_eq!(transferred_to_beta["new_parent"], "beta");

    let beta_to_worker: SendResponse = ok_json(
        call(
            &beta,
            "send",
            serde_json::json!({"to": ["worker-1"], "text": "now beta is your parent"}),
        )
        .await,
    );
    assert_eq!(beta_to_worker.deliveries[0].to, "agent:worker-1");

    let (tx_back_status, transferred_back) = local_operator_call(
        &sock,
        "POST",
        &format!("/v1/agents/{}/transfer", spawned.id),
        &serde_json::json!({
            "parent": "alpha",
            "prompt": "returning worker-1 to alpha"
        })
        .to_string(),
    );
    assert!(
        tx_back_status.contains("200"),
        "{tx_back_status} {transferred_back}"
    );
    assert_eq!(transferred_back["previous_parent"], "beta");
    assert_eq!(transferred_back["new_parent"], "alpha");

    // Archive the sub-agent (`state = archived`), then pause `beta` to verify `to: ["all"]` reports paused Root peers in `skipped`.
    let archived: AgentView =
        ok_json(call(&alpha, "archive", serde_json::json!({"agent": &spawned.id})).await);
    assert_eq!(archived.state, "archived");

    // Also join `worker-1` to `crew` and pause `worker-1` via unified `agents` tool to verify `group:crew` reports paused members in `skipped`.
    let _: serde_json::Value = ok_json(
        call(
            &alpha,
            "groups",
            serde_json::json!({"action": "join", "group": "crew", "agent": &spawned.id}),
        )
        .await,
    );
    let paused_worker: AgentView = ok_json(
        call(
            &alpha,
            "agents",
            serde_json::json!({"action": "pause", "name": &spawned.id}),
        )
        .await,
    );
    assert_eq!(paused_worker.state, "paused");

    let to_crew_with_paused: SendResponse = ok_json(
        call(
            &alpha,
            "send",
            serde_json::json!({"to": ["group:crew"], "text": "group message while worker paused"}),
        )
        .await,
    );
    assert!(
        to_crew_with_paused
            .skipped
            .iter()
            .any(|s| s.to == "agent:worker-1" && s.reason == "paused"),
        "expected paused worker in group skipped receipt: {:?}",
        to_crew_with_paused.skipped
    );

    let archived: AgentView = ok_json(
        call(
            &alpha,
            "agents",
            serde_json::json!({"action": "archive", "name": &spawned.id}),
        )
        .await,
    );
    assert_eq!(archived.state, "archived");

    let to_all: SendResponse = ok_json(
        call(
            &alpha,
            "send",
            serde_json::json!({"to": ["all"], "text": "broadcast to active"}),
        )
        .await,
    );
    let all_recipients: Vec<&str> = to_all.deliveries.iter().map(|d| d.to.as_str()).collect();
    assert!(all_recipients.contains(&"agent:beta"), "{all_recipients:?}");
    assert!(
        !all_recipients.contains(&"agent:worker-1"),
        "{all_recipients:?}"
    );

    // A direct send, by immutable id or by name, never wakes the archived
    // sub-agent: it fails closed and names the explicit wake.
    for to in [spawned.id.as_str(), "worker-1"] {
        let refused = call(
            &alpha,
            "send",
            serde_json::json!({"to": [to], "text": "wake up and continue"}),
        )
        .await;
        assert_eq!(refused.is_error, Some(true), "{refused:?}");
        let text = format!("{refused:?}");
        assert!(text.contains("is archived"), "{text}");
        assert!(text.contains("agents(action="), "{text}");
    }
    let after_send: AgentsResponse = ok_json(
        call(
            &alpha,
            "agents",
            serde_json::json!({"action": "list", "state": "all"}),
        )
        .await,
    );
    let worker_row = after_send
        .agents
        .iter()
        .find(|a| a.id == spawned.id)
        .expect("worker-1");
    assert_eq!(worker_row.state, "archived");

    let woken: AgentView = ok_json(
        call(
            &alpha,
            "agents",
            serde_json::json!({"action": "wake", "name": &spawned.id}),
        )
        .await,
    );
    assert_eq!(woken.state, "running");
    let queued_send: SendResponse = ok_json(
        call(
            &alpha,
            "send",
            serde_json::json!({"to": [&spawned.id], "text": "continue after wake"}),
        )
        .await,
    );
    assert_eq!(queued_send.deliveries[0].to, "agent:worker-1");

    let baseline_worker: serde_json::Value = ok_json(
        call(
            &alpha,
            "agents",
            serde_json::json!({
                "action": "spawn",
                "name": "baseline-general"
            }),
        )
        .await,
    );
    let default_body = baseline_worker["instructions"].as_str().unwrap();
    assert!(
        default_body.starts_with("You are a cohesive end-to-end execution worker."),
        "{default_body}"
    );
    for route in ["agents", "subagents"] {
        let worker: serde_json::Value = ok_json(
            call(
                &alpha,
                route,
                serde_json::json!({
                    "action": "spawn",
                    "name": format!("{route}-separate-instructions"),
                    "description": "Public role",
                    "instructions": "Private instructions"
                }),
            )
            .await,
        );
        assert_eq!(worker["role"], "general");
        assert_eq!(worker["scope"], "private");
        assert_eq!(worker["description"], "Public role");
        assert_eq!(
            worker["instructions"],
            format!("{default_body}\n\nPrivate instructions")
        );
    }
    let legacy_worker: serde_json::Value = ok_json(
        call(
            &alpha,
            "agents",
            serde_json::json!({
                "action": "spawn",
                "name": "description-only",
                "description": "Legacy instructions"
            }),
        )
        .await,
    );
    assert_eq!(legacy_worker["role"], "general");
    assert_eq!(legacy_worker["scope"], "private");
    assert_eq!(legacy_worker["description"], "Legacy instructions");
    assert_eq!(
        legacy_worker["instructions"],
        format!("{default_body}\n\nLegacy instructions")
    );

    alpha.cancel().await.unwrap();
    beta.cancel().await.unwrap();
    drop(server);

    let inbox = bin()
        .arg("--data")
        .arg(dir.path())
        .args(["inbox", "beta", "--json"])
        .output()
        .unwrap();
    assert!(
        inbox.status.success(),
        "{}",
        String::from_utf8_lossy(&inbox.stderr)
    );
    // Two addressed lines, the room line, and the three `all` broadcast lines, each readable from the CLI too.
    let listed = String::from_utf8(inbox.stdout).unwrap();
    let response: serde_json::Value = serde_json::from_str(&listed).unwrap();
    assert_eq!(
        response["messages"].as_array().unwrap().len(),
        6,
        "{listed}"
    );
    assert!(listed.contains("room line"), "{listed}");
    assert!(listed.contains("broadcast to active"), "{listed}");
}

#[tokio::test]
async fn redaction_mcp_and_cli_share_authenticated_audited_api() {
    use anyagents_core::{NewAgent, Store, agent_config_dir};
    let dir = support::physical_tempdir();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let root = store
        .create_agent(NewAgent::new("redaction-root", "spiron"))
        .unwrap();
    let peer = store
        .create_agent(NewAgent::new("redaction-peer", "spiron"))
        .unwrap();
    let mut input = NewAgent::new("redaction-child", "spiron");
    input.parent_id = Some(root.id.clone());
    let child = store.create_agent(input).unwrap();
    for agent in [&root, &peer, &child] {
        store.set_agent_state(&agent.id, "paused").unwrap();
    }
    let config = agent_config_dir(dir.path(), &child.id);
    std::fs::create_dir_all(&config).unwrap();
    let transcript = config.join("sylphx-transcript.jsonl");
    std::fs::write(&transcript, "{\"type\":\"function_call_output\",\"call_id\":\"call_1\",\"output\":\"prefix syntheticcanary suffix\"}\n").unwrap();
    let _server = start_server(dir.path());
    let root_mcp = connect(dir.path(), &root.name).await;
    let peer_mcp = connect(dir.path(), &peer.name).await;
    let args = serde_json::json!({"action":"redact", "name":child.name, "message_id":"transcript:0", "span":{"unit":"chars","start":7,"end":22}, "reason":"accidental synthetic capture"});
    let denied = call(&peer_mcp, "agents", args.clone()).await;
    assert_eq!(denied.is_error, Some(true), "{denied:?}");
    assert!(
        serde_json::to_string(&denied)
            .unwrap()
            .contains("requires a target ancestor"),
        "{denied:?}"
    );
    assert!(
        std::fs::read_to_string(&transcript)
            .unwrap()
            .contains("syntheticcanary")
    );
    let receipt: serde_json::Value = ok_json(call(&root_mcp, "agents", args).await);
    assert_eq!(receipt["permission"], "conversation.redact");
    assert_eq!(receipt["full_scrub"], true);
    assert!(
        !std::fs::read_to_string(&transcript)
            .unwrap()
            .contains("syntheticcanary")
    );
    let cli = bin()
        .env("ANYAGENTS_AGENT_ID", root.id.as_str())
        .env(
            "ANYAGENTS_AGENT_CONFIG",
            agent_config_dir(dir.path(), &root.id),
        )
        .arg("--data")
        .arg(dir.path())
        .args([
            "redact",
            &child.name,
            "--message-id",
            "transcript:0",
            "--start",
            "7",
            "--end",
            "22",
            "--unit",
            "chars",
            "--reason",
            "same request replay",
        ])
        .output()
        .unwrap();
    assert!(
        cli.status.success(),
        "{}",
        String::from_utf8_lossy(&cli.stderr)
    );
    let replay: serde_json::Value = serde_json::from_slice(&cli.stdout).unwrap();
    assert_eq!(replay["audit_id"], receipt["audit_id"]);
    let invalid = call(&root_mcp, "agents", serde_json::json!({"action":"redact", "name":child.name,"message_id":"transcript:0","span":{"unit":"bytes","start":0,"end":9999},"reason":"invalid span check"})).await;
    assert_eq!(invalid.is_error, Some(true));
    assert!(
        serde_json::to_string(&invalid)
            .unwrap()
            .contains("span out of range"),
        "{invalid:?}"
    );
    root_mcp.cancel().await.unwrap();
    peer_mcp.cancel().await.unwrap();
}

#[tokio::test]
async fn session_reset_mcp_and_cli_share_quiescent_authorized_api() {
    use anyagents_core::{NewAgent, Store, host_session};
    let dir = support::physical_tempdir();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let parent = store
        .create_agent(NewAgent::new("session-parent", "spiron"))
        .unwrap();
    let peer = store
        .create_agent(NewAgent::new("session-peer", "spiron"))
        .unwrap();
    let mut input = NewAgent::new("session-child", "spiron");
    input.parent_id = Some(parent.id.clone());
    let child = store.create_agent(input).unwrap();
    for agent in [&parent, &peer, &child] {
        store.set_agent_state(&agent.id, "paused").unwrap();
    }
    let root = dir.path().join("agents").join(child.id.as_str());
    std::fs::create_dir_all(root.join("config")).unwrap();
    std::fs::create_dir_all(root.join("memory")).unwrap();
    std::fs::write(root.join("memory/MEMORY.md"), "first pin").unwrap();
    let old = host_session::ensure_active(&root, "spiron").unwrap();
    std::fs::write(root.join("config/sylphx-session-id"), "old-vendor\n").unwrap();
    std::fs::write(root.join("config/spiron-session-id"), "old-vendor\n").unwrap();
    std::fs::write(
        root.join("config/sylphx-transcript.jsonl"),
        "retained raw bytes",
    )
    .unwrap();
    std::fs::write(root.join("memory/MEMORY.md"), "second pin").unwrap();
    let server = start_server(dir.path());
    let parent_mcp = connect(dir.path(), &parent.name).await;
    let peer_mcp = connect(dir.path(), &peer.name).await;
    let before = std::fs::read(root.join("sessions/registry.json")).unwrap();
    let denied = call(
        &peer_mcp,
        "agents",
        serde_json::json!({"action":"reset_session","name":child.name}),
    )
    .await;
    assert_eq!(denied.is_error, Some(true));
    assert_eq!(
        std::fs::read(root.join("sessions/registry.json")).unwrap(),
        before
    );
    let process = store.try_lock_agent(&child.id).unwrap();
    let busy = call(
        &parent_mcp,
        "agents",
        serde_json::json!({"action":"new_session","name":child.name}),
    )
    .await;
    assert_eq!(busy.is_error, Some(true));
    assert_eq!(
        std::fs::read(root.join("sessions/registry.json")).unwrap(),
        before
    );
    assert_eq!(
        std::fs::read(root.join("config/sylphx-session-id")).unwrap(),
        b"old-vendor\n"
    );
    assert_eq!(
        std::fs::read(root.join("config/spiron-session-id")).unwrap(),
        b"old-vendor\n"
    );
    drop(process);
    let result: serde_json::Value = ok_json(
        call(
            &parent_mcp,
            "agents",
            serde_json::json!({"action":"reset_session","name":child.name}),
        )
        .await,
    );
    let second = host_session::active(&root).unwrap().unwrap();
    assert_eq!(result["generation"], second.generation);
    assert_ne!(second.generation, old.generation);
    assert!(second.memory.section.contains("second pin"));
    assert!(!root.join("config/spiron-session-id").exists());
    assert_eq!(
        std::fs::read(root.join("config/sylphx-session-id")).unwrap(),
        b"old-vendor\n"
    );
    std::fs::write(root.join("memory/MEMORY.md"), "third pin").unwrap();
    let cli = bin()
        .arg("--data")
        .arg(dir.path())
        .args(["agents", "new-session", &child.name])
        .output()
        .unwrap();
    assert!(
        cli.status.success(),
        "{}",
        String::from_utf8_lossy(&cli.stderr)
    );
    let result: serde_json::Value = serde_json::from_slice(&cli.stdout).unwrap();
    let third = host_session::active(&root).unwrap().unwrap();
    assert_eq!(result["generation"], third.generation);
    assert_ne!(third.generation, second.generation);
    assert!(third.memory.section.contains("third pin"));
    assert_eq!(
        std::fs::read(root.join("config/sylphx-transcript.jsonl")).unwrap(),
        b"retained raw bytes"
    );
    assert_eq!(
        host_session::resume_vendor(&root, "spiron", "old-vendor").unwrap(),
        old
    );
    parent_mcp.cancel().await.unwrap();
    peer_mcp.cancel().await.unwrap();
    drop(server);
}

#[tokio::test]
async fn describe_preserves_config_permissions_without_vendor_restart_writers() {
    let dir = support::physical_tempdir();
    let created = bin()
        .env("PATH", machine_path(dir.path()))
        .arg("--data")
        .arg(dir.path())
        .args(["agents", "create", "alpha", "--harness", "claude-code"])
        .output()
        .unwrap();
    assert!(
        created.status.success(),
        "{}",
        String::from_utf8_lossy(&created.stderr)
    );
    // Pause before startup so describe is the only config writer.
    let store = anyagents_core::Store::open(dir.path().join("state.db")).unwrap();
    let agent = store.agent_by_name("alpha").unwrap();
    store.set_agent_state(&agent.id, "paused").unwrap();
    let server = start_server(dir.path());
    let alpha = connect(dir.path(), "alpha").await;
    let alpha_id = created_id(dir.path(), "alpha");
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
        let alpha_cfg = dir.path().join("agents").join(&alpha_id).join("config");
        std::fs::set_permissions(&alpha_cfg, std::fs::Permissions::from_mode(0o0700)).unwrap();
        let before_gid = std::fs::metadata(&alpha_cfg).unwrap().gid();
        let self_mut_err = error_text(
            &call(
                &alpha,
                "describe",
                serde_json::json!({"agent": "self", "description": "Unauthorized self mutation."}),
            )
            .await,
        );
        assert!(
            self_mut_err.contains("administrative authority required"),
            "unexpected error: {self_mut_err}"
        );
        // Operator updates root description via control-plane REST API, preserving directory permissions.
        let sock = dir.path().join("anyagents.sock");
        let (desc_status, desc_res) = local_operator_call(
            &sock,
            "POST",
            "/v1/agents/alpha/description",
            &serde_json::json!({"description": "Standing owner role description."}).to_string(),
        );
        assert!(desc_status.contains("200"), "{desc_status} {desc_res}");
        assert_eq!(desc_res["system_prompt_synced"], true);
        let after_meta = std::fs::metadata(&alpha_cfg).unwrap();
        assert_eq!(after_meta.permissions().mode() & 0o7777, 0o0700);
        assert_eq!(after_meta.gid(), before_gid);
    }

    alpha.cancel().await.unwrap();
    drop(server);
}

#[tokio::test]
async fn history_search_defaults_and_all_never_escape_a_private_childs_tree() {
    use anyagents_core::{Incoming, Store};
    let dir = support::physical_tempdir();
    for name in ["coordinator", "planner", "tester", "builder"] {
        let created = bin()
            .env("PATH", machine_path(dir.path()))
            .arg("--data")
            .arg(dir.path())
            .args(["agents", "create", name, "--harness", "claude-code"])
            .output()
            .unwrap();
        assert!(created.status.success(), "{created:?}");
    }
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let coordinator = store.agent_by_name("coordinator").unwrap();
    let planner = store.agent_by_name("planner").unwrap();
    let tester = store.agent_by_name("tester").unwrap();
    let builder = store.agent_by_name("builder").unwrap();
    store
        .set_agent_parent(&planner.id, Some(&coordinator.id))
        .unwrap();
    store
        .set_agent_parent(&tester.id, Some(&coordinator.id))
        .unwrap();
    store
        .set_agent_parent(&builder.id, Some(&planner.id))
        .unwrap();
    // Synthetic bodies only. These identifiers reproduce the reported search
    // shape without importing any production conversation content.
    for (id, text) in [
        (
            &coordinator.id,
            "C msg_01m6svh4znevfapf3mmpsnk0a4 #2291 private parent",
        ),
        (
            &tester.id,
            "C msg_01m6svrywtez39wvnttdvz53xf #2291 private sibling",
        ),
        (&planner.id, "C #2291 authorized own mailbox"),
        (&builder.id, "C #2291 authorized descendant"),
    ] {
        store
            .receive(
                id,
                Incoming {
                    text: text.into(),
                    sender: Some(
                        if id == &coordinator.id {
                            "agent:planner"
                        } else {
                            "agent:coordinator"
                        }
                        .into(),
                    ),
                    source_time_ms: None,
                    source_key: None,
                    reply_to: None,
                    quote: None,
                    origin: None,
                    received_at_ms: Some(1_000),
                },
            )
            .unwrap();
        store
            .record_tool_event(id, "browser", &format!("C {text}"))
            .unwrap();
    }
    store
        .record_outbound(
            &coordinator.id,
            "telegram:chat:-100",
            None,
            "C out_msg_01m6svqn4nesw9ws13fjy48kzt #2291 private outbound",
        )
        .unwrap();
    // Keep synthetic history stable while exercising the real API and MCP path.
    for agent in [&coordinator, &planner, &tester, &builder] {
        store.set_agent_state(&agent.id, "paused").unwrap();
    }
    let server = start_server(dir.path());
    let client = connect(dir.path(), "planner").await;
    let native: ConversationResponse = ok_json(
        call(
            &client,
            "conversation",
            serde_json::json!({"with":"planner", "direction":"in", "query":"C", "limit":4}),
        )
        .await,
    );
    assert!(!native.turns.is_empty());
    assert!(
        native
            .turns
            .iter()
            .all(|turn| turn.text.contains("authorized own mailbox")),
        "{:?}",
        native.turns
    );
    for args in [
        serde_json::json!({"agent":"all", "query":"msg_01m6svh4znevfapf3mmpsnk0a4", "queries":["#2291", "msg_01m6svrywtez39wvnttdvz53xf", "out_msg_01m6svqn4nesw9ws13fjy48kzt"], "radius":10}),
        serde_json::json!({"agent":"*", "query":"C", "radius":10}),
        serde_json::json!({"query":"C", "only_tools":true, "tool_name":"browser", "radius":10}),
        serde_json::json!({"agent":"all", "range":"seq:1-1", "direction":"in", "radius":10}),
        serde_json::json!({"agent":"all", "limit":50}),
    ] {
        let response: ConversationResponse =
            ok_json(call(&client, "conversation", args.clone()).await);
        assert!(!response.turns.is_empty(), "{args}");
        assert!(
            response
                .turns
                .iter()
                .all(|turn| turn.text.contains("authorized")),
            "{args}: {:?}",
            response.turns
        );
    }
    for target in [
        "coordinator",
        "tester",
        coordinator.id.as_str(),
        tester.id.as_str(),
        // A relation word names the same agent it names in `send`.
        "parent",
    ] {
        for tool in ["conversation", "search"] {
            let denied = call(
                &client,
                tool,
                serde_json::json!({"agent":target, "query":"C"}),
            )
            .await;
            let text = error_text(&denied);
            assert!(text.contains("cannot read private conversation"), "{text}");
            assert!(text.contains("conversation({with:"), "{text}");
        }
    }
    client.cancel().await.unwrap();
    drop(server);
}

/// The shapes agents were seen writing reach the call they meant, and a shape
/// that cannot be read names the one that can.
#[tokio::test]
async fn tool_contracts_accept_the_shapes_agents_write() {
    let dir = support::physical_tempdir();
    for name in ["alpha", "beta"] {
        let created = bin()
            .env("PATH", machine_path(dir.path()))
            .arg("--data")
            .arg(dir.path())
            .args(["agents", "create", name, "--harness", "claude-code"])
            .output()
            .unwrap();
        assert!(created.status.success(), "{created:?}");
    }
    let server = start_server(dir.path());
    let alpha = connect(dir.path(), "alpha").await;

    // `schedules`: the action names the trigger, unused fields are `"null"`.
    let watched: serde_json::Value = ok_json(
        call(
            &alpha,
            "schedules",
            serde_json::json!({"action": "watch", "watch": "agent:idle:beta", "prompt": "beta idle", "at": "null", "every": null, "id": "null", "agent": "null"}),
        )
        .await,
    );
    assert_eq!(watched["schedule"]["kind"], "watch", "{watched}");
    let at: serde_json::Value = ok_json(
        call(
            &alpha,
            "schedules",
            serde_json::json!({"action": "at", "spec": "10m", "prompt": "check back"}),
        )
        .await,
    );
    assert_eq!(at["schedule"]["kind"], "at", "{at}");
    let listed: serde_json::Value =
        ok_json(call(&alpha, "schedules", serde_json::json!({"action": "list"})).await);
    assert_eq!(listed["schedules"].as_array().unwrap().len(), 2, "{listed}");
    let unknown = error_text(
        &call(
            &alpha,
            "schedules",
            serde_json::json!({"action": "update", "id": "sch_x"}),
        )
        .await,
    );
    assert!(
        unknown.contains("unknown schedule action `update`")
            && unknown.contains("{action: \"create\", watch: \"webhook:deploy-ready\""),
        "{unknown}"
    );
    let two = error_text(
        &call(
            &alpha,
            "schedules",
            serde_json::json!({"action": "create", "watch": "agent:idle:beta", "at": "30m", "prompt": "p"}),
        )
        .await,
    );
    assert!(
        two.contains("exactly one trigger") && two.contains("`at` and `watch`"),
        "{two}"
    );

    // `send`: optional fields a harness filled with `"null"` are absent.
    let sent: SendResponse = ok_json(
        call(
            &alpha,
            "send",
            serde_json::json!({"action": "send", "to": ["agent:beta"], "text": "report", "article": "null", "checklist": "null", "emoji": "null", "idempotency_key": "null", "location": "null", "media": "null", "options": "null", "poll": "null", "quote": "null", "reply_to": "null", "target": "null"}),
        )
        .await,
    );
    assert_eq!(sent.deliveries[0].to, "agent:beta");

    // `agents`: `self` names the caller.
    let inspected: serde_json::Value = ok_json(
        call(
            &alpha,
            "agents",
            serde_json::json!({"action": "inspect", "name": "self", "limit": 1}),
        )
        .await,
    );
    assert_eq!(inspected["agent"]["name"], "alpha", "{inspected}");
    // A root agent has no parent, and says so.
    let no_parent = error_text(
        &call(
            &alpha,
            "conversation",
            serde_json::json!({"agent": "parent"}),
        )
        .await,
    );
    assert!(
        no_parent.contains("is a root agent and has no parent"),
        "{no_parent}"
    );
    alpha.cancel().await.unwrap();

    // A child's `parent` in `conversation` is its parent, and the refusal
    // to read that private history names the route that works.
    let store = anyagents_core::Store::open(dir.path().join("state.db")).unwrap();
    let alpha_row = store.agent_by_name("alpha").unwrap();
    let beta_row = store.agent_by_name("beta").unwrap();
    store
        .set_agent_parent(&beta_row.id, Some(&alpha_row.id))
        .unwrap();
    let beta = connect(dir.path(), "beta").await;
    let parent_history = error_text(
        &call(
            &beta,
            "conversation",
            serde_json::json!({"agent": "parent", "limit": 5}),
        )
        .await,
    );
    assert!(
        parent_history.contains("cannot read private conversation history of `alpha`")
            && parent_history.contains("conversation({with: \"alpha\"})"),
        "{parent_history}"
    );
    let with_parent: serde_json::Value = ok_json(
        call(
            &beta,
            "conversation",
            serde_json::json!({"with": "parent", "limit": 5}),
        )
        .await,
    );
    let everything: serde_json::Value =
        ok_json(call(&beta, "conversation", serde_json::json!({"limit": 20})).await);
    let with_name: serde_json::Value = ok_json(
        call(
            &beta,
            "conversation",
            serde_json::json!({"with": "alpha", "limit": 5}),
        )
        .await,
    );
    assert_eq!(with_parent["turns"], with_name["turns"]);
    assert!(
        with_parent["turns"]
            .as_array()
            .unwrap()
            .iter()
            .any(|turn| turn["text"].as_str().unwrap_or("").contains("report")),
        "with parent: {with_parent}\nall: {everything}"
    );
    beta.cancel().await.unwrap();
    drop(server);
}

#[tokio::test]
async fn mcp_spawn_and_create_default_to_general_and_schema_lists_general_first() {
    let dir = support::physical_tempdir();
    let owner = dir.path().join("owner");
    std::fs::create_dir_all(owner.join(".agents/agents")).unwrap();
    // Alphabetically earlier roles on disk must still appear after built-in `general`.
    std::fs::write(
        owner.join(".agents/agents/assistant.md"),
        "---\nname: assistant\ndescription: Assistant role\n---\nAssistant body.\n",
    )
    .unwrap();
    std::fs::write(
        owner.join(".agents/agents/counsel.md"),
        "---\nname: counsel\ndescription: Counsel role\n---\nCounsel body.\n",
    )
    .unwrap();

    let created_alpha = bin()
        .env("PATH", machine_path(dir.path()))
        .arg("--data")
        .arg(dir.path())
        .args(["agents", "create", "alpha", "--harness", "claude-code"])
        .output()
        .unwrap();
    assert!(
        created_alpha.status.success(),
        "{}",
        String::from_utf8_lossy(&created_alpha.stderr)
    );
    let _server = start_server(dir.path());
    let alpha = connect_with_owner(dir.path(), "alpha", Some(&owner)).await;

    let tools = alpha.list_all_tools().await.unwrap();
    let agents_tool = tools.iter().find(|t| t.name == "agents").unwrap();
    let tool_desc = agents_tool.description.as_deref().unwrap_or_default();
    assert!(
        tool_desc.contains("Available roles (`general` is the built-in default): `general`"),
        "{tool_desc}"
    );
    let catalog_start = tool_desc.find("Available roles (").unwrap();
    let catalog = &tool_desc[catalog_start..];
    let general_pos = catalog
        .find("`general`")
        .unwrap_or_else(|| panic!("{catalog}"));
    let assistant_pos = catalog
        .find("`assistant`")
        .unwrap_or_else(|| panic!("{catalog}"));
    let counsel_pos = catalog
        .find("`counsel`")
        .unwrap_or_else(|| panic!("{catalog}"));
    assert!(
        general_pos < assistant_pos && general_pos < counsel_pos,
        "{catalog}"
    );

    // Omitted, empty, and explicit `general` all resolve to built-in `general` with `private` scope.
    let mut expected_default_body: Option<String> = None;
    for (tool_name, payload) in [
        (
            "agents",
            serde_json::json!({"action": "spawn", "name": "spawn-omitted"}),
        ),
        (
            "agents",
            serde_json::json!({"action": "spawn", "name": "spawn-empty", "role": ""}),
        ),
        (
            "agents",
            serde_json::json!({"action": "spawn", "name": "spawn-explicit", "role": "general"}),
        ),
        (
            "spawn",
            serde_json::json!({"name": "legacy-spawn-empty", "role": ""}),
        ),
    ] {
        let created: serde_json::Value = ok_json(call(&alpha, tool_name, payload).await);
        assert_eq!(created["role"], "general", "{created:?}");
        assert_eq!(created["scope"], "private", "{created:?}");
        let instructions = created["instructions"].as_str().unwrap();
        if let Some(expected) = &expected_default_body {
            assert_eq!(instructions, expected, "{created:?}");
        } else {
            assert!(
                instructions.starts_with("You are a cohesive end-to-end execution worker."),
                "{instructions}"
            );
            expected_default_body = Some(instructions.to_string());
        }
    }

    // Explicit `public: true` / `public: false` without `scope` is respected on MCP spawn.
    for (public_flag, expected_scope) in [(true, "public"), (false, "private")] {
        let flagged: serde_json::Value = ok_json(
            call(
                &alpha,
                "agents",
                serde_json::json!({
                    "action": "spawn",
                    "name": format!("spawn-public-{public_flag}"),
                    "public": public_flag
                }),
            )
            .await,
        );
        assert_eq!(flagged["role"], "general", "{flagged:?}");
        assert_eq!(flagged["scope"], expected_scope, "{flagged:?}");
    }

    // Adding `.agents/agents/general.md` overrides the built-in `general` template.
    std::fs::write(
        owner.join(".agents/agents/general.md"),
        "---\nname: general\ndescription: Owner General Override\nscope: public\n---\nOwner general override body.\n",
    )
    .unwrap();
    let overridden: serde_json::Value = ok_json(
        call(
            &alpha,
            "agents",
            serde_json::json!({"action": "spawn", "name": "spawn-overridden", "workdir": &owner}),
        )
        .await,
    );
    assert_eq!(overridden["role"], "general");
    assert_eq!(overridden["scope"], "public");
    assert_eq!(overridden["description"], "Owner General Override");
    assert!(
        overridden["instructions"]
            .as_str()
            .is_some_and(|text| text.contains("Owner general override body.")),
        "{overridden:?}"
    );

    alpha.cancel().await.unwrap();
}
