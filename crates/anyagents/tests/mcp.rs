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
        .env_remove("ANYAGENTS_AGENT_ID");
    cmd
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
    id: String,
    name: String,
    #[serde(default)]
    parent: Option<String>,
    harness: String,
    state: String,
    process: String,
    delivery_mode: String,
    steer_support: String,
}

#[derive(Deserialize)]
struct GroupsResponse {
    groups: Vec<GroupView>,
}

#[derive(Deserialize)]
struct GroupView {
    name: String,
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
        .env("PATH", machine_path(data))
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
    assert_eq!(
        names,
        vec![
            "agents".to_string(),
            "browser".to_string(),
            "conversation".to_string(),
            "groups".to_string(),
            "schedule".to_string(),
            "send".to_string(),
        ],
        "expected consolidated 6 canonical orthogonal MCP tools, got {names:?}"
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
    assert_eq!(got.turns[0].from, "agent:alpha");

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

    // Cross-agent conversation reading: alpha can read beta's conversation by name or ID, with optional limit.
    let peer_conv: ConversationResponse = ok_json(
        call(
            &alpha,
            "conversation",
            serde_json::json!({"agent": "beta", "limit": 2}),
        )
        .await,
    );
    assert_eq!(peer_conv.turns.len(), 2);
    assert_eq!(peer_conv.turns[1].text, "room line");

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
            &beta,
            "groups",
            serde_json::json!({"action": "join", "group": "brain-trust"}),
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

    // Custody transfer (`agents` `action = "transfer"`): `alpha` transfers `worker-1` to `beta`,
    // after which `beta` CAN message `worker-1` directly, and `beta` transfers `worker-1` back to `alpha`.
    let transferred_to_beta: serde_json::Value = ok_json(
        call(
            &alpha,
            "agents",
            serde_json::json!({
                "action": "transfer",
                "name": &spawned.id,
                "parent": "beta",
                "prompt": "handing off worker-1 to beta"
            }),
        )
        .await,
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

    let transferred_back: serde_json::Value = ok_json(
        call(
            &beta,
            "agents",
            serde_json::json!({
                "action": "transfer",
                "name": &spawned.id,
                "parent": "alpha",
                "prompt": "returning worker-1 to alpha"
            }),
        )
        .await,
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

    // An intentional direct send by immutable id wakes the archived sub-agent.
    let queued_send: SendResponse = ok_json(
        call(
            &alpha,
            "send",
            serde_json::json!({"to": [&spawned.id], "text": "wake up and continue"}),
        )
        .await,
    );
    assert_eq!(queued_send.deliveries[0].to, "agent:worker-1");
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
    assert_eq!(worker_row.state, "running");

    let woken: AgentView = ok_json(
        call(
            &alpha,
            "agents",
            serde_json::json!({"action": "wake", "name": &spawned.id}),
        )
        .await,
    );
    assert_eq!(woken.state, "running");

    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
        let alpha_cfg = dir.path().join("agents").join(&alpha_id).join("config");
        std::fs::set_permissions(&alpha_cfg, std::fs::Permissions::from_mode(0o0700)).unwrap();
        let before_gid = std::fs::metadata(&alpha_cfg).unwrap().gid();
        let desc_res: serde_json::Value = ok_json(
            call(
                &alpha,
                "describe",
                serde_json::json!({"agent": "self", "description": "Standing owner role description."}),
            )
            .await,
        );
        assert_eq!(desc_res["system_prompt_synced"], true);
        let after_meta = std::fs::metadata(&alpha_cfg).unwrap();
        assert_eq!(after_meta.permissions().mode() & 0o7777, 0o0700);
        assert_eq!(after_meta.gid(), before_gid);
    }

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
        assert_eq!(worker["description"], "Public role");
        assert_eq!(worker["instructions"], "Private instructions");
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
    assert_eq!(legacy_worker["description"], "Legacy instructions");
    assert_eq!(legacy_worker["instructions"], "Legacy instructions");

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
    // Two addressed lines, the room line, and the three `all` broadcast lines, each readable from the CLI too.
    let listed = String::from_utf8(inbox.stdout).unwrap();
    assert_eq!(listed.lines().count(), 6, "{listed}");
    assert!(listed.contains("room line"), "{listed}");
    assert!(listed.contains("broadcast to active"), "{listed}");
}
