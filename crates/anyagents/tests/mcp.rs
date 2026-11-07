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
}

impl Drop for Serve {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn start_server(data: &std::path::Path) -> Serve {
    start_server_mode(data, "serve")
}

fn start_server_mode(data: &std::path::Path, mode: &str) -> Serve {
    let mut child = bin()
        .arg("--data")
        .arg(data)
        .arg(mode)
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
    let socket = data.join(if mode == "serve-admin" {
        "admin.sock"
    } else {
        "anyagents.sock"
    });
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

    // Encapsulation boundary across levels: Root `alpha` (grandparent) and `beta` (unrelated root)
    // cannot skip `worker-1` to message private L2 sub-agent `worker-1-child` (`public: false`) directly.
    let root_skip_l1_err = error_text(
        &call(
            &alpha,
            "send",
            serde_json::json!({"to": ["worker-1-child"], "text": "skipping L1 to L2"}),
        )
        .await,
    );
    assert!(
        root_skip_l1_err.contains("cannot message sub-agent `worker-1-child` directly")
            && root_skip_l1_err.contains("agent:worker-1"),
        "unexpected error: {root_skip_l1_err}"
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
            && p2p_direct_err.contains("public: false"),
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

#[tokio::test]
async fn describe_preserves_config_permissions_without_vendor_restart_writers() {
    let dir = tempfile::tempdir().unwrap();
    let created = bin()
        .env("PATH", machine_path(dir.path()))
        .arg("--data")
        .arg(dir.path())
        .args(["agents", "create", "alpha"])
        .output()
        .unwrap();
    assert!(
        created.status.success(),
        "{}",
        String::from_utf8_lossy(&created.stderr)
    );
    // The primary fixture's /bin/true harness exits and restarts, concurrently
    // preparing config. This case measures describe's own permission contract,
    // so keep the real MCP/auth/HTTP path but run no vendor-config writer.
    let server = start_server_mode(dir.path(), "serve-admin");
    // MCP initialization asks whoami on the primary socket name. Route that
    // fixture-only name to the admin router rather than starting a supervisor.
    std::os::unix::fs::symlink("admin.sock", dir.path().join("anyagents.sock")).unwrap();
    let alpha = connect(dir.path(), "alpha").await;
    let alpha_id = created_id(dir.path(), "alpha");
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

    alpha.cancel().await.unwrap();
    drop(server);
}

#[tokio::test]
async fn history_search_defaults_and_all_never_escape_a_private_childs_tree() {
    use anyagents_core::{Incoming, Store};
    let dir = tempfile::tempdir().unwrap();
    for name in ["argus", "lyra", "helios", "runner"] {
        let created = bin()
            .env("PATH", machine_path(dir.path()))
            .arg("--data")
            .arg(dir.path())
            .args(["agents", "create", name])
            .output()
            .unwrap();
        assert!(created.status.success(), "{created:?}");
    }
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let argus = store.agent_by_name("argus").unwrap();
    let lyra = store.agent_by_name("lyra").unwrap();
    let helios = store.agent_by_name("helios").unwrap();
    let runner = store.agent_by_name("runner").unwrap();
    store.set_agent_parent(&lyra.id, Some(&argus.id)).unwrap();
    store.set_agent_parent(&helios.id, Some(&argus.id)).unwrap();
    store.set_agent_parent(&runner.id, Some(&lyra.id)).unwrap();
    // Synthetic bodies only. These identifiers reproduce the reported search
    // shape without importing any production conversation content.
    for (id, text) in [
        (
            &argus.id,
            "C msg_01m6svh4znevfapf3mmpsnk0a4 #2291 private parent",
        ),
        (
            &helios.id,
            "C msg_01m6svrywtez39wvnttdvz53xf #2291 private sibling",
        ),
        (&lyra.id, "C #2291 authorized own mailbox"),
        (&runner.id, "C #2291 authorized descendant"),
    ] {
        store
            .receive(
                id,
                Incoming {
                    text: text.into(),
                    sender: Some(
                        if id == &argus.id {
                            "agent:lyra"
                        } else {
                            "agent:argus"
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
            &argus.id,
            "telegram:chat:-100",
            None,
            "C out_msg_01m6svqn4nesw9ws13fjy48kzt #2291 private outbound",
        )
        .unwrap();
    let server = start_server_mode(dir.path(), "serve-admin");
    std::os::unix::fs::symlink("admin.sock", dir.path().join("anyagents.sock")).unwrap();
    let client = connect(dir.path(), "lyra").await;
    let native: ConversationResponse = ok_json(
        call(
            &client,
            "conversation",
            serde_json::json!({"with":"lyra", "direction":"in", "query":"C", "limit":4}),
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
    for target in ["argus", "helios", argus.id.as_str(), helios.id.as_str()] {
        for tool in ["conversation", "search"] {
            let denied = call(
                &client,
                tool,
                serde_json::json!({"agent":target, "query":"C"}),
            )
            .await;
            assert!(
                error_text(&denied).contains("cannot read private conversation"),
                "{denied:?}"
            );
        }
    }
    client.cancel().await.unwrap();
    drop(server);
}
