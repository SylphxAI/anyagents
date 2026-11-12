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
    let mut child = support::contract_server()
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

fn error_text(result: &CallToolResult) -> String {
    assert_eq!(result.is_error, Some(true), "{result:?}");
    match result.content.first() {
        Some(ContentBlock::Text(text)) => text.text.clone(),
        other => panic!("expected text error, got {other:?}"),
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
            .args(["agents", "create", name])
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
        [
            "send",
            "inbox",
            "agents",
            "groups",
            "schedules",
            "conversation",
            "code",
            "web_search",
            "web_fetch",
            "memory",
            "skills",
            "browser",
            "computer",
        ]
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>(),
        "expected Full MCP profile tools on claude-code stdio session, got {names:?}"
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
    {
        let store = anyagents_core::Store::open(dir.path().join("state.db")).unwrap();
        let alpha_rec = store.agent_by_name("alpha").unwrap();
        let beta_rec = store.agent_by_name("beta").unwrap();
        store.set_agent_admin(&alpha_rec.id, true).unwrap();
        store.set_agent_admin(&beta_rec.id, true).unwrap();
    }
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
    {
        let store = anyagents_core::Store::open(dir.path().join("state.db")).unwrap();
        let alpha_rec = store.agent_by_name("alpha").unwrap();
        let beta_rec = store.agent_by_name("beta").unwrap();
        store.set_agent_admin(&alpha_rec.id, false).unwrap();
        store.set_agent_admin(&beta_rec.id, false).unwrap();
    }

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
async fn session_reset_prompt_for_a_paused_agent_is_queued_for_the_new_session() {
    use anyagents_core::{NewAgent, Store, host_session};
    let dir = support::physical_tempdir();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let parent = store
        .create_agent(NewAgent::new("reset-prompt-parent", "spiron"))
        .unwrap();
    let mut input = NewAgent::new("reset-prompt-child", "spiron");
    input.parent_id = Some(parent.id.clone());
    let child = store.create_agent(input).unwrap();
    for agent in [&parent, &child] {
        store.set_agent_state(&agent.id, "paused").unwrap();
    }
    let root = dir.path().join("agents").join(child.id.as_str());
    std::fs::create_dir_all(root.join("config")).unwrap();
    std::fs::create_dir_all(root.join("memory")).unwrap();
    let old = host_session::ensure_active(&root, "spiron").unwrap();
    let server = start_server(dir.path());
    let parent_mcp = connect(dir.path(), &parent.name).await;

    // Without a prompt nothing is queued.
    ok_json(
        call(
            &parent_mcp,
            "agents",
            serde_json::json!({"action":"reset_session","name":child.name}),
        )
        .await,
    );
    assert!(store.next_undelivered(&child.id).unwrap().is_none());

    let result: serde_json::Value = ok_json(
        call(
            &parent_mcp,
            "agents",
            serde_json::json!({"action":"reset_session","name":child.name,"prompt":"start over from the saved plan"}),
        )
        .await,
    );
    let generation = host_session::active(&root).unwrap().unwrap().generation;
    assert_ne!(generation, old.generation);
    assert_eq!(result["generation"], generation);
    assert!(result.get("prompt").is_some(), "{result}");
    let queued = store
        .next_undelivered(&child.id)
        .unwrap()
        .expect("the reset prompt waits in the mailbox for the new session");
    assert!(queued.text.contains("start over from the saved plan"));
    assert_eq!(store.agent(&child.id).unwrap().state, "paused");
    parent_mcp.cancel().await.unwrap();
    drop(server);
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
        .args(["agents", "create", "alpha"])
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
        store.set_agent_admin(&agent.id, true).unwrap();
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
    let dir = support::physical_tempdir();
    for name in ["coordinator", "planner", "tester", "builder"] {
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
    ] {
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

#[tokio::test]
async fn code_mode_supports_memory_sdk_workspace_patch_list_exec_and_computer_snapshot_refs() {
    let dir = support::physical_tempdir();
    let created = bin()
        .env("PATH", machine_path(dir.path()))
        .arg("--data")
        .arg(dir.path())
        .args(["agents", "create", "sage"])
        .output()
        .unwrap();
    assert!(
        created.status.success(),
        "{}",
        String::from_utf8_lossy(&created.stderr)
    );
    let server = start_server(dir.path());
    let client = connect(dir.path(), "sage").await;

    let code_res: serde_json::Value = ok_json(
        call(
            &client,
            "code",
            serde_json::json!({
                "code": r##"
                // 1. First-class anyagents.memory SDK
                const w = await anyagents.memory.write({
                    topic: "arch-decisions",
                    description: "Pure Code Mode architecture",
                    content: "# Pure Code Mode\nOnly `code` is exposed in list_tools."
                });
                const r = await anyagents.memory.read("arch-decisions");
                const list = await anyagents.memory.list();
                const search = await anyagents.memory.search("Pure Code Mode");
                const del = await anyagents.memory.delete("arch-decisions");

                // 2. Enhanced workspace SDK: write, 4-stage edit, patch, list, exec
                await workspace.write("src/demo.rs", "fn main() {\n    let x = 1;\n    println!(\"{}\", x);\n}\n");
                // Indentation-different edit (stage 3)
                const edited = await workspace.edit({
                    path: "src/demo.rs",
                    target: "let x = 1;\nprintln!(\"{}\", x);",
                    replacement: "let x = 42;\n    println!(\"val={}\", x);"
                });
                // V4A patch
                const patched = await workspace.patch([
                    "*** Begin Patch",
                    "*** Update File: src/demo.rs",
                    "@@",
                    "-    let x = 42;",
                    "+    let x = 99;",
                    "*** Add File: src/extra.txt",
                    "+hello patch",
                    "*** End Patch"
                ].join("\n"));
                const listed = await workspace.list("src");
                const execOut = await workspace.exec({ cmd: "printf 'exec-ok'" });

                // 3. Semantic Accessibility Snapshot & STALE_REF guard on tools.computer
                const snap = await tools.computer.snapshot();
                const windows = await tools.computer.list_windows();
                let staleCaught = false;
                try {
                    await tools.computer.click("@e1");
                } catch (err: any) {
                    staleCaught = String(err).includes("STALE_REF") && String(err).includes("stale_ref");
                }

                return {
                    memTopic: r.topic,
                    memHasContent: r.content.includes("Only `code` is exposed"),
                    memCountBeforeDelete: list.count,
                    memSearchCount: search.count,
                    memDeleted: del.deleted,
                    editReplacements: edited.replacements_made,
                    patchCount: patched.count,
                    listCount: listed.count,
                    execStdout: execOut.stdout,
                    snapshotIdPrefix: snap.snapshot_id.startsWith("s"),
                    firstRef: snap.elements[0]?.ref_id,
                    snapshotRole: snap.elements[0]?.role,
                    windowCount: windows.count,
                    staleCaught,
                };
            "##
            }),
        )
        .await,
    );

    let res = &code_res["result"];
    assert_eq!(res["memTopic"], "arch-decisions", "{code_res:?}");
    assert_eq!(res["memHasContent"], true, "{code_res:?}");
    assert!(
        res["memCountBeforeDelete"].as_u64().unwrap_or(0) >= 1,
        "{code_res:?}"
    );
    assert!(
        res["memSearchCount"].as_u64().unwrap_or(0) >= 1,
        "{code_res:?}"
    );
    assert_eq!(res["memDeleted"], true, "{code_res:?}");
    assert_eq!(res["editReplacements"], 1, "{code_res:?}");
    assert_eq!(res["patchCount"], 2, "{code_res:?}");
    assert_eq!(res["listCount"], 2, "{code_res:?}");
    assert_eq!(res["execStdout"], "exec-ok", "{code_res:?}");
    assert_eq!(res["snapshotIdPrefix"], true, "{code_res:?}");
    assert!(
        res["firstRef"].as_str().unwrap_or("").contains(":e1"),
        "{code_res:?}"
    );
    assert_eq!(res["snapshotRole"], "desktop", "{code_res:?}");
    assert_eq!(res["windowCount"], 0, "{code_res:?}");
    assert_eq!(res["staleCaught"], true, "{code_res:?}");

    client.cancel().await.unwrap();
    drop(server);
}
