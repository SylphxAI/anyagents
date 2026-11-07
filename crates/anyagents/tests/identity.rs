//! One authenticated identity per agent session, decided by the server before
//! any route runs.
//!
//! These test the server as a deployment runs it: with an operator token, so
//! the boundary is in place. A capability is minted by the operator for one
//! agent; an agent's call acts as that agent and nobody else; a name, a body
//! sender, another agent's inbox or an administrative route are refused with no
//! write and no post.

mod support;

use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

struct Serve {
    child: Child,
    socket: std::path::PathBuf,
}

impl Drop for Serve {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        // Say why it ended, when it ended before this test did.
        if let Some(mut stderr) = self.child.stderr.take() {
            let mut text = String::new();
            let _ = std::io::Read::read_to_string(&mut stderr, &mut text);
            if !text.is_empty() {
                eprintln!("server {} stderr: {text}", self.socket.display());
            }
        }
    }
}

fn start_server(data: &Path) -> Serve {
    let child = Command::new(env!("CARGO_BIN_EXE_anyagents"))
        .env("PATH", support::machine_path(data))
        .env("ANYAGENTS_CLAUDE_BIN", "/bin/true")
        .arg("--data")
        .arg(data)
        .arg("serve")
        .stdout(Stdio::null())
        .stderr(Stdio::from(
            std::fs::File::create(data.join("server.stderr")).expect("stderr"),
        ))
        .spawn()
        .expect("server");
    let socket = data.join("anyagents.sock");
    let deadline = Instant::now() + Duration::from_secs(10);
    while std::os::unix::net::UnixStream::connect(&socket).is_err() {
        assert!(
            Instant::now() < deadline,
            "the server never listened: {:?}",
            std::fs::read_to_string(data.join("server.stderr")).unwrap_or_default()
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    Serve { child, socket }
}

/// The capability the server left in one agent's own config directory. Reading
/// it here stands for the agent's own process reading its own file.
fn capability(data: &Path, id: &str) -> String {
    std::fs::read_to_string(
        data.join("agents")
            .join(id)
            .join("config")
            .join("mcp-capability"),
    )
    .expect("the server minted a capability")
    .trim()
    .to_string()
}

fn create(data: &Path, name: &str) -> String {
    let socket = data.join("anyagents.sock");
    let (status, body) = support::call(
        &socket,
        "POST",
        "/v1/agents",
        &json!({"name": name, "harness": "claude-code"}).to_string(),
    );
    assert!(status.contains("201"), "{status} {body}");
    body["id"]
        .as_str()
        .unwrap_or_else(|| panic!("{body}"))
        .to_string()
}

#[test]
fn owner_authority_is_explicit_live_revocable_and_never_inherited() {
    let _env = support::lock();
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    support::operator_with_configured_seat(&data);
    let server = start_server(&data);
    let sage = create(&data, "sage");
    let auth = format!("x-anyagents-agent: {}\r\n", capability(&data, &sage));
    let request = |method: &str, path: &str, body: Value| {
        support::call_raw(&server.socket, method, path, &body.to_string(), &auth)
    };
    assert!(request("GET", "/v1/profiles", json!(null)).starts_with("HTTP/1.1 403"));
    let (status, _) = support::call(
        &server.socket,
        "POST",
        "/v1/agents/sage/admin",
        r#"{"admin":true}"#,
    );
    assert!(status.contains("200"), "{status}");
    assert!(
        request("POST", "/v1/agents/sage/admin", json!({"admin":true})).starts_with("HTTP/1.1 403")
    );
    let root = request(
        "POST",
        "/v1/agents",
        json!({"name":"rhea", "harness":"claude-code", "parent":"root"}),
    );
    assert!(root.starts_with("HTTP/1.1 201"), "{root}");
    let store = anyagents_core::Store::open(data.join("state.db")).unwrap();
    let rhea = store.agent_by_name("rhea").unwrap();
    assert!(rhea.parent_id.is_none());
    assert!(!rhea.is_admin);
    let rhea_auth = format!(
        "x-anyagents-agent: {}\r\n",
        capability(&data, rhea.id.as_str())
    );
    for path in ["/v1/channels", "/v1/profiles"] {
        assert!(
            support::call_raw(&server.socket, "GET", path, "", &rhea_auth)
                .starts_with("HTTP/1.1 403")
        );
    }
    let child = request("POST", "/v1/agents", json!({"name":"child"}));
    assert!(child.starts_with("HTTP/1.1 201"), "{child}");
    assert!(!store.agent_by_name("child").unwrap().is_admin);
    let (status, _) = support::call(
        &server.socket,
        "POST",
        "/v1/agents/child/admin",
        r#"{"admin":true}"#,
    );
    assert!(status.contains("400"), "{status}");
    let key = "test-provider-secret-never-return";
    let token = "123:test-channel-secret-never-return";
    let mut responses = Vec::new();
    responses.push(request(
        "POST",
        "/v1/agents/rhea/model",
        json!({"model":"test-model", "base_url":"https://api.example.test", "key":key}),
    ));
    responses.push(request(
        "POST",
        "/v1/channels",
        json!({"kind":"telegram", "agent":"rhea", "token":token}),
    ));
    for response in &responses {
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    }
    responses.push(request("GET", "/v1/channels", json!(null)));
    responses.push(request("GET", "/v1/profiles", json!(null)));
    // Unknown path fields are rejected rather than read in the daemon's seat.
    let rejected = request(
        "POST",
        "/v1/agents/rhea/model",
        json!({"model":"test", "key_file":"/etc/passwd"}),
    );
    assert!(rejected.starts_with("HTTP/1.1 422"), "{rejected}");
    responses.push(request(
        "POST",
        "/v1/channels",
        json!({"kind":"invalid", "agent":"rhea", "token":token}),
    ));
    let (status, _) = support::call(
        &server.socket,
        "POST",
        "/v1/agents/sage/admin",
        r#"{"admin":false}"#,
    );
    assert!(status.contains("200"));
    for path in ["/v1/profiles", "/v1/channels"] {
        assert!(request("GET", path, json!(null)).starts_with("HTTP/1.1 403"));
    }
    let conn = rusqlite::Connection::open(data.join("state.db")).unwrap();
    let mut stmt = conn
        .prepare("SELECT actor || ':' || action || ':' || target FROM admin_audit")
        .unwrap();
    let audit: Vec<String> = stmt
        .query_map([], |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert!(
        audit
            .iter()
            .any(|r| r == "agent:sage:agents.create_root:rhea")
    );
    assert!(
        audit
            .iter()
            .any(|r| r == "operator:agents.revoke_admin:sage")
    );
    for text in responses.iter().chain(audit.iter()) {
        assert!(!text.contains(key));
        assert!(!text.contains(token));
    }
}

#[test]
fn a_session_acts_as_its_own_agent_and_nothing_else() {
    let _env = support::lock();
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    support::operator_with_configured_seat(&data);
    let server = start_server(&data);
    let socket = data.join("anyagents.sock");
    let alpha = create(&data, "alpha");
    let beta = create(&data, "beta");

    let alpha_token = capability(&data, &alpha);
    let alpha_auth = format!("x-anyagents-agent: {alpha_token}\r\n");
    let beta_auth = format!("x-anyagents-agent: {}\r\n", capability(&data, &beta));

    // An agent's own send and a read of its own mailbox both work.
    let sent = support::call_raw(
        &socket,
        "POST",
        "/v1/send",
        &json!({"to": ["beta"], "text": "hello"}).to_string(),
        &alpha_auth,
    );
    let sent: Value = serde_json::from_str(sent.split("\r\n\r\n").nth(1).unwrap()).unwrap();
    assert!(sent["deliveries"][0]["seq"].is_number(), "{sent}");
    let beta_inbox = support::call_raw(
        &socket,
        "GET",
        &format!("/v1/agents/{beta}/inbox"),
        "",
        &beta_auth,
    );
    assert!(beta_inbox.contains("hello"), "{beta_inbox}");
    // The author recorded is the capability's agent, never the body.
    assert!(beta_inbox.contains("agent:alpha"), "{beta_inbox}");

    // A body that names another sender is refused, with nothing written.
    let forged = support::call_raw(
        &socket,
        "POST",
        "/v1/send",
        &json!({"to": ["beta"], "text": "forged", "sender": "agent:beta"}).to_string(),
        &alpha_auth,
    );
    assert!(forged.starts_with("HTTP/1.1 403"), "{forged}");
    // Messaging a peer does not grant access to that peer's private history.
    let other_inbox = support::call_raw(
        &socket,
        "GET",
        &format!("/v1/agents/{alpha}/inbox"),
        "",
        &beta_auth,
    );
    assert!(other_inbox.starts_with("HTTP/1.1 403"), "{other_inbox}");
    let other_conv = support::call_raw(
        &socket,
        "GET",
        &format!("/v1/agents/{alpha}/conversation"),
        "",
        &beta_auth,
    );
    assert!(other_conv.starts_with("HTTP/1.1 403"), "{other_conv}");
    // No capability at all is refused.
    let none = support::call_raw(&socket, "GET", "/v1/agents", "", "");
    assert!(none.starts_with("HTTP/1.1 401"), "{none}");
    // An operator-only route is refused to an agent.
    let op_inbox = support::call_raw(
        &socket,
        "GET",
        &format!("/v1/operator/agents/{alpha}/inbox"),
        "",
        &beta_auth,
    );
    assert!(op_inbox.starts_with("HTTP/1.1 403"), "{op_inbox}");
    let minted = support::call_raw(
        &socket,
        "POST",
        &format!("/v1/operator/agents/{beta}/capability"),
        "{}",
        &alpha_auth,
    );
    assert!(minted.starts_with("HTTP/1.1 403"), "{minted}");

    // Nothing from the refused calls reached a mailbox.
    let after = support::call_raw(
        &socket,
        "GET",
        &format!("/v1/agents/{beta}/inbox"),
        "",
        &beta_auth,
    );
    assert!(!after.contains("forged"), "{after}");

    // An agent spawning a sub-agent via POST /v1/agents sets itself as parent.
    let spawned = support::call_raw(
        &socket,
        "POST",
        "/v1/agents",
        &json!({"name": "gamma", "prompt": "subagent task"}).to_string(),
        &alpha_auth,
    );
    assert!(spawned.starts_with("HTTP/1.1 201"), "{spawned}");
    assert!(
        spawned.contains(&format!("\"parent\":\"{alpha}\"")),
        "{spawned}"
    );
    let gamma_conv = support::call_raw(
        &socket,
        "GET",
        "/v1/agents/gamma/conversation",
        "",
        &alpha_auth,
    );
    assert!(gamma_conv.contains("subagent task"), "{gamma_conv}");

    // A root agent cannot be archived by an agent capability (prevents accidental self-archive).
    let root_archive_refused = support::call_raw(
        &socket,
        "POST",
        "/v1/agents/alpha/archive",
        "{}",
        &alpha_auth,
    );
    assert!(
        root_archive_refused.starts_with("HTTP/1.1 400"),
        "{root_archive_refused}"
    );

    // An intentional direct send wakes an explicitly archived agent.
    let archived = support::call_raw(
        &socket,
        "POST",
        "/v1/agents/gamma/archive",
        "{}",
        &alpha_auth,
    );
    assert!(archived.contains("\"state\":\"archived\""), "{archived}");
    let send_while_archived = support::call_raw(
        &socket,
        "POST",
        "/v1/send",
        &json!({"to": ["gamma"], "text": "message while archived"}).to_string(),
        &alpha_auth,
    );
    assert!(
        send_while_archived.starts_with("HTTP/1.1 200"),
        "{send_while_archived}"
    );
    let agents_archived = support::call_raw(&socket, "GET", "/v1/agents", "", &support::header());
    let agents_archived_json: Value =
        serde_json::from_str(agents_archived.split("\r\n\r\n").nth(1).unwrap()).unwrap();
    let gamma_archived_row = agents_archived_json["agents"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["name"] == "gamma")
        .unwrap();
    assert_eq!(
        gamma_archived_row["state"], "running",
        "{gamma_archived_row}"
    );
    let woke_explicit =
        support::call_raw(&socket, "POST", "/v1/agents/gamma/wake", "{}", &alpha_auth);
    assert!(woke_explicit.starts_with("HTTP/1.1 200"), "{woke_explicit}");
    let agents = support::call_raw(&socket, "GET", "/v1/agents", "", &support::header());
    let agents_json: Value =
        serde_json::from_str(agents.split("\r\n\r\n").nth(1).unwrap()).unwrap();
    let gamma_row = agents_json["agents"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["name"] == "gamma")
        .unwrap();
    assert_eq!(gamma_row["state"], "running", "{gamma_row}");
    assert_eq!(gamma_row["parent"], alpha, "{gamma_row}");

    // A replaced session stops working at once.
    let re_minted = support::call_raw(
        &socket,
        "POST",
        "/v1/operator/agents/alpha/capability",
        "{}",
        &support::header(),
    );
    assert!(re_minted.contains("minted"), "{re_minted}");
    let old = support::call_raw(&socket, "GET", "/v1/agents", "", &alpha_auth);
    assert!(old.starts_with("HTTP/1.1 401"), "{old}");
    let fresh = format!("x-anyagents-agent: {}\r\n", capability(&data, &alpha));
    let working = support::call_raw(&socket, "GET", "/v1/agents", "", &fresh);
    assert!(working.starts_with("HTTP/1.1 200"), "{working}");

    drop(server);
}

/// Each agent's capability can read its own and peer mailboxes, while operator routes stay restricted.
#[test]
fn each_agents_capability_reads_only_own_mailbox_not_peer_history() {
    let _env = support::lock();
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    support::operator_with_configured_seat(&data);
    let first = start_server(&data);
    let socket = data.join("anyagents.sock");
    let alpha = create(&data, "alpha");
    let beta = create(&data, "beta");
    let alpha_token = capability(&data, &alpha);
    let beta_token = capability(&data, &beta);
    assert_ne!(alpha_token, beta_token);
    let as_alpha = format!("x-anyagents-agent: {alpha_token}\r\n");
    let as_beta = format!("x-anyagents-agent: {beta_token}\r\n");
    let own = support::call_raw(
        &socket,
        "GET",
        &format!("/v1/agents/{alpha}/inbox"),
        "",
        &as_alpha,
    );
    assert!(own.starts_with("HTTP/1.1 200"), "{own}");
    let peer_inbox = support::call_raw(
        &socket,
        "GET",
        &format!("/v1/agents/{alpha}/inbox"),
        "",
        &as_beta,
    );
    assert!(peer_inbox.starts_with("HTTP/1.1 403"), "{peer_inbox}");
    let peer_conv = support::call_raw(
        &socket,
        "GET",
        &format!("/v1/agents/{alpha}/conversation"),
        "",
        &as_beta,
    );
    assert!(peer_conv.starts_with("HTTP/1.1 403"), "{peer_conv}");
    let op_route = support::call_raw(
        &socket,
        "GET",
        &format!("/v1/operator/agents/{alpha}/inbox"),
        "",
        &as_beta,
    );
    assert!(op_route.starts_with("HTTP/1.1 403"), "{op_route}");
    drop(first);
}

/// A capability a deployment's own config directory holds is exactly one
/// agent's: another agent's tool server reads its own file, and the server
/// answers with that agent, never with a name the model wrote.
#[test]
fn the_capability_file_of_one_agent_never_opens_another() {
    let _env = support::lock();
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    support::operator_with_configured_seat(&data);
    let server = start_server(&data);
    let _ = server;
    let socket = data.join("anyagents.sock");
    let alpha = create(&data, "alpha");
    let beta = create(&data, "beta");

    // Each config directory holds one file, mode 0600 or 0640, and nothing else.
    for id in [&alpha, &beta] {
        let path = data
            .join("agents")
            .join(id)
            .join("config")
            .join("mcp-capability");
        let mode = {
            use std::os::unix::fs::PermissionsExt;
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777
        };
        assert!(mode == 0o600 || mode == 0o640, "{path:?} is {mode:o}");
    }

    // The whoami answer is the capability's agent, and operator routes cannot be
    // called by an agent capability.
    let alpha_auth = format!("x-anyagents-agent: {}\r\n", capability(&data, &alpha));
    let who = support::call_raw(&socket, "GET", "/v1/whoami", "", &alpha_auth);
    assert!(who.contains(&alpha), "{who}");
    assert!(!who.contains(&beta), "{who}");
    let op_only = support::call_raw(
        &socket,
        "GET",
        &format!("/v1/operator/agents/{beta}/inbox"),
        "",
        &alpha_auth,
    );
    assert!(op_only.starts_with("HTTP/1.1 403"), "{op_only}");
}

#[test]
fn conversation_reads_are_downward_only_for_every_scope_and_cli_fails_closed() {
    use anyagents_core::{AgentId, Incoming, Store};
    let _env = support::lock();
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    support::operator_with_configured_seat(&data);
    let server = start_server(&data);
    let ids: Vec<_> = ["argus", "lyra", "helios", "runner", "outsider"]
        .into_iter()
        .map(|name| create(&data, name))
        .collect();
    let store = Store::open(data.join("state.db")).unwrap();
    let aids: Vec<_> = ids.iter().map(|id| AgentId::parse(id).unwrap()).collect();
    store.set_agent_parent(&aids[1], Some(&aids[0])).unwrap();
    store.set_agent_parent(&aids[2], Some(&aids[0])).unwrap();
    store.set_agent_parent(&aids[3], Some(&aids[1])).unwrap();
    for id in &aids {
        store.join_group("shared", id).unwrap();
        let name = store.agent(id).unwrap().name;
        store
            .receive(
                id,
                Incoming {
                    text: format!("C private fixture {name}"),
                    sender: Some("agent:argus".into()),
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
            .record_outbound(
                id,
                "telegram:chat:-100",
                None,
                &format!("C outbound fixture {name}"),
            )
            .unwrap();
        store
            .record_tool_event(id, "browser", &format!("C tool fixture {name}"))
            .unwrap();
    }
    let as_agent = |index: usize, target: &str, suffix: &str| {
        support::call_raw(
            &server.socket,
            "GET",
            &format!("/v1/agents/{target}/conversation{suffix}"),
            "",
            &format!("x-anyagents-agent: {}\r\n", capability(&data, &ids[index])),
        )
    };
    // Reachability and membership are never private-history grants. The same
    // boundary applies to isolated, private, sibling and public callers.
    for scope in ["isolated", "private", "siblings", "public"] {
        store
            .set_agent_launch_env(&aids[1], &[("ANYAGENTS_SCOPE".into(), scope.into())])
            .unwrap();
        for target in [
            "argus",
            "helios",
            "outsider",
            ids[0].as_str(),
            ids[2].as_str(),
        ] {
            for suffix in [
                "",
                "?query=C",
                "?only_tools=true",
                "?from_seq=1&to_seq=2&radius=10",
            ] {
                let response = as_agent(1, target, suffix);
                assert!(
                    response.starts_with("HTTP/1.1 403"),
                    "{scope} {target} {suffix}: {response}"
                );
            }
        }
        for target in [
            "all",
            "*",
            "self",
            "lyra",
            ids[1].as_str(),
            "runner",
            ids[3].as_str(),
        ] {
            for suffix in [
                "",
                "?query=C&radius=10",
                "?only_tools=true&tool_name=browser",
                "?from_seq=1&to_seq=1&direction=in",
                "?query=C&with=argus&sender=argus&direction=in",
            ] {
                let response = as_agent(1, target, suffix);
                assert!(
                    response.starts_with("HTTP/1.1 200"),
                    "{scope} {target} {suffix}: {response}"
                );
                for hidden in ["fixture argus", "fixture helios", "fixture outsider"] {
                    assert!(
                        !response.contains(hidden),
                        "{scope} {target} {suffix}: {response}"
                    );
                }
            }
        }
    }
    assert!(as_agent(1, "missing", "?query=C").starts_with("HTTP/1.1 404"));
    // Grandchildren are visible to ancestors, but neither ancestors nor
    // siblings are visible in the opposite direction.
    assert!(as_agent(0, "runner", "?query=C").contains("private fixture runner"));
    assert!(as_agent(3, "lyra", "").starts_with("HTTP/1.1 403"));
    assert!(as_agent(2, "lyra", "").starts_with("HTTP/1.1 403"));
    assert!(!as_agent(4, "all", "?query=C").contains("fixture lyra"));
    // Party copies in the caller's own mailbox remain readable.
    assert!(as_agent(1, "self", "?query=C&with=argus").contains("private fixture lyra"));
    assert!(!as_agent(1, "all", "?query=C&with=outsider").contains("fixture"));

    let cli = |target: &str| {
        Command::new(env!("CARGO_BIN_EXE_anyagents"))
            .arg("--data")
            .arg(&data)
            .args(["conversation", target, "--query", "C"])
            .env_remove("ANYAGENTS_OPERATOR_TOKEN_FILE")
            .env("ANYAGENTS_AGENT_ID", &ids[1])
            .env(
                "ANYAGENTS_AGENT_CONFIG",
                data.join("agents").join(&ids[1]).join("config"),
            )
            .output()
            .unwrap()
    };
    let denied = cli("argus");
    assert!(!denied.status.success());
    assert!(denied.stdout.is_empty(), "{denied:?}");
    let allowed = cli("all");
    assert!(allowed.status.success(), "{allowed:?}");
    let text = String::from_utf8(allowed.stdout).unwrap();
    assert!(
        text.contains("fixture lyra") && text.contains("fixture runner"),
        "{text}"
    );
    assert!(
        !text.contains("fixture argus") && !text.contains("fixture helios"),
        "{text}"
    );

    // Explicit root-admin and operator reads are broad and auditable; a
    // non-admin root is not privileged merely because it is a root.
    assert!(!as_agent(0, "all", "?query=C").contains("fixture outsider"));
    store.set_agent_admin(&aids[0], true).unwrap();
    assert!(as_agent(0, "all", "?query=C").contains("fixture outsider"));
    assert!(as_agent(0, "self", "?query=C").contains("fixture argus"));
    let (status, body) = support::call(
        &server.socket,
        "GET",
        "/v1/agents/all/conversation?query=C",
        "",
    );
    assert!(status.contains("200"), "{status} {body}");
    assert!(body.to_string().contains("fixture outsider"));
    let db = rusqlite::Connection::open(data.join("state.db")).unwrap();
    for actor in ["operator", "agent:argus"] {
        let count: i64 = db.query_row("SELECT COUNT(*) FROM admin_audit WHERE action = 'conversation.read' AND target = 'all' AND actor = ?1", [actor], |row| row.get(0)).unwrap();
        assert!(count > 0, "{actor} read must have an audit row");
    }
    drop(server);
    // Existing fixture SQLite data must not make a failed socket read succeed.
    for target in ["all", "argus", "lyra"] {
        let offline = cli(target);
        assert!(!offline.status.success(), "{offline:?}");
        assert!(offline.stdout.is_empty(), "{offline:?}");
    }
}

/// Exercise both executable entrypoints, not just a maintenance helper: neither
/// corrupt recovery state nor a held global lock may prevent socket service.
#[test]
fn migration_failure_and_busy_global_lock_do_not_block_host_sockets() {
    use anyagents_core::{NewAgent, Store};
    use std::fs;
    let _env = support::lock();
    for command in ["serve", "serve-admin"] {
        for busy in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path();
            support::operator_with_configured_seat(data);
            let store = Store::open(data.join("state.db")).unwrap();
            let agent = store
                .create_agent(NewAgent::new("maintenance-only", "sylphx"))
                .unwrap();
            store.set_agent_state(&agent.id, "archived").unwrap();
            let memory = data
                .join("agents")
                .join(agent.id.as_str())
                .join("config/projects/known/memory");
            fs::create_dir_all(&memory).unwrap();
            fs::write(memory.join("MEMORY.md"), b"must survive failed maintenance").unwrap();
            let migration = data.join("host-memory-migration");
            fs::create_dir_all(&migration).unwrap();
            let manifest = migration.join("manifest-v1.json");
            let original = if busy {
                b"{\"version\":1,\"entries\":[]}".as_slice()
            } else {
                b"corrupt manifest must not be replaced".as_slice()
            };
            fs::write(&manifest, original).unwrap();
            let held = fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(migration.join("migration.lock"))
                .unwrap();
            if busy {
                held.lock().unwrap();
            }
            let child = Command::new(env!("CARGO_BIN_EXE_anyagents"))
                .env("PATH", support::machine_path(data))
                .env("ANYAGENTS_CLAUDE_BIN", "/bin/true")
                .arg("--data")
                .arg(data)
                .arg(command)
                .stdout(Stdio::null())
                .stderr(Stdio::from(
                    fs::File::create(data.join("server.stderr")).unwrap(),
                ))
                .spawn()
                .unwrap();
            let socket = data.join(if command == "serve" {
                "anyagents.sock"
            } else {
                "admin.sock"
            });
            let server = Serve { child, socket };
            let deadline = Instant::now() + Duration::from_secs(10);
            while std::os::unix::net::UnixStream::connect(&server.socket).is_err() {
                assert!(
                    Instant::now() < deadline,
                    "{command}, busy={busy}: {}",
                    fs::read_to_string(data.join("server.stderr")).unwrap_or_default()
                );
                std::thread::sleep(Duration::from_millis(20));
            }
            let receipt_path = migration.join(if busy {
                "deferred-startup-v1.json"
            } else {
                "startup-error-v1.json"
            });
            let receipt: Value = serde_json::from_slice(&fs::read(receipt_path).unwrap()).unwrap();
            assert_eq!(
                receipt["status"],
                if busy {
                    "deferred_busy_lock"
                } else {
                    "deferred_error"
                }
            );
            assert_eq!(receipt["retryable"], true);
            assert_eq!(fs::read(&manifest).unwrap(), original);
            assert_eq!(
                fs::read(memory.join("MEMORY.md")).unwrap(),
                b"must survive failed maintenance"
            );
            drop(server);
            drop(held);
        }
    }
}

#[test]
fn contended_migration_publishes_wait_without_pin_or_dispatch_then_retries() {
    use anyagents_core::{NewAgent, Store, host_session};
    use std::{fs, os::unix::fs::PermissionsExt};
    // Complete salt residue classes and every distinct exponential shift,
    // including saturation: the existing retry path never sleeps over 60 s.
    let delays: Vec<_> = (0..=18)
        .chain([u32::MAX])
        .flat_map(|attempt| {
            (0..40).map(move |salt| anyagents_supervisor::restart_delay_ms(attempt, salt))
        })
        .collect();
    assert!(delays.iter().all(|delay| (1..=60_000).contains(delay)));
    assert_eq!(delays.iter().max(), Some(&60_000));
    assert_eq!(anyagents_supervisor::restart_delay_ms(1, 0), 200);
    assert_eq!(anyagents_supervisor::restart_delay_ms(1, 39), 297);
    let _env = support::lock();
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path();
    support::operator_with_configured_seat(data);
    let store = Store::open(data.join("state.db")).unwrap();
    let parent = store
        .create_agent(NewAgent::new("waiting-parent", "claude-code"))
        .unwrap();
    let agent = store
        .create_agent(NewAgent {
            parent_id: Some(parent.id.clone()),
            ..NewAgent::new("launch-waits", "claude-code")
        })
        .unwrap();
    store.set_agent_state(&agent.id, "running").unwrap();
    let config = data.join("agents").join(agent.id.as_str()).join("config");
    let root = config.parent().unwrap();
    let vendor_memory = config.join("projects/known/memory");
    fs::create_dir_all(&vendor_memory).unwrap();
    fs::write(
        vendor_memory.join("MEMORY.md"),
        b"vendor-only authoritative facts",
    )
    .unwrap();
    fs::write(config.join("adapter.json"), br#"{"harness":"claude-code"}"#).unwrap();
    let adapter = data.join("adapter");
    fs::write(
        &adapter,
        r#"#!/bin/sh
printf dispatched > "$ANYAGENTS_AGENT_CONFIG/dispatch"
exec /bin/cat
"#,
    )
    .unwrap();
    fs::set_permissions(&adapter, fs::Permissions::from_mode(0o755)).unwrap();
    let migration = data.join("host-memory-migration");
    fs::create_dir_all(&migration).unwrap();
    let held = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(migration.join("migration.lock"))
        .unwrap();
    held.lock().unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_anyagents"))
        .env("PATH", support::machine_path(data))
        .env("ANYAGENTS_HARNESS_BIN", &adapter)
        .env("ANYAGENTS_FAKE_MODE", "1")
        .arg("--data")
        .arg(data)
        .arg("serve")
        .stdout(Stdio::null())
        .stderr(Stdio::from(
            fs::File::create(data.join("server.stderr")).unwrap(),
        ))
        .spawn()
        .unwrap();
    let server = Serve {
        child,
        socket: data.join("anyagents.sock"),
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if std::os::unix::net::UnixStream::connect(&server.socket).is_ok() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "{}",
            fs::read_to_string(data.join("server.stderr")).unwrap()
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    loop {
        let (_, body) = support::call(&server.socket, "GET", "/v1/agents", "");
        let row = body["agents"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["id"] == agent.id.as_str())
            .unwrap();
        if row["block_reason"] == "memory_migration_wait" {
            assert_eq!(row["turn_state"], "idle", "{row}");
            assert!(
                store.next_undelivered(&parent.id).unwrap().is_none(),
                "wait emitted terminal alert"
            );
            assert!(
                row["last_error"]
                    .as_str()
                    .unwrap()
                    .contains("waiting_on=migration.lock"),
                "{row}"
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "missing explicit migration wait: {body}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(!config.join("dispatch").exists());
    assert!(host_session::active(root).unwrap().is_none());
    assert!(!root.join("memory/MEMORY.md").exists());
    drop(held); // flock's existing descriptor-close path releases the holder.
    let deadline = Instant::now() + Duration::from_secs(10);
    while !config.join("dispatch").exists() {
        assert!(
            Instant::now() < deadline,
            "retry did not dispatch: {}",
            fs::read_to_string(data.join("server.stderr")).unwrap()
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    let session = host_session::active(root).unwrap().unwrap();
    assert!(
        session
            .memory
            .section
            .contains("vendor-only authoritative facts")
    );
    assert_eq!(
        fs::read(root.join("memory/MEMORY.md")).unwrap(),
        b"vendor-only authoritative facts"
    );
    assert!(!vendor_memory.join("MEMORY.md").exists());
}
