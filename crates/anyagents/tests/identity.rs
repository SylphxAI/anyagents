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
    // An agent's inbox is strictly its own incoming queue; peers read each
    // other's full two-way conversation via `/v1/agents/{name}/conversation`.
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
    assert!(other_conv.starts_with("HTTP/1.1 200"), "{other_conv}");
    assert!(other_conv.contains("hello"), "{other_conv}");
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

    // Archiving gamma sets state = archived, and sending to gamma wakes it back to running.
    let archived = support::call_raw(
        &socket,
        "POST",
        "/v1/agents/gamma/archive",
        "{}",
        &alpha_auth,
    );
    assert!(archived.contains("\"state\":\"archived\""), "{archived}");
    let woke_send = support::call_raw(
        &socket,
        "POST",
        "/v1/send",
        &json!({"to": ["gamma"], "text": "resume now"}).to_string(),
        &alpha_auth,
    );
    assert!(woke_send.starts_with("HTTP/1.1 200"), "{woke_send}");
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
fn each_agents_capability_reads_own_and_peer_mailboxes() {
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
    assert!(peer_conv.starts_with("HTTP/1.1 200"), "{peer_conv}");
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
