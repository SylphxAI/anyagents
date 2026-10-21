//! The server on an Antigravity agent and on a Muse agent: create, one send,
//! one turn, and the reply the agent made through the AnyAgents tool server.
//!
//! The vendors' models cannot run here, so each agent's config names the
//! AnyAgents harness double (`anyagents harness <config>`), which obeys the
//! adapter contract: it takes each user line, calls `send` through the same
//! `anyagents mcp --agent <name>` the vendor is given, and ends the turn with a
//! `result`. That exercises the real path — create, supervisor, delivery loop,
//! the adapter's create line, the mailbox cursor and the tool server — for both
//! harnesses. The vendor protocols themselves are covered by each adapter's
//! protocol tests.

use std::io::{Read, Write};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyagents_core::Store;
use serde_json::{Value, json};

struct Server {
    child: Child,
}

impl Server {
    fn start(dir: &Path, harness_bin: &Path) -> Self {
        let child = Command::new(env!("CARGO_BIN_EXE_anyagents"))
            .env("ANYAGENTS_HARNESS_BIN", harness_bin)
            .arg("--data")
            .arg(dir)
            .arg("serve")
            .stdout(Stdio::null())
            .stderr(std::fs::File::create(dir.join("shared.log")).unwrap())
            .env("ANYAGENTS_FAKE_LOG", dir.join("shared.log"))
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

/// A `PATH` where creating an agent installs nothing: `npm` answers a version
/// and a no-op install. The harnesses the tests run are on the machine already.
fn machine_path(dir: &Path) -> std::ffi::OsString {
    use std::os::unix::fs::PermissionsExt;
    let bin = dir.join("bin");
    std::fs::create_dir_all(&bin).unwrap();
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

fn bin() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_anyagents"));
    command.env("ANYAGENTS_HARNESS_BIN", env!("CARGO_BIN_EXE_anyagents"));
    command
}

/// Create an agent whose config names the contract double.
fn create(dir: &Path, name: &str, harness: &str, agent_file: &Path) -> String {
    let out = bin()
        .env("PATH", machine_path(dir))
        .arg("--data")
        .arg(dir)
        .args(["agents", "create", name, "--harness", harness])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let id = String::from_utf8(out.stdout)
        .unwrap()
        .lines()
        .next()
        .unwrap()
        .split('\t')
        .next()
        .unwrap()
        .to_string();
    let config = dir.join("agents").join(&id).join("config");
    let adapter = json!({
        "harness": harness,
        "reply_with": format!("{} --data {} mcp", env!("CARGO_BIN_EXE_anyagents"), dir.display()),
        "agent_file": agent_file.display().to_string(),
        // The double answers as `kind`, the agent that asks below.
        "sender": "agent:kind",
        "reply_to": "kind",
    });
    std::fs::write(
        config.join("adapter.json"),
        serde_json::to_vec_pretty(&adapter).unwrap(),
    )
    .unwrap();
    id
}

/// An agent on another harness, created before the one that answers.
fn create_asker(dir: &Path) {
    let out = Command::new(env!("CARGO_BIN_EXE_anyagents"))
        .env("PATH", machine_path(dir))
        .arg("--data")
        .arg(dir)
        .args(["agents", "create", "kind", "--harness", "claude-code"])
        .output()
        .unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
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

/// The answer comes back to the agent that asked, through its `send` call.
fn ask(dir: &Path, name: &str, harness: &str, sent_id: &str) {
    let store = Store::open(dir.join("state.db")).unwrap();
    let agent = store.agent_by_name("kind").unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let inbox = store.inbox(&agent.id, None).unwrap();
        let answered = inbox
            .iter()
            .any(|message| message.reply_to.as_deref() == Some(sent_id));
        if answered {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "{name} on {harness} did not answer through its tool server: {inbox:?}\n{}",
            std::fs::read_to_string(dir.join("shared.log")).unwrap_or_default()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn one_round(data: &Path, name: &str, harness: &str) {
    let socket = data.join("anyagents.sock");
    let sent = call(
        &socket,
        &json!({"to": [name], "text": "ping", "sender": "agent:kind"}).to_string(),
    );
    let text = sent.to_string();
    let sent_id = sent["deliveries"][0]["id"].as_str().expect(&text);
    ask(data, name, harness, sent_id);
}

#[test]
fn an_agy_agent_and_a_muse_agent_each_answer_a_mailbox_message() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    // The agent that asks: the answers come back to it.
    create_asker(&data);
    let (agy_file, muse_file) = (data.join("agent-argon"), data.join("agent-calliope"));
    std::fs::write(&agy_file, "argon").unwrap();
    std::fs::write(&muse_file, "calliope").unwrap();
    let agy_id = create(&data, "argon", "agy", &agy_file);
    let muse_id = create(&data, "calliope", "muse", &muse_file);

    // The agy config carries the steering hook; the Muse one carries the tool
    // server. Both were written by create, before the server was up.
    let agy_config = data.join("agents").join(&agy_id).join("config");
    // The adapter registers the hook in `hooks.json`, the file it also serves.
    let settings: Value =
        serde_json::from_slice(&std::fs::read(agy_config.join("hooks.json")).unwrap()).unwrap();
    // The hook `agy` reads: `hooks.PreInvocation`, one entry holding hooks.
    let entries = settings["hooks"]["PreInvocation"]
        .as_array()
        .unwrap_or_else(|| panic!("no PreInvocation: {settings}"));
    assert_eq!(entries.len(), 1, "{settings}");
    let command = &entries[0]["hooks"][0]["command"];
    assert!(
        command.as_str().unwrap().ends_with("agy-steer-hook"),
        "{settings}"
    );
    let muse_config = data
        .join("agents")
        .join(&muse_id)
        .join("config")
        .join("settings.json");
    let muse_settings: Value =
        serde_json::from_slice(&std::fs::read(muse_config).unwrap()).unwrap();
    assert_eq!(muse_settings["mcpServers"]["anyagents"]["mode"], "required");

    let server = Server::start(&data, Path::new(env!("CARGO_BIN_EXE_anyagents")));
    one_round(&data, "argon", "agy");
    one_round(&data, "calliope", "muse");

    // The catalog names both harnesses and their config variable.
    let listed = bin().args(["harnesses", "list"]).output().unwrap();
    let text = String::from_utf8_lossy(&listed.stdout);
    assert!(text.contains("agy\tAGY_CONFIG_DIR\t"), "{text}");
    assert!(text.contains("muse\tMUSE_CONFIG_HOME\t"), "{text}");
    drop(server);
}
