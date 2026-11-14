//! The server on every catalog harness: create, repeated sends and turns,
//! and replies correlated through the AnyAgents tool server.
//!
//! The vendors' models cannot run here, so each agent's config names the
//! AnyAgents harness double (`anyagents harness <config>`), which obeys the
//! double contract: it takes each user line, calls `send` through the same
//! `anyagents mcp --agent <name>` the vendor is given, and ends the turn with a
//! `result`. That exercises the real path — create, supervisor, delivery loop,
//! harness launch, the mailbox cursor and the tool server — for every catalog
//! harness. The vendor protocols themselves are covered by each adapter's
//! protocol tests.

mod support;

use std::io::{Read, Write};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyagents_core::Store;
use serde_json::{Value, json};

struct Server {
    child: Child,
    _keepers: support::Keepers,
}

impl Server {
    fn start(dir: &Path, harness_bin: &Path) -> Self {
        Self::start_from(dir, harness_bin, Path::new(env!("CARGO_BIN_EXE_anyagents")))
    }

    /// A server run from `server_bin`, appending to the shared log so a test
    /// that restarts it keeps the whole story.
    fn start_from(dir: &Path, harness_bin: &Path, server_bin: &Path) -> Self {
        Self::start_with(dir, harness_bin, server_bin, &[])
    }

    /// [`Server::start_from`] with extra environment for the server (and so
    /// for the keepers it starts).
    fn start_with(dir: &Path, harness_bin: &Path, server_bin: &Path, env: &[(&str, &str)]) -> Self {
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("shared.log"))
            .unwrap();
        let mut child = support::contract_server_at(server_bin)
            .env("ANYAGENTS_HARNESS_BIN", harness_bin)
            .arg("--data")
            .arg(dir)
            .arg("serve")
            .stdout(Stdio::null())
            .stderr(log)
            .env("ANYAGENTS_SHARED_LOG", dir.join("shared.log"))
            .envs(env.iter().copied())
            .spawn()
            .expect("server");
        // A server that died leaves its socket file: ready is a connection.
        let socket = dir.join("anyagents.sock");
        let deadline = Instant::now() + Duration::from_secs(10);
        while std::os::unix::net::UnixStream::connect(&socket).is_err() {
            assert!(
                Instant::now() < deadline && child.try_wait().unwrap().is_none(),
                "no socket: {}",
                std::fs::read_to_string(dir.join("shared.log")).unwrap_or_default()
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        Self {
            child,
            _keepers: support::Keepers::of(dir),
        }
    }

    /// The server dies without a word, as a crash or an OOM kill does. Its
    /// keepers are left running.
    fn crash(mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        std::mem::forget(self);
    }

    /// A clean stop, as a deployment's restart sends.
    fn terminate(mut self) {
        // SAFETY: SIGTERM to the server this test started.
        unsafe { libc::kill(self.child.id() as i32, libc::SIGTERM) };
        let _ = self.child.wait();
        std::mem::forget(self);
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn fixture_agent_uids_never_match_the_server_uid() {
    // SAFETY: getuid takes no arguments and cannot fail.
    let uid = unsafe { libc::getuid() };
    let line = support::agents_line();
    let seats: Vec<_> = line
        .split(',')
        .map(|entry| {
            let (name, seat) = entry.split_once('=').unwrap();
            (name, seat.parse::<u32>().unwrap())
        })
        .collect();
    assert_eq!(seats.len(), 8);
    let unique: std::collections::HashSet<_> = seats.iter().map(|(_, seat)| seat).collect();
    assert_eq!(unique.len(), seats.len());
    for (name, seat) in seats {
        assert_ne!(seat, 0, "root is never a synthetic agent: {name}");
        assert_ne!(seat, uid, "{name} collides with the server's real UID");
    }
}

#[test]
fn synthetic_uid_selection_excludes_every_possible_candidate_collision() {
    let names = [
        "alpha", "beta", "ada", "bea", "cy", "kind", "gamma", "delta",
    ];
    for uid in [0, 1, 501, 1001, u32::MAX].into_iter().chain(2101..=2109) {
        let line = support::agents_line_for_uid(uid);
        let entries: Vec<_> = line.split(',').collect();
        assert_eq!(entries.len(), names.len());
        let mut seen = std::collections::HashSet::new();
        for (entry, expected_name) in entries.into_iter().zip(names) {
            let (name, seat) = entry.split_once('=').unwrap();
            let seat: u32 = seat.parse().unwrap();
            assert_eq!(name, expected_name);
            assert_ne!(seat, 0);
            assert_ne!(seat, uid, "{name} collides with server UID {uid}");
            assert!(seen.insert(seat), "duplicate fixture UID {seat}");
        }
    }
}

#[test]
fn the_real_server_refuses_a_matching_uid_but_accepts_the_fixture() {
    let _env = support::lock();
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    support::operator_for_contract(&data);
    create_asker(&data);
    let agent = Store::open(data.join("state.db"))
        .unwrap()
        .agent_by_name("kind")
        .unwrap();
    let principals = |seat_uid| {
        serde_json::json!({
            agent.id.as_str(): {"user":"worker", "uid":seat_uid, "gid":42002, "socket_gid":42003}
        })
        .to_string()
    };
    // SAFETY: getuid takes no arguments and cannot fail.
    let uid = unsafe { libc::getuid() };
    let log = data.join("matching-uid.log");
    let child = bin()
        .env("ANYAGENTS_PRINCIPALS", principals(uid))
        .arg("--data")
        .arg(&data)
        .arg("serve")
        .stdout(Stdio::null())
        .stderr(std::fs::File::create(&log).unwrap())
        .spawn()
        .unwrap();
    let mut refused = Server {
        child,
        _keepers: support::Keepers::of(&data),
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Some(status) = refused.child.try_wait().unwrap() {
            break status;
        }
        assert!(Instant::now() < deadline, "a matching UID was not refused");
        std::thread::sleep(Duration::from_millis(20));
    };
    let stderr = std::fs::read_to_string(log).unwrap();
    assert!(!status.success(), "{stderr}");
    assert!(
        stderr.contains("no OS seat is in place for its agents"),
        "{stderr}"
    );
    assert!(!data.join("anyagents.sock").exists());
    drop(refused);

    // Keep the configured seat and launcher on both sides. The protocol-only
    // Server::start helper intentionally removes those declarations and would
    // not distinguish a corrected map from an absent launch boundary.
    let accepted_log = data.join("distinct-uid.log");
    let child = bin()
        .env(
            "ANYAGENTS_PRINCIPALS",
            principals(if uid != 42001 { 42001 } else { 42004 }),
        )
        .arg("--data")
        .arg(&data)
        .arg("serve")
        .stdout(Stdio::null())
        .stderr(std::fs::File::create(&accepted_log).unwrap())
        .spawn()
        .unwrap();
    let mut accepted = Server {
        child,
        _keepers: support::Keepers::of(&data),
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    while !data.join("anyagents.sock").exists() {
        assert!(
            accepted.child.try_wait().unwrap().is_none() && Instant::now() < deadline,
            "the corrected fixture was not admitted: {}",
            std::fs::read_to_string(&accepted_log).unwrap_or_default()
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

use support::machine_path;

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
        // The model answers the message it was handed, by the mailbox reference in
        // the server's trusted context: recorded here so the test checks that.
        "context_reply_to": true,
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
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    // This agent is a mailbox, not another model. Keep it out of the
    // supervisor's eager launch population: only responders have an adapter.
    let paused = bin()
        .arg("--data")
        .arg(dir)
        .args(["agents", "pause", "kind"])
        .output()
        .unwrap();
    assert!(
        paused.status.success(),
        "{}",
        String::from_utf8_lossy(&paused.stderr)
    );
    assert_eq!(
        Store::open(dir.join("state.db"))
            .unwrap()
            .agent_by_name("kind")
            .unwrap()
            .state,
        "paused"
    );
}

fn call(socket: &Path, request: &str) -> Value {
    let mut stream = std::os::unix::net::UnixStream::connect(socket).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    stream
        .write_all(
            format!(
                "POST /v1/send HTTP/1.1\r\nHost: local\r\n{}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{request}",
                support::header(),
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
    let author = store.agent_by_name(name).unwrap();
    let expected_sender = format!("agent:{}", author.id);
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let inbox = store.inbox(&agent.id, None).unwrap();
        // The answer names the message it answers by the reference the server
        // put in its trusted context, and the server resolved that reference
        // back to the asker: this mailbox holds it, from that agent.
        let answers: Vec<_> = inbox
            .iter()
            .filter(|message| {
                message.text == "pong"
                    && message.reply_to.as_deref() == Some(sent_id)
                    && message
                        .sender
                        .as_deref()
                        .is_some_and(|sender| sender == expected_sender)
            })
            .collect();
        assert!(
            answers.len() <= 1,
            "duplicate replies to {sent_id}: {answers:?}"
        );
        if !answers.is_empty() {
            assert_eq!(store.agent_by_name("kind").unwrap().state, "paused");
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

/// Each case has its own server and store so a reply from another harness or
/// an earlier turn cannot satisfy it. These exercise the common contract-double
/// launch route, not the native vendor wire protocols or live authentication.
fn repeated_replies(harness: &str) {
    let _env = support::lock();
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    support::operator_for_contract(&data);
    create_asker(&data);
    let name = "responder";
    let agent_file = data.join("responder-name");
    std::fs::write(&agent_file, name).unwrap();
    create(&data, name, harness, &agent_file);
    let _server = Server::start(&data, Path::new(env!("CARGO_BIN_EXE_anyagents")));
    one_round(&data, name, harness);
    one_round(&data, name, harness);
    // The double ends each turn with a `result` line: the turn closes, and
    // nothing is left open for a recovery note.
    let store = Store::open(data.join("state.db")).unwrap();
    let responder = store.agent_by_name(name).unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    while let Some(open) = store.open_turn(&responder.id).unwrap() {
        assert!(
            Instant::now() < deadline,
            "{harness}: an answered turn stayed open: {open:?}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

macro_rules! reply_cases {
    ($($test:ident => $harness:literal),+ $(,)?) => {
        $(#[test]
        fn $test() {
            repeated_replies($harness);
        })+
    };
}

reply_cases! {
    claude_code_replies_to_each_turn => "claude-code",
    codex_replies_to_each_turn => "codex",
    opencode_replies_to_each_turn => "opencode",
    omp_replies_to_each_turn => "omp",
    dsh_replies_to_each_turn => "dsh",
    grok_replies_to_each_turn => "grok",
}

#[test]
fn an_agy_agent_and_a_muse_agent_each_answer_a_mailbox_message() {
    let _env = support::lock();
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    support::operator_for_contract(&data);
    // The agent that asks: the answers come back to it.
    create_asker(&data);
    let (agy_file, muse_file) = (data.join("agent-gamma"), data.join("agent-delta"));
    std::fs::write(&agy_file, "gamma").unwrap();
    std::fs::write(&muse_file, "delta").unwrap();
    let agy_id = create(&data, "gamma", "agy", &agy_file);
    let muse_id = create(&data, "delta", "muse", &muse_file);

    // The Muse config carries the tool server, written by create before the
    // server was up. The Antigravity CLI keeps its own config.
    let agy_config = data.join("agents").join(&agy_id).join("config");
    assert!(
        agy_config.is_dir(),
        "the agy agent has its config directory"
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
    one_round(&data, "gamma", "agy");
    one_round(&data, "delta", "muse");
    // A second turn must produce a new reply, not pass by finding the first
    // turn's pong in the asker's persistent mailbox.
    one_round(&data, "gamma", "agy");
    one_round(&data, "delta", "muse");

    // The catalog names both harnesses and their config variable.
    let listed = bin().args(["harnesses", "list"]).output().unwrap();
    let text = String::from_utf8_lossy(&listed.stdout);
    assert!(text.contains("agy\tAGY_CONFIG_DIR\t"), "{text}");
    assert!(text.contains("muse\tMUSE_CONFIG_HOME\t"), "{text}");
    drop(server);
}

/// Send `text` to `name` as the asker; the id of the message it became.
fn send(data: &Path, name: &str) -> String {
    let sent = call(
        &data.join("anyagents.sock"),
        &json!({"to": [name], "text": "ping", "sender": "agent:kind"}).to_string(),
    );
    let text = sent.to_string();
    sent["deliveries"][0]["id"]
        .as_str()
        .expect(&text)
        .to_string()
}

/// Wait for the shared log to say `line`; on failure show the log.
fn wait_logged(data: &Path, line: &str, why: &str) {
    let deadline = Instant::now() + support::WAIT;
    while !shared_log(data).contains(line) {
        assert!(Instant::now() < deadline, "{why}:\n{}", shared_log(data));
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// How many answers to `sent_id` reached the asker.
fn answers(data: &Path, sent_id: &str) -> usize {
    let store = Store::open(data.join("state.db")).unwrap();
    let asker = store.agent_by_name("kind").unwrap();
    store
        .inbox(&asker.id, None)
        .unwrap()
        .iter()
        .filter(|message| message.text == "pong" && message.reply_to.as_deref() == Some(sent_id))
        .count()
}

/// Make the agent's contract double hold each turn open while `hold` exists.
fn hold_turns(data: &Path, agent_id: &str, hold: &Path) {
    let path = data
        .join("agents")
        .join(agent_id)
        .join("config")
        .join("adapter.json");
    let mut adapter: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    adapter["hold_while"] = json!(hold.display().to_string());
    std::fs::write(&path, serde_json::to_vec_pretty(&adapter).unwrap()).unwrap();
}

/// The harness process the agent's newest live keeper holds.
fn kept_harness(data: &Path, agent_id: &str) -> u32 {
    let mut found = None;
    support::wait_for(
        || {
            let Ok(entries) = std::fs::read_dir(data.join("keepers")) else {
                return false;
            };
            let mut dirs: Vec<_> = entries.flatten().map(|entry| entry.path()).collect();
            dirs.sort();
            found = dirs.iter().rev().find_map(|dir| {
                let agent = std::fs::read_to_string(dir.join("agent")).ok()?;
                if agent.trim() != agent_id {
                    return None;
                }
                let record: Value =
                    serde_json::from_slice(&std::fs::read(dir.join("keeper.json")).ok()?).ok()?;
                let pid = u32::try_from(record["harness_process"]["pid"].as_u64()?).ok()?;
                alive(pid).then_some(pid)
            });
            found.is_some()
        },
        "no keeper holds a live harness for the agent",
    );
    found.unwrap()
}

/// No keeper directory serves the agent.
fn no_keeper(data: &Path, agent_id: &str) -> bool {
    let Ok(entries) = std::fs::read_dir(data.join("keepers")) else {
        return true;
    };
    entries.flatten().all(|entry| {
        std::fs::read_to_string(entry.path().join("agent"))
            .ok()
            .is_none_or(|agent| agent.trim() != agent_id)
    })
}

fn alive(pid: u32) -> bool {
    // SAFETY: signal 0 only asks whether the process exists.
    unsafe { libc::kill(pid as i32, 0) == 0 }
}

fn shared_log(data: &Path) -> String {
    std::fs::read_to_string(data.join("shared.log")).unwrap_or_default()
}

/// A running agent's harness outlives its server. The server crashes in the
/// middle of a turn with a second message already handed over, and comes back
/// as another binary (an upgrade); later it stops cleanly in the middle of a
/// turn and starts again. Each time the same vendor process finishes the turn,
/// every message is answered exactly once, and delivery resumes from the
/// mailbox cursor.
#[test]
fn a_harness_outlives_a_server_crash_an_upgrade_and_a_clean_stop() {
    let _env = support::lock();
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    support::operator_for_contract(&data);
    create_asker(&data);
    let name = "responder";
    let agent_file = data.join("responder-name");
    std::fs::write(&agent_file, name).unwrap();
    let id = create(&data, name, "claude-code", &agent_file);
    let hold = data.join("hold-turn");
    hold_turns(&data, &id, &hold);
    let anyagents = Path::new(env!("CARGO_BIN_EXE_anyagents"));
    let agent_id = anyagents_core::AgentId::parse(&id).unwrap();

    // A turn in progress, and a second message handed over behind it.
    std::fs::write(&hold, b"").unwrap();
    let server = Server::start(&data, anyagents);
    let first = send(&data, name);
    support::wait_for(
        || shared_log(&data).contains(&format!("holding {first}")),
        "the first turn did not start",
    );
    let second = send(&data, name);
    let store = Store::open(data.join("state.db")).unwrap();
    support::wait_for(
        || store.next_undelivered(&agent_id).unwrap().is_none(),
        "the second message was not handed over",
    );
    let harness = kept_harness(&data, &id);

    server.crash();
    assert!(alive(harness), "the server's crash ended the harness");
    let upgraded = dir.path().join("anyagents-next");
    std::fs::copy(anyagents, &upgraded).unwrap();
    let server = Server::start_from(&data, anyagents, &upgraded);
    assert_eq!(
        kept_harness(&data, &id),
        harness,
        "the upgraded server started another harness"
    );
    std::fs::remove_file(&hold).unwrap();
    ask(&data, name, "claude-code", &first);
    ask(&data, name, "claude-code", &second);

    // A clean stop in the middle of the next turn.
    std::fs::write(&hold, b"").unwrap();
    let third = send(&data, name);
    support::wait_for(
        || shared_log(&data).contains(&format!("holding {third}")),
        "the third turn did not start",
    );
    let harness = kept_harness(&data, &id);
    server.terminate();
    assert!(alive(harness), "a clean stop ended the harness");
    let server = Server::start(&data, anyagents);
    assert_eq!(kept_harness(&data, &id), harness);
    std::fs::remove_file(&hold).unwrap();
    ask(&data, name, "claude-code", &third);

    for sent in [&first, &second, &third] {
        assert_eq!(
            answers(&data, sent),
            1,
            "{sent} was not answered exactly once"
        );
    }
    let starts = shared_log(&data).matches("harness: start").count();
    assert!(starts <= 2, "the harness double started {starts} times");
    drop(server);
    support::wait_for(|| !alive(harness), "stopping the keepers left the harness");
}

/// The real binary across protocol versions. A server whose keepers speak a
/// range overlapping the next server's has them taken over; one whose
/// keepers speak no version the next server speaks has them stopped and
/// started again, and mail sent before and after is answered exactly once.
#[test]
fn keepers_across_protocol_versions_are_taken_over_or_replaced() {
    let _env = support::lock();
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    support::operator_for_contract(&data);
    create_asker(&data);
    let name = "responder";
    let agent_file = data.join("responder-name");
    std::fs::write(&agent_file, name).unwrap();
    let id = create(&data, name, "claude-code", &agent_file);
    let hold = data.join("hold-turn");
    hold_turns(&data, &id, &hold);
    let anyagents = Path::new(env!("CARGO_BIN_EXE_anyagents"));
    let speaking = |range: &'static str| [(anyagents_keeper::PROTOCOL_ENV, range)];

    // A newer binary that also speaks this one's version: taken over.
    std::fs::write(&hold, b"").unwrap();
    let server = Server::start_with(&data, anyagents, anyagents, &speaking("2-3"));
    let first = send(&data, name);
    support::wait_for(
        || shared_log(&data).contains(&format!("holding {first}")),
        "the first turn did not start",
    );
    let harness = kept_harness(&data, &id);
    server.crash();
    let server = Server::start(&data, anyagents);
    assert_eq!(
        kept_harness(&data, &id),
        harness,
        "a shared version was not spoken"
    );
    std::fs::remove_file(&hold).unwrap();
    ask(&data, name, "claude-code", &first);
    // A keeper only the later binary speaks to: none of the older ones
    // may be left, so they are stopped while no server runs.
    std::fs::write(&hold, b"").unwrap();
    server.crash();
    let stopped = Command::new(anyagents)
        .arg("--data")
        .arg(&data)
        .args(["keepers", "stop", "--all"])
        .output()
        .unwrap();
    assert!(
        stopped.status.success(),
        "{}",
        String::from_utf8_lossy(&stopped.stderr)
    );
    assert!(no_keeper(&data, &id));

    // A binary that speaks only a later version: stopped and replaced. The
    // turn it held is interrupted once, and recovered by the replacement.
    let server = Server::start_with(&data, anyagents, anyagents, &speaking("3-3"));
    let second = send(&data, name);
    wait_logged(
        &data,
        &format!("holding {second}"),
        "the second turn did not start",
    );
    let unspoken = kept_harness(&data, &id);
    server.crash();
    let server = Server::start(&data, anyagents);
    support::wait_for(
        || !alive(unspoken),
        "a keeper no version is shared with was left",
    );
    assert_ne!(kept_harness(&data, &id), unspoken);
    std::fs::remove_file(&hold).unwrap();
    ask(&data, name, "claude-code", &second);
    for sent in [&first, &second] {
        assert_eq!(
            answers(&data, sent),
            1,
            "{sent} was not answered exactly once"
        );
    }
    drop(server);
}

/// A harness kept across a restart sends its gateway token to the port the
/// last server held. When something else listens there by the next start,
/// the gateway moves: every agent's token is replaced, and an API-key agent's
/// kept harness is replaced rather than taken over, so none keeps sending its
/// token to the old port. The turn it held is recovered and answered once.
#[test]
fn a_moved_gateway_replaces_the_kept_harnesses_of_api_key_agents() {
    let _env = support::lock();
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    support::operator_for_contract(&data);
    create_asker(&data);
    let name = "responder";
    let agent_file = data.join("responder-name");
    std::fs::write(&agent_file, name).unwrap();
    let id = create(&data, name, "claude-code", &agent_file);
    let key_file = dir.path().join("key");
    std::fs::write(&key_file, "sk-provider-secret\n").unwrap();
    let keyed = bin()
        .arg("--data")
        .arg(&data)
        .args([
            "model",
            name,
            "--base-url",
            "http://127.0.0.1:9",
            "--model",
            "claude",
        ])
        .arg("--key-file")
        .arg(&key_file)
        .output()
        .unwrap();
    assert!(
        keyed.status.success(),
        "{}",
        String::from_utf8_lossy(&keyed.stderr)
    );
    let hold = data.join("hold-turn");
    hold_turns(&data, &id, &hold);
    std::fs::write(&hold, b"").unwrap();
    let anyagents = Path::new(env!("CARGO_BIN_EXE_anyagents"));
    let server = Server::start(&data, anyagents);
    let sent = send(&data, name);
    wait_logged(&data, &format!("holding {sent}"), "the turn did not start");
    let harness = kept_harness(&data, &id);
    let token_file = data.join("agents").join(&id).join("gateway-token.txt");
    let token = std::fs::read_to_string(&token_file).unwrap();
    let port: u16 = std::fs::read_to_string(data.join("gateway.port"))
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    server.crash();

    // Something else takes the port while no server runs.
    let squatter = std::net::TcpListener::bind(("127.0.0.1", port)).unwrap();
    let server = Server::start(&data, anyagents);
    support::wait_for(|| !alive(harness), "the kept harness was taken over");
    let moved: u16 = std::fs::read_to_string(data.join("gateway.port"))
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert_ne!(moved, port, "the gateway stayed on a port it does not hold");
    assert_ne!(
        std::fs::read_to_string(&token_file).unwrap(),
        token,
        "the token handed out before the move still works"
    );
    assert_ne!(kept_harness(&data, &id), harness);
    std::fs::remove_file(&hold).unwrap();
    ask(&data, name, "claude-code", &sent);
    assert_eq!(
        answers(&data, &sent),
        1,
        "{sent} was not answered exactly once"
    );
    drop(server);
    drop(squatter);
}

/// Stopping keepers is the step before a rollback to a binary without them.
/// It refuses while a server runs (that server would start them again),
/// exits non-zero when it refuses, and once no server runs it ends every
/// harness and frees every agent lock.
#[test]
fn keepers_stop_only_when_no_server_runs() {
    let _env = support::lock();
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    support::operator_for_contract(&data);
    create_asker(&data);
    let name = "responder";
    let agent_file = data.join("responder-name");
    std::fs::write(&agent_file, name).unwrap();
    let id = create(&data, name, "claude-code", &agent_file);
    let hold = data.join("hold-turn");
    hold_turns(&data, &id, &hold);
    std::fs::write(&hold, b"").unwrap();
    let anyagents = Path::new(env!("CARGO_BIN_EXE_anyagents"));
    let server = Server::start(&data, anyagents);
    let sent = send(&data, name);
    support::wait_for(
        || shared_log(&data).contains(&format!("holding {sent}")),
        "the turn did not start",
    );
    let harness = kept_harness(&data, &id);
    let stop = || {
        Command::new(anyagents)
            .arg("--data")
            .arg(&data)
            .args(["keepers", "stop", "--all"])
            .output()
            .unwrap()
    };
    let refused = stop();
    assert!(
        !refused.status.success(),
        "stop went ahead under a running server"
    );
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains("a server is running"),
        "{}",
        String::from_utf8_lossy(&refused.stderr)
    );
    assert!(alive(harness));
    server.terminate();
    assert!(
        alive(harness),
        "a clean stop of the server ended the harness"
    );
    let stopped = stop();
    assert!(
        stopped.status.success(),
        "{}",
        String::from_utf8_lossy(&stopped.stderr)
    );
    support::wait_for(|| !alive(harness), "stop left the harness running");
    let store = Store::open(data.join("state.db")).unwrap();
    let agent = anyagents_core::AgentId::parse(&id).unwrap();
    drop(
        store
            .try_lock_agent(&agent)
            .expect("the agent lock is still held"),
    );
}

/// A real adapter's dedupe under a keeper: Spiron (here its protocol fake)
/// has a prompt and a steer written into a turn it has not started when the
/// server crashes. The next server, another binary, offers both again; the
/// adapter in the keeper answers from its memory, and the vendor process reads
/// each exactly once.
#[test]
fn a_spiron_adapter_in_its_keeper_never_writes_a_line_twice_across_a_restart() {
    use std::os::unix::fs::PermissionsExt as _;
    let _env = support::lock();
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    support::operator_for_contract(&data);
    create_asker(&data);
    let name = "spirited";
    let out = bin()
        .env("PATH", machine_path(&data))
        .arg("--data")
        .arg(&data)
        .args(["agents", "create", name, "--harness", "spiron"])
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
    let config = data.join("agents").join(&id).join("config");
    let fake = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../anyagents-harness-stream/tests/fake_spiron.py");
    let spiron = data.join("bin").join("spiron-fake");
    std::fs::write(
        &spiron,
        format!(
            "#!/bin/sh\nexec /usr/bin/python3 {} \"$@\"\n",
            fake.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&spiron, std::fs::Permissions::from_mode(0o755)).unwrap();
    let spiron_env = [
        ("ANYAGENTS_SPIRON_BIN", spiron.to_str().unwrap()),
        ("FAKE_SPIRON_CAPABLE", "1"),
    ];
    let anyagents = Path::new(env!("CARGO_BIN_EXE_anyagents"));
    // No turn is announced yet: the prompt and the steer stay in flight.
    std::fs::write(config.join("hold"), b"").unwrap();
    let server = Server::start_with(&data, anyagents, anyagents, &spiron_env);
    let first = send(&data, name);
    let second = send(&data, name);
    let wire = || -> Vec<(u64, String)> {
        std::fs::read_to_string(config.join("wire.jsonl"))
            .unwrap_or_default()
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .filter_map(|line| {
                Some((
                    line["launch"].as_u64()?,
                    line["msg"]["request_id"].as_str()?.to_string(),
                ))
            })
            .collect()
    };
    support::wait_for(
        || wire().len() == 2,
        "the prompt and the steer did not reach the vendor",
    );
    let harness = kept_harness(&data, &id);
    server.crash();
    let upgraded = dir.path().join("anyagents-next");
    std::fs::copy(anyagents, &upgraded).unwrap();
    let server = Server::start_with(&data, anyagents, &upgraded, &spiron_env);
    assert_eq!(
        kept_harness(&data, &id),
        harness,
        "the vendor process was restarted"
    );
    // The turn starts, its step journals the steer, and it ends.
    std::fs::remove_file(config.join("hold")).unwrap();
    std::fs::write(config.join("step"), b"").unwrap();
    let store = Store::open(data.join("state.db")).unwrap();
    let agent = anyagents_core::AgentId::parse(&id).unwrap();
    let last = store.inbox(&agent, None).unwrap().last().unwrap().seq;
    support::wait_for(
        || store.delivered_cursor(&agent).unwrap() >= last,
        "the consumed lines were not committed",
    );
    assert_eq!(
        wire(),
        vec![(1, first.clone()), (1, second.clone())],
        "a line was written twice, or by another process"
    );
    assert_eq!(
        std::fs::read_to_string(config.join("launches"))
            .unwrap()
            .trim(),
        "1"
    );
    drop(server);
}
