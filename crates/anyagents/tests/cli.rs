mod support;

#[path = "support/archive_reap.rs"]
mod archive_reap;

use std::process::Command;

use anyagents_core::{BillingMode, NewUsage, Store};

use support::machine_path;

fn bin() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_anyagents"));
    // These commands act as the fixture's operator, not its executor's agent.
    command
        .env_remove("ANYAGENTS_AGENT_ID")
        .env_remove("ANYAGENTS_AGENT_CONFIG")
        .env_remove("ANYAGENTS_DATA")
        .env_remove("ANYAGENTS_PARENT_NAME")
        .env_remove("ANYAGENTS_OWNER_WORKDIR");
    command
}

/// A command in a fresh data directory whose harness needs no download.
fn bin_in(dir: &std::path::Path) -> Command {
    let mut command = bin();
    command
        .env("PATH", machine_path(dir))
        .env("ANYAGENTS_TESTS", "1")
        .env("ANYAGENTS_CLAUDE_CODE_BIN", env!("CARGO_BIN_EXE_anyagents"))
        .arg("--data")
        .arg(dir);
    command
}

#[test]
fn offline_fixture_creates_verified_receipts_without_runtime_downloads() {
    let dir = tempfile::tempdir().unwrap();
    for harness in ["claude-code", "omp", "agy"] {
        let out = bin_in(dir.path())
            .args(["agents", "create", harness, "--harness", harness])
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        let root = dir.path().join("harnesses").join(harness);
        let receipt: serde_json::Value =
            serde_json::from_slice(&std::fs::read(root.join("0.0.0/receipt.json")).unwrap())
                .unwrap();
        assert_eq!(receipt["state"], "ok");
        assert_eq!(receipt["doctor"]["ok"], true);
        assert!(receipt["digest"].as_str().unwrap().starts_with("sha256:"));
        assert_eq!(receipt["launcher"], "bin/harness");
        let current: serde_json::Value =
            serde_json::from_slice(&std::fs::read(root.join("current.json")).unwrap()).unwrap();
        assert_eq!(current["version"], "0.0.0");
    }
    assert!(!dir.path().join("runtimes").exists());
}

#[test]
fn offline_fixture_rejects_tampered_and_undeclared_sources() {
    let dir = tempfile::tempdir().unwrap();
    support::managed_harnesses(dir.path());
    std::fs::write(dir.path().join("bin/managed-harness"), "tampered").unwrap();
    let out = bin_in(dir.path())
        .args(["agents", "create", "tampered", "--harness", "agy"])
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("checksum mismatch"));
    assert!(!dir.path().join("harnesses/agy/current.json").exists());

    // An explicit empty declaration keeps the production fail-closed default.
    std::fs::write(dir.path().join("harnesses.toml"), "").unwrap();
    let out = bin_in(dir.path())
        .args(["agents", "create", "undeclared", "--harness", "grok"])
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("unverifiable source for grok"));
    assert!(!dir.path().join("harnesses/grok/current.json").exists());
}

#[test]
fn configured_owner_catalog_is_authoritative_for_create_discovery_and_updates() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let owner = dir.path().join("owner");
    let workspace = dir.path().join("workspace");
    let home = dir.path().join("home");
    for base in [&data, &workspace, &home] {
        std::fs::create_dir_all(base.join(".agents/agents")).unwrap();
        std::fs::write(base.join(".agents/agents/retired.md"), "Stale template.").unwrap();
        std::fs::write(base.join(".agents/agents/worker.md"), "Wrong template.").unwrap();
    }
    for root in ["agents-templates", "archetypes"] {
        std::fs::create_dir_all(data.join(root)).unwrap();
        std::fs::write(data.join(root).join("retired.md"), "Stale template.").unwrap();
    }
    let run = |args: &[&str]| {
        bin_in(&data)
            .env("ANYAGENTS_OWNER_WORKDIR", &owner)
            .env("HOME", &home)
            .args(args)
            .output()
            .unwrap()
    };
    let workspace = workspace.to_str().unwrap();
    // A no-role agent remains possible, even before the catalog exists.
    let created = run(&["agents", "create", "plain", "--workdir", workspace]);
    assert!(
        created.status.success(),
        "{}",
        String::from_utf8_lossy(&created.stderr)
    );
    for state in ["missing", "empty", "populated"] {
        if state == "empty" {
            std::fs::create_dir_all(owner.join(".agents/agents")).unwrap();
        } else if state == "populated" {
            std::fs::write(
                owner.join(".agents/agents/worker.md"),
                "---\nname: worker\ndescription: Current worker\n---\nAuthoritative template.",
            )
            .unwrap();
        }
        for role in ["retired", "unknown"] {
            let rejected = run(&[
                "agents",
                "create",
                "rejected",
                "--workdir",
                workspace,
                "--role",
                role,
            ]);
            assert!(!rejected.status.success(), "{state}: accepted {role}");
            assert!(
                String::from_utf8_lossy(&rejected.stderr).contains("unknown archetype template"),
                "{}",
                String::from_utf8_lossy(&rejected.stderr)
            );
            let update = run(&["role", "plain", role]);
            assert!(
                !update.status.success(),
                "{state}: accepted role update {role}"
            );
        }
    }
    let created = run(&[
        "agents",
        "create",
        "current",
        "--workdir",
        workspace,
        "--role",
        "worker",
    ]);
    assert!(
        created.status.success(),
        "{}",
        String::from_utf8_lossy(&created.stderr)
    );
    let update = run(&["role", "plain", "worker"]);
    assert!(
        update.status.success(),
        "{}",
        String::from_utf8_lossy(&update.stderr)
    );
    let store = Store::open(data.join("state.db")).unwrap();
    assert!(store.agent_by_name("rejected").is_err());
    let agent = store.agent_by_name("current").unwrap();
    assert_eq!(agent.role.as_deref(), Some("worker"));
    assert!(
        agent
            .instructions
            .as_deref()
            .unwrap()
            .contains("Authoritative template.")
    );
    let prompt = std::fs::read_to_string(
        data.join("agents")
            .join(agent.id.as_str())
            .join("config/AGENTS.md"),
    )
    .unwrap();
    let catalog = prompt
        .lines()
        .find(|line| line.contains("Available sub-agent roles"))
        .unwrap();
    assert!(catalog.contains("worker"), "{catalog}");
    assert!(!catalog.contains("retired"), "{catalog}");

    // Removing the last owner role must not resurrect an identically named fallback.
    std::fs::remove_file(owner.join(".agents/agents/worker.md")).unwrap();
    let rejected = run(&["agents", "create", "removed", "--role", "worker"]);
    assert!(!rejected.status.success());
    let update = run(&["role", "plain", "worker"]);
    assert!(!update.status.success());
    assert!(store.agent_by_name("removed").is_err());
}

#[test]
fn operator_fixture_ignores_an_inherited_agent_session() {
    let inherited = tempfile::tempdir().unwrap();
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "create_send_inbox_and_usage", "--nocapture"])
        .env(
            "ANYAGENTS_AGENT_ID",
            anyagents_core::AgentId::generate().as_str(),
        )
        .env("ANYAGENTS_AGENT_CONFIG", inherited.path().join("config"))
        .env("ANYAGENTS_DATA", inherited.path().join("data"))
        .env("ANYAGENTS_PARENT_NAME", "inherited-parent")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "the operator fixture inherited an agent session:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("1 passed; 0 failed"),
        "the nested operator fixture must execute, not select zero tests"
    );
    assert!(!inherited.path().join("data").exists());
    assert!(!inherited.path().join("config").exists());
}

#[test]
fn mcp_without_a_server_fails() {
    let dir = tempfile::tempdir().unwrap();
    let mcp = bin()
        .arg("--data")
        .arg(dir.path())
        .arg("mcp")
        .arg("--config")
        .arg(dir.path().join("agents").join("alpha").join("config"))
        .output()
        .unwrap();
    assert!(!mcp.status.success());
    let stderr = String::from_utf8(mcp.stderr).unwrap();
    // Nothing starts without the one capability its own config holds.
    assert!(stderr.contains("no mailbox capability"), "{stderr}");
}

#[test]
fn operator_read_commands_fall_back_on_socket_failure_but_agents_do_not() {
    use anyagents_core::{Incoming, NewAgent};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let own = store
        .create_agent(NewAgent::new("fallback", "spiron"))
        .unwrap();
    store
        .receive(
            &own.id,
            Incoming {
                text: "offline read canary".into(),
                sender: Some("operator".into()),
                source_time_ms: None,
                source_key: None,
                reply_to: None,
                quote: None,
                origin: None,
                received_at_ms: None,
            },
        )
        .unwrap();
    let token = dir.path().join("operator-token");
    std::fs::write(&token, "fixture-token").unwrap();
    let socket = dir.path().join("anyagents.sock");
    // A stale socket must trigger the same safe fallback as a disconnected read.
    drop(std::os::unix::net::UnixListener::bind(&socket).unwrap());
    for args in [
        vec!["agents", "list", "-d", "1"],
        vec!["logs", "fallback", "-n", "5", "--json"],
        vec!["inbox", "fallback", "-n", "5", "--json"],
        vec!["inspect", "fallback", "-n", "5", "--json"],
        vec!["status", "--json"],
    ] {
        let output = bin()
            .env("ANYAGENTS_TESTS", "1")
            .env("ANYAGENTS_OPERATOR_TOKEN_FILE", &token)
            .arg("--data")
            .arg(dir.path())
            .args(&args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let expected = if args[0] == "inbox" {
            "offline read canary"
        } else {
            "fallback"
        };
        assert!(
            String::from_utf8_lossy(&output.stdout).contains(expected),
            "{args:?}"
        );
        let output = bin()
            .env("ANYAGENTS_TESTS", "1")
            .env("ANYAGENTS_OPERATOR_TOKEN_FILE", &token)
            .env("ANYAGENTS_AGENT_ID", own.id.as_str())
            .arg("--data")
            .arg(dir.path())
            .args(&args)
            .output()
            .unwrap();
        assert!(
            !output.status.success(),
            "agent used offline fallback: {args:?}"
        );
        assert!(!String::from_utf8_lossy(&output.stdout).contains("offline read canary"));
    }
}

#[test]
fn operator_read_fallback_handles_disconnect_without_hiding_http_refusals() {
    use std::io::{Read, Write};
    let dir = tempfile::tempdir().unwrap();
    Store::open(dir.path().join("state.db"))
        .unwrap()
        .create_agent(anyagents_core::NewAgent::new("socket-reader", "spiron"))
        .unwrap();
    let token = dir.path().join("operator-token");
    std::fs::write(&token, "fixture-token").unwrap();
    for refuse in [false, true] {
        let socket = dir.path().join("anyagents.sock");
        let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                .unwrap();
            let mut header = Vec::new();
            while !header.ends_with(b"\r\n\r\n") {
                let mut byte = [0];
                stream.read_exact(&mut byte).unwrap();
                header.push(byte[0]);
            }
            if refuse {
                // Even transport-looking server error text is an HTTP refusal.
                let body = r#"{"error":"the server did not answer"}"#;
                write!(stream, "HTTP/1.1 403 Forbidden\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
            }
        });
        let output = bin()
            .env("ANYAGENTS_TESTS", "1")
            .env("ANYAGENTS_OPERATOR_TOKEN_FILE", &token)
            .arg("--data")
            .arg(dir.path())
            .args(["agents", "list"])
            .output()
            .unwrap();
        assert_eq!(
            output.status.success(),
            !refuse,
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            String::from_utf8_lossy(&output.stdout).contains("socket-reader"),
            !refuse
        );
        server.join().unwrap();
        std::fs::remove_file(socket).unwrap();
    }
}

#[test]
fn daemon_survives_sigpipe_from_closed_child_pipes() {
    let dir = tempfile::tempdir().unwrap();
    let mut child = bin()
        .env("ANYAGENTS_TESTS", "1")
        .arg("--data")
        .arg(dir.path())
        .arg("serve")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !dir.path().join("anyagents.sock").exists() {
        if child.try_wait().unwrap().is_some() || std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("fixture daemon never listened");
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert_eq!(unsafe { libc::kill(child.id() as i32, libc::SIGPIPE) }, 0);
    std::thread::sleep(std::time::Duration::from_millis(50));
    let status = child.try_wait().unwrap();
    let _ = child.kill();
    let _ = child.wait();
    assert!(status.is_none(), "SIGPIPE terminated daemon: {status:?}");
}

#[test]
fn closed_stdout_uses_sigpipe_without_panicking() {
    use std::os::unix::process::ExitStatusExt;
    use std::process::Stdio;
    let dir = tempfile::tempdir().unwrap();
    Store::open(dir.path().join("state.db"))
        .unwrap()
        .create_agent(anyagents_core::NewAgent::new("pipeline", "spiron"))
        .unwrap();
    let mut child = bin()
        .env("ANYAGENTS_TESTS", "1")
        .arg("--data")
        .arg(dir.path())
        .args(["agents", "list"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    drop(child.stdout.take());
    let output = child.wait_with_output().unwrap();
    assert_eq!(output.status.signal(), Some(libc::SIGPIPE));
    assert!(
        output.stderr.is_empty(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// Worktree admission runs this binary's own helper as the seat. It decides
/// with nothing on `PATH` but the Git it is handed (a host image that wraps
/// `find`, `sh` or `test` cannot change the answer) and answers through its
/// exit status: 0 admitted, 1 refused with the reason on stderr, 2 misused.
#[test]
fn internal_admission_probe_needs_no_host_tool_but_git() {
    use std::os::unix::fs::PermissionsExt;
    // A real Git binary first: the test PATH may start with an operator
    // wrapper script, which cannot run on the probe's empty PATH.
    let git = ["/usr/bin", "/usr/local/bin", "/bin"]
        .into_iter()
        .map(std::path::PathBuf::from)
        .chain(std::env::split_paths(&std::env::var_os("PATH").unwrap()))
        .map(|dir| dir.join("git"))
        .find(|path| path.is_file())
        .expect("git on the test PATH");
    let dir = tempfile::tempdir().unwrap();
    let tree = dir.path().join("tree");
    std::fs::create_dir_all(tree.join("src")).unwrap();
    std::fs::write(tree.join("src/file"), "x").unwrap();
    let init = Command::new(&git)
        .args(["init", "-q", "-b", "main"])
        .arg(&tree)
        .output()
        .unwrap();
    assert!(init.status.success());
    let probe = |args: &[&std::ffi::OsStr]| {
        bin()
            .env_clear()
            .env("PATH", "/nonexistent")
            .env("HOME", "/nonexistent")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_SYSTEM", "/dev/null")
            .args(["internal", "admission-probe"])
            .args(args)
            .output()
            .unwrap()
    };
    let full = |path: &std::path::Path| {
        probe(&[
            "--git".as_ref(),
            git.as_os_str(),
            "--".as_ref(),
            path.as_os_str(),
        ])
    };
    let admitted = full(&tree);
    assert!(
        admitted.status.success(),
        "{}",
        String::from_utf8_lossy(&admitted.stderr)
    );
    assert_eq!(probe(&["--git".as_ref()]).status.code(), Some(2));
    if unsafe { libc::geteuid() } == 0 {
        return; // root may write everything
    }
    let file = tree.join("src/file");
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o444)).unwrap();
    let refused = full(&tree);
    assert_eq!(refused.status.code(), Some(1));
    let reason = String::from_utf8_lossy(&refused.stderr);
    assert!(reason.contains(file.to_str().unwrap()), "{reason}");
}

/// `anyagents internal seat-file`, the helper a seat runs for the daemon's
/// file changes, needs no environment at all (no `PATH`, so no host tool).
/// It exits 0 done, 1 refused with the reason on stderr, 2 misused.
#[test]
fn internal_seat_file_needs_no_host_tool() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    let bytes: Vec<u8> = (0..100_000u32).map(|i| (i % 253) as u8).collect();
    std::fs::write(&source, &bytes).unwrap();
    let helper = |args: &[&str]| {
        bin()
            .env_clear()
            .current_dir(dir.path())
            .args(["internal", "seat-file"])
            .args(args)
            .stdin(std::fs::File::open(&source).unwrap())
            .output()
            .unwrap()
    };
    let copy = dir.path().join(".seat-copy-x");
    let copied = helper(&["copy", ".seat-copy-x"]);
    assert!(
        copied.status.success(),
        "{}",
        String::from_utf8_lossy(&copied.stderr)
    );
    assert_eq!(std::fs::read(&copy).unwrap(), bytes);
    assert_eq!(
        std::fs::metadata(&copy).unwrap().permissions().mode() & 0o7777,
        0o660
    );
    assert!(helper(&["readback", ".seat-copy-x"]).status.success());
    // A copy never replaces an existing name.
    assert_eq!(helper(&["copy", ".seat-copy-x"]).status.code(), Some(1));
    std::fs::write(&source, b"changed").unwrap();
    let differs = helper(&["readback", ".seat-copy-x"]);
    assert_eq!(differs.status.code(), Some(1));
    let reason = String::from_utf8_lossy(&differs.stderr);
    assert!(reason.contains("differs"), "{reason}");
    assert!(helper(&["inode", "600"]).status.success());
    assert_eq!(
        std::fs::metadata(&source).unwrap().permissions().mode() & 0o7777,
        0o600
    );
    assert!(helper(&["unlink", ".seat-copy-x"]).status.success());
    assert!(!copy.exists());
    assert!(helper(&["unlink", ".seat-copy-x"]).status.success());
    for misuse in [
        &["copy", "../escape"][..],
        &["copy"],
        &["inode", "99999"],
        &["inode", "600", "group"],
        &["chmod", "600"],
    ] {
        assert_eq!(helper(misuse).status.code(), Some(2), "{misuse:?}");
    }
}

#[test]
fn plain_log_tail_keeps_the_bounded_conversation_route() {
    use std::io::{Read, Write};
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("operator-token"), "fixture-token").unwrap();
    let listener =
        std::os::unix::net::UnixListener::bind(dir.path().join("anyagents.sock")).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        let mut header = Vec::new();
        while !header.ends_with(b"\r\n\r\n") {
            let mut byte = [0];
            stream.read_exact(&mut byte).unwrap();
            header.push(byte[0]);
        }
        let header = String::from_utf8(header).unwrap();
        let line = header.lines().next().unwrap();
        assert!(
            line.contains("/v1/agents/vulcan/conversation?limit=5"),
            "{line}"
        );
        assert!(
            !line.contains("include_tools"),
            "plain tails must not select indexed/native transcript search: {line}"
        );
        let body = r#"{"agent":"vulcan","agent_id":"fixture","turns":[]}"#;
        write!(stream,"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",body.len(),body).unwrap();
    });
    let output = bin()
        .env(
            "ANYAGENTS_OPERATOR_TOKEN_FILE",
            dir.path().join("operator-token"),
        )
        .arg("--data")
        .arg(dir.path())
        .args(["logs", "vulcan", "-n", "5", "--json"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(value["turns"].as_array().unwrap().is_empty());
    server.join().unwrap();
}

#[test]
fn create_send_inbox_and_usage() {
    let dir = tempfile::tempdir().unwrap();
    let create = bin_in(dir.path())
        .args(["agents", "create", "alpha"])
        .output()
        .unwrap();
    assert!(
        create.status.success(),
        "{}",
        String::from_utf8_lossy(&create.stderr)
    );
    let created = String::from_utf8(create.stdout).unwrap();
    assert!(created.contains("alpha"), "{created}");

    let send = bin()
        .arg("--data")
        .arg(dir.path())
        .args(["send", "alpha", "hello", "--from", "agent:alpha"])
        .output()
        .unwrap();
    assert!(
        send.status.success(),
        "{}",
        String::from_utf8_lossy(&send.stderr)
    );
    let sent = String::from_utf8(send.stdout).unwrap();
    assert!(sent.contains('\t'), "{sent}");

    let inbox = bin()
        .arg("--data")
        .arg(dir.path())
        .args(["inbox", "alpha"])
        .output()
        .unwrap();
    assert!(
        inbox.status.success(),
        "{}",
        String::from_utf8_lossy(&inbox.stderr)
    );
    let listed = String::from_utf8(inbox.stdout).unwrap();
    assert!(listed.contains("hello"), "{listed}");
    assert!(listed.contains("received"), "{listed}");

    let usage = bin()
        .arg("--data")
        .arg(dir.path())
        .arg("usage")
        .output()
        .unwrap();
    assert!(
        usage.status.success(),
        "{}",
        String::from_utf8_lossy(&usage.stderr)
    );
    assert!(usage.stdout.is_empty());

    // A named group reaches its own members, and nobody else.
    let beta = bin_in(dir.path())
        .args(["agents", "create", "beta"])
        .output()
        .unwrap();
    assert!(
        beta.status.success(),
        "{}",
        String::from_utf8_lossy(&beta.stderr)
    );
    for member in ["alpha", "beta"] {
        let joined = bin()
            .arg("--data")
            .arg(dir.path())
            .args(["groups", "join", "crew", member])
            .output()
            .unwrap();
        assert!(
            joined.status.success(),
            "{}",
            String::from_utf8_lossy(&joined.stderr)
        );
    }
    let room = bin()
        .arg("--data")
        .arg(dir.path())
        .args(["send", "group:crew", "room line", "--from", "agent:alpha"])
        .output()
        .unwrap();
    assert!(
        room.status.success(),
        "{}",
        String::from_utf8_lossy(&room.stderr)
    );
    let room_out = String::from_utf8(room.stdout).unwrap();
    // One line per recipient, each naming that agent first.
    let names: Vec<&str> = room_out
        .lines()
        .map(|line| line.split('\t').next().unwrap())
        .collect();
    assert!(names.contains(&"beta"), "{room_out}");
    // The author is not sent its own room message.
    assert!(!names.contains(&"alpha"), "{room_out}");
    let beta_inbox = bin()
        .arg("--data")
        .arg(dir.path())
        .args(["inbox", "beta"])
        .output()
        .unwrap();
    let beta_text = String::from_utf8(beta_inbox.stdout).unwrap();
    assert!(beta_text.contains("room line"), "{beta_text}");
    assert!(beta_text.contains("received"), "{beta_text}");
    let alpha_inbox = bin()
        .arg("--data")
        .arg(dir.path())
        .args(["inbox", "alpha"])
        .output()
        .unwrap();
    let alpha_text = String::from_utf8(alpha_inbox.stdout).unwrap();
    assert!(!alpha_text.contains("room line"), "{alpha_text}");

    // A name the server does not know is not a room: it reaches nobody, and the
    // send says so rather than fanning out to every agent.
    // A fresh data directory has no agents at all, so the room is empty.
    let lonely = bin()
        .arg("--data")
        .arg(dir.path().join("empty"))
        .args(["send", "group:crew", "hello", "--from", "agent:alpha"])
        .output()
        .unwrap();
    assert!(!lonely.status.success());
    let stderr = String::from_utf8(lonely.stderr).unwrap();
    assert!(stderr.contains("no agent"), "{stderr}");
}

#[test]
fn create_registers_mcp_only_in_the_agent_config() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path().join("home");
    std::fs::create_dir_all(home.join(".codex")).unwrap();
    let claude_sentinel = home.join(".claude.json");
    let codex_sentinel = home.join(".codex").join("config.toml");
    std::fs::write(&claude_sentinel, b"sentinel-claude").unwrap();
    std::fs::write(&codex_sentinel, b"sentinel-codex").unwrap();
    let custom = dir.path().join("custom-work");

    let created = bin_in(dir.path())
        .env("HOME", &home)
        .args([
            "agents",
            "create",
            "alpha",
            "--harness",
            "claude-code",
            "--model",
            "sonnet",
            "--instructions",
            "be brief",
            "--workdir",
        ])
        .arg(&custom)
        .output()
        .unwrap();
    assert!(
        created.status.success(),
        "{}",
        String::from_utf8_lossy(&created.stderr)
    );
    let id = String::from_utf8(created.stdout)
        .unwrap()
        .lines()
        .next()
        .unwrap()
        .split('\t')
        .next()
        .unwrap()
        .to_string();

    let store = Store::open(dir.path().join("state.db")).unwrap();
    let agent = store.agent_by_name("alpha").unwrap();
    assert_eq!(agent.state, "running");
    assert_eq!(agent.instructions.as_deref(), Some("be brief"));
    assert_eq!(agent.model.as_deref(), Some("sonnet"));
    assert_eq!(
        agent.workdir.as_deref(),
        Some(std::path::absolute(&custom).unwrap().to_str().unwrap())
    );
    assert!(custom.is_dir());
    assert!(!dir.path().join("agents").join(&id).join("work").exists());
    assert!(!custom.join(".mcp.json").exists());
    assert!(!dir.path().join(".claude.json").exists());
    assert!(!dir.path().join("config.toml").exists());
    assert_eq!(std::fs::read(&claude_sentinel).unwrap(), b"sentinel-claude");
    assert_eq!(std::fs::read(&codex_sentinel).unwrap(), b"sentinel-codex");

    let claude_cfg = std::fs::read_to_string(
        dir.path()
            .join("agents")
            .join(&id)
            .join("config")
            .join(".claude.json"),
    )
    .unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&claude_cfg).unwrap();
    let server = &parsed["mcpServers"]["anyagents"];
    assert_eq!(server["type"], "stdio");
    assert_eq!(server["env"], serde_json::json!({}));
    let args = server["args"].as_array().unwrap();
    assert!(args.iter().any(|arg| arg == "mcp"));
    // The server is pointed at the agent's own config directory, which holds
    // its capability; no name on the command line is an identity.
    assert!(args.iter().any(|arg| arg == "--config"), "{args:?}");
    assert!(!args.iter().any(|arg| arg == "--agent"), "{args:?}");

    let codex = bin_in(dir.path())
        .env("HOME", &home)
        .args(["agents", "create", "beta", "--harness", "codex"])
        .output()
        .unwrap();
    assert!(
        codex.status.success(),
        "{}",
        String::from_utf8_lossy(&codex.stderr)
    );
    let codex_id = String::from_utf8(codex.stdout)
        .unwrap()
        .lines()
        .next()
        .unwrap()
        .split('\t')
        .next()
        .unwrap()
        .to_string();
    assert!(
        dir.path()
            .join("agents")
            .join(&codex_id)
            .join("work")
            .is_dir()
    );
    let toml = std::fs::read_to_string(
        dir.path()
            .join("agents")
            .join(&codex_id)
            .join("config")
            .join("config.toml"),
    )
    .unwrap();
    assert!(toml.contains("[mcp_servers.anyagents]"), "{toml}");
    assert_eq!(std::fs::read(&claude_sentinel).unwrap(), b"sentinel-claude");
    assert_eq!(std::fs::read(&codex_sentinel).unwrap(), b"sentinel-codex");
}

#[test]
fn usage_sums_by_agent_and_day() {
    let dir = tempfile::tempdir().unwrap();
    for name in ["alpha", "beta"] {
        let created = bin_in(dir.path())
            .args(["agents", "create", name])
            .output()
            .unwrap();
        assert!(created.status.success());
    }
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let alpha = store.agent_by_name("alpha").unwrap();
    let beta = store.agent_by_name("beta").unwrap();
    record(&store, &alpha.id, "2023-11-14", 5, Some(10));
    record(&store, &alpha.id, "2023-11-14", 7, Some(3));
    record(&store, &alpha.id, "2023-11-15", 2, None);
    record(&store, &beta.id, "2023-11-14", 4, Some(1));

    let summed = usage(dir.path(), &["usage"]);
    assert!(summed.contains("alpha\t-\t14\t0\t0\t0\t13\n"), "{summed}");
    assert!(summed.contains("beta\t-\t4\t0\t0\t0\t1\n"), "{summed}");

    let by_day = usage(dir.path(), &["usage", "--by", "day"]);
    assert!(
        by_day.contains("alpha\t2023-11-14\t12\t0\t0\t0\t13\n"),
        "{by_day}"
    );
    assert!(
        by_day.contains("alpha\t2023-11-15\t2\t0\t0\t0\t-\n"),
        "{by_day}"
    );
    assert!(
        by_day.contains("beta\t2023-11-14\t4\t0\t0\t0\t1\n"),
        "{by_day}"
    );

    let only = usage(dir.path(), &["usage", "--agent", "alpha"]);
    assert!(only.contains("alpha\t-\t14\t0\t0\t0\t13\n"), "{only}");
    assert!(!only.contains("beta"), "{only}");

    let rejected = bin()
        .arg("--data")
        .arg(dir.path())
        .args(["usage", "--by", "week"])
        .output()
        .unwrap();
    assert!(!rejected.status.success());
}

fn record(
    store: &Store,
    agent: &anyagents_core::AgentId,
    day: &str,
    input: i64,
    cost: Option<i64>,
) {
    store
        .record_usage(NewUsage {
            agent_id: agent.clone(),
            harness: "claude-code".to_string(),
            model: None,
            input_tokens: input,
            output_tokens: 0,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
            reported_cost_micros: cost,
            estimated_cost_micros: None,
            billing_mode: BillingMode::Subscription,
            quota_state: None,
            day: day.to_string(),
        })
        .unwrap();
}

fn usage(data: &std::path::Path, args: &[&str]) -> String {
    let output = bin().arg("--data").arg(data).args(args).output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

#[test]
fn add_telegram_copies_the_token_and_lists_no_secret() {
    use anyagents_core::CredentialStore;
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let created = bin_in(dir.path())
        .args(["agents", "create", "alpha"])
        .output()
        .unwrap();
    assert!(
        created.status.success(),
        "{}",
        String::from_utf8_lossy(&created.stderr)
    );
    let token_file = dir.path().join("token");
    std::fs::write(&token_file, b"123456:ABC\n").unwrap();
    // Token custody is tested through the real CLI, but identity lookup is
    // always local, independent of test-runner environment heuristics.
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let api = format!("http://{}", listener.local_addr().unwrap());
    let provider = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            let mut byte = [0];
            socket.read_exact(&mut byte).unwrap();
            request.push(byte[0]);
        }
        assert!(
            String::from_utf8_lossy(&request).starts_with("POST /bot123456:ABC/getMe HTTP/1.1\r\n")
        );
        let header = String::from_utf8(request).unwrap();
        let length: usize = header
            .lines()
            .find_map(|line| {
                let (key, value) = line.split_once(':')?;
                key.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse().unwrap())
            })
            .unwrap();
        let mut body = vec![0; length];
        socket.read_exact(&mut body).unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
            serde_json::json!({})
        );
        let body =
            r#"{"ok":true,"result":{"id":123456,"is_bot":true,"username":"alpha_fixture_bot"}}"#;
        write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
    });
    let added = bin()
        .env("ANYAGENTS_TELEGRAM_API", api)
        .env("ANYAGENTS_FAKE_MODE", "1")
        .env_remove("ANYAGENTS_TELEGRAM_WEBHOOK_URL")
        .arg("--data")
        .arg(dir.path())
        .args([
            "channels",
            "add",
            "telegram",
            "--agent",
            "alpha",
            "--token-file",
        ])
        .arg(&token_file)
        .output()
        .unwrap();
    assert!(
        added.status.success(),
        "{}",
        String::from_utf8_lossy(&added.stderr)
    );
    provider.join().unwrap();
    let line = String::from_utf8(added.stdout).unwrap();
    assert!(!line.contains("123456"), "{line}");
    let id = line.split('\t').next().unwrap().trim();
    let secrets = anyagents_core::FileCredentialStore::new(dir.path());
    let secret = secrets.read(&format!("channels/{id}")).unwrap();
    assert_eq!(secret.as_str().unwrap(), "123456:ABC");
    let path = dir.path().join("secrets").join("channels").join(id);
    let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o600);
    let listed = bin()
        .arg("--data")
        .arg(dir.path())
        .args(["channels", "list"])
        .output()
        .unwrap();
    assert!(listed.status.success());
    let stdout = String::from_utf8(listed.stdout).unwrap();
    assert!(stdout.contains("telegram"), "{stdout}");
    assert!(stdout.contains("alpha"), "{stdout}");
    assert!(!stdout.contains("123456"), "{stdout}");
}
