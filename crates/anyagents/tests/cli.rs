use std::process::Command;

use anyagents_core::{BillingMode, NewUsage, Store};

/// A `PATH` where `claude` is this binary and `npm` fails, so creating an agent
/// records the vendor tool the machine already has and downloads nothing.
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
        // A create installs its harness; these tests run none of it. `view`
        // answers a version and an install does nothing.
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
    Command::new(env!("CARGO_BIN_EXE_anyagents"))
}

/// A command in a fresh data directory whose harness needs no download.
fn bin_in(dir: &std::path::Path) -> Command {
    let mut command = bin();
    command
        .env("PATH", machine_path(dir))
        .env("ANYAGENTS_CLAUDE_BIN", env!("CARGO_BIN_EXE_anyagents"))
        .arg("--data")
        .arg(dir);
    command
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
    assert!(dir.path().join("agents").join(&id).join("work").is_dir());
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
    let toml = std::fs::read_to_string(
        dir.path()
            .join("agents")
            .join(codex_id)
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
    let added = bin()
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
