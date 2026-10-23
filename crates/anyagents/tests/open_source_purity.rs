//! Verifies ADR 0002 invariants:
//! 1. Single-UID Local Mode (`!seats_configured()`) runs end-to-end with
//!    capability-token authentication (`mcp-capability`) and without requiring
//!    `/usr/local/bin/anyagents-setpriv` or multi-UID OS seats.
//! 2. Open-source purity: the repository contains no hardcoded developer paths,
//!    internal environment variables, or deployment-specific IDs.

mod support;

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::json;

fn collect_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if name_str == "target" || name_str.starts_with('.') {
            continue;
        }
        if path.is_dir() {
            collect_files(&path, out);
        } else if path.is_file() {
            let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
            if matches!(ext, "rs" | "md" | "toml" | "yml" | "yaml" | "sh") {
                out.push(path);
            }
        }
    }
}

#[test]
fn repository_has_no_hardcoded_host_paths_or_internal_identifiers() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repo_root = manifest_dir
        .parent()
        .and_then(Path::parent)
        .expect("workspace root");

    let forbidden: &[(&str, &str)] = &[
        ("hardcoded user home", concat!("/home/", "kyle")),
        (
            "hardcoded scratch path",
            concat!("/scratch/", "coordinator"),
        ),
        ("internal env prefix", concat!("DESK", "_")),
        ("internal config path", concat!(".config/", "desk/")),
        ("internal telegram user id", concat!("16807", "7807")),
        ("internal telegram group id", concat!("-100354", "4170260")),
        ("internal domain", concat!("desk.", "kylet.se")),
        ("hardcoded seat name", concat!("seat-", "argon")),
        ("hardcoded seat name", concat!("seat-", "calliope")),
    ];

    let mut files = Vec::new();
    collect_files(&repo_root.join("crates"), &mut files);
    collect_files(&repo_root.join("tests"), &mut files);
    collect_files(&repo_root.join("docs"), &mut files);
    files.push(repo_root.join("README.md"));

    let mut violations = Vec::new();
    for file in files {
        let Ok(content) = std::fs::read_to_string(&file) else {
            continue;
        };
        for (line_no, line) in content.lines().enumerate() {
            for (reason, needle) in forbidden {
                if line.contains(needle) {
                    violations.push(format!(
                        "{}:{} ({reason}: {needle})",
                        file.strip_prefix(repo_root).unwrap_or(&file).display(),
                        line_no + 1
                    ));
                }
            }
        }
    }

    assert!(
        violations.is_empty(),
        "open-source purity check failed:\n{}",
        violations.join("\n")
    );
}

#[test]
fn single_uid_local_mode_starts_and_routes_without_setpriv_or_seat_uids() {
    let _guard = support::lock();
    support::clear();

    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();

    // Write operator token & capability key, but configure NO seats (`ANYAGENTS_AGENTS`,
    // `ANYAGENTS_AGENT_UID`, `ANYAGENTS_AGENT_GID`, `ANYAGENTS_SOCKET_GID` all unset)
    // and point `ANYAGENTS_SETPRIV` at a non-existent binary.
    let token_path = data.join("operator-token");
    std::fs::write(&token_path, format!("{}\n", support::TOKEN)).unwrap();
    let cap_path = data.join("mcp-capability");
    std::fs::write(&cap_path, format!("{}\n", support::CAPABILITY_KEY)).unwrap();

    let bin = env!("CARGO_BIN_EXE_anyagents");
    let create_agent = |name: &str| -> String {
        let out = Command::new(bin)
            .env("ANYAGENTS_OPERATOR_TOKEN_FILE", &token_path)
            .env("ANYAGENTS_CAPABILITY_KEY", support::CAPABILITY_KEY)
            .env_remove("ANYAGENTS_AGENTS")
            .env_remove("ANYAGENTS_AGENT_UID")
            .env_remove("ANYAGENTS_AGENT_GID")
            .env_remove("ANYAGENTS_SOCKET_GID")
            .env("ANYAGENTS_OPENCODE_BIN", bin)
            .env("ANYAGENTS_FAKE_OPENCODE", "1")
            .arg("--data")
            .arg(&data)
            .args([
                "agents",
                "create",
                name,
                "--harness",
                "opencode",
                "--model",
                "opencode/big-pickle",
            ])
            .output()
            .expect("create agent");
        assert!(
            out.status.success(),
            "create {name} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout)
            .split_whitespace()
            .next()
            .unwrap()
            .to_string()
    };

    let alpha_id = create_agent("alpha");
    let beta_id = create_agent("beta");

    let mut server = Command::new(bin)
        .env("ANYAGENTS_OPERATOR_TOKEN_FILE", &token_path)
        .env("ANYAGENTS_CAPABILITY_KEY", support::CAPABILITY_KEY)
        .env_remove("ANYAGENTS_AGENTS")
        .env_remove("ANYAGENTS_AGENT_UID")
        .env_remove("ANYAGENTS_AGENT_GID")
        .env_remove("ANYAGENTS_SOCKET_GID")
        .env_remove("ANYAGENTS_LAUNCHER")
        .env("ANYAGENTS_SETPRIV", data.join("nonexistent-setpriv"))
        .env("ANYAGENTS_OPENCODE_BIN", bin)
        .env("ANYAGENTS_FAKE_OPENCODE", "1")
        .env("ANYAGENTS_FAKE_LOG", data.join("fake.log"))
        .arg("--data")
        .arg(&data)
        .arg("serve")
        .stdout(Stdio::null())
        .stderr(std::fs::File::create(data.join("server.err")).unwrap())
        .spawn()
        .expect("spawn server in single-UID local mode");

    let socket = data.join("anyagents.sock");
    let deadline = Instant::now() + Duration::from_secs(10);
    while !socket.exists() {
        assert!(
            Instant::now() < deadline,
            "single-UID local server did not bind socket: {}",
            std::fs::read_to_string(data.join("server.err")).unwrap_or_default()
        );
        std::thread::sleep(Duration::from_millis(25));
    }

    // Read each agent's minted `mcp-capability` token from its isolated config dir.
    let alpha_cap = std::fs::read_to_string(
        data.join("agents")
            .join(&alpha_id)
            .join("config")
            .join("mcp-capability"),
    )
    .expect("alpha mcp-capability")
    .trim()
    .to_string();
    let beta_cap = std::fs::read_to_string(
        data.join("agents")
            .join(&beta_id)
            .join("config")
            .join("mcp-capability"),
    )
    .expect("beta mcp-capability")
    .trim()
    .to_string();

    let alpha_auth = format!("x-anyagents-agent: {alpha_cap}\r\n");
    let beta_auth = format!("x-anyagents-agent: {beta_cap}\r\n");

    // 1. Verify `/v1/whoami` authenticates `alpha` by capability token in single-UID mode.
    let whoami_raw = support::call_raw(&socket, "GET", "/v1/whoami", "", &alpha_auth);
    assert!(
        whoami_raw.starts_with("HTTP/1.1 200"),
        "whoami failed: {whoami_raw}"
    );
    assert!(whoami_raw.contains("\"alpha\""), "{whoami_raw}");

    // 2. Send a message from `alpha` to `beta` using `alpha`'s capability token (`x-anyagents-agent`).
    let send_raw = support::call_raw(
        &socket,
        "POST",
        "/v1/send",
        &json!({
            "to": ["agent:beta"],
            "text": "ping from single-uid local mode"
        })
        .to_string(),
        &alpha_auth,
    );
    assert!(
        send_raw.starts_with("HTTP/1.1 200"),
        "send with capability token failed: {send_raw}"
    );

    // 3. Verify `beta` reads the message from its own inbox using `beta`'s capability token.
    let inbox_raw = support::call_raw(&socket, "GET", "/v1/agents/beta/inbox", "", &beta_auth);
    let _ = server.kill();
    let _ = server.wait();
    assert!(
        inbox_raw.starts_with("HTTP/1.1 200"),
        "beta inbox failed: {inbox_raw}"
    );
    assert!(
        inbox_raw.contains("ping from single-uid local mode"),
        "beta inbox missing message: {inbox_raw}"
    );
}
