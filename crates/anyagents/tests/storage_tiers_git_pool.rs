//! End-to-end verification of multi-tier storage isolation:
//! - Tier 1 (`ANYAGENTS_STATE_DIR`): control-plane SQLite, capabilities, agent configs
//! - Tier 2 (`ANYAGENTS_WORKSPACES_DIR` + `ANYAGENTS_GIT_POOL_DIR`): linked worktrees
//!   backed by a shared bare Git object pool (`--reference-if-able` / `worktree add`)
//! - Tier 3 (`ANYAGENTS_SCRATCH_DIR`): per-agent compile & temp trees
//!   (`<scratch>/builds/<id>/{cargo,gradle,derived-data}`, `<scratch>/tmp/<id>`)
//!   reaped synchronously on `archive`.

mod support;

use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::json;

fn git(dir: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .expect("run git");
    assert!(
        out.status.success(),
        "git {args:?} in {}: {}",
        dir.display(),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn json_body(raw_http: &str) -> serde_json::Value {
    let (_, body) = raw_http.split_once("\r\n\r\n").expect("http body");
    serde_json::from_str(body).unwrap_or_else(|e| panic!("invalid json ({e}): {raw_http}"))
}

#[test]
fn three_tier_storage_bare_git_pool_and_archive_scratch_reap_end_to_end() {
    let _guard = support::lock();
    support::clear();

    let fixture = tempfile::tempdir().unwrap();
    let tier1_state = fixture.path().join("tier1-state");
    let tier2_workspaces = fixture.path().join("tier2-workspaces");
    let tier2_pool = fixture.path().join("tier2-repos");
    let tier3_scratch = fixture.path().join("tier3-scratch");
    let remotes_root = fixture.path().join("remotes");
    let home_dir = fixture.path().join("home");

    for dir in [
        &tier1_state,
        &tier2_workspaces,
        &tier2_pool,
        &tier3_scratch,
        &remotes_root,
        &home_dir,
    ] {
        std::fs::create_dir_all(dir).unwrap();
    }

    // Seed an upstream bare Git repository at remotes/ExampleOrg/demo.git
    let seed = fixture.path().join("seed-src");
    std::fs::create_dir_all(seed.join("src")).unwrap();
    git(&seed, &["init", "-b", "main"]);
    git(&seed, &["config", "user.name", "Test Author"]);
    git(&seed, &["config", "user.email", "author@example.test"]);
    std::fs::write(seed.join("README.md"), "# Tiered Workspace Demo\n").unwrap();
    std::fs::write(seed.join("src/lib.rs"), "pub fn answer() -> u32 { 42 }\n").unwrap();
    git(&seed, &["add", "."]);
    git(&seed, &["commit", "-m", "Initial commit"]);
    let upstream_bare = remotes_root.join("ExampleOrg").join("demo.git");
    std::fs::create_dir_all(upstream_bare.parent().unwrap()).unwrap();
    git(
        &seed,
        &[
            "clone",
            "--bare",
            "--no-hardlinks",
            ".",
            upstream_bare.to_str().unwrap(),
        ],
    );

    // Operator & capability credentials in Tier 1 state dir
    let token_path = tier1_state.join("operator-token");
    std::fs::write(&token_path, format!("{}\n", support::TOKEN)).unwrap();
    let cap_path = tier1_state.join("mcp-capability");
    std::fs::write(&cap_path, format!("{}\n", support::CAPABILITY_KEY)).unwrap();

    // Fake harness wrapper that records injected build/tmp/cache environment into
    // CARGO_TARGET_DIR/env-receipt.json before delegating to scripted_opencode.
    let bin = env!("CARGO_BIN_EXE_anyagents");
    let scripted_opencode = Path::new(bin)
        .parent()
        .expect("test binary parent")
        .join("scripted_opencode");
    assert!(
        scripted_opencode.is_file(),
        "missing scripted_opencode beside test binary at {}",
        scripted_opencode.display()
    );
    let scripted_bin = scripted_opencode.display();
    let wrapper = fixture.path().join("fake-opencode-wrapper.sh");
    let wrapper_body = format!(
        r#"#!/bin/sh
set -eu
if [ -n "${{CARGO_TARGET_DIR:-}}" ]; then
  mkdir -p "$CARGO_TARGET_DIR/debug/deps"
  printf 'compiled-artifact' > "$CARGO_TARGET_DIR/debug/deps/libdemo.rlib"
  python3 -c 'import json, os, sys
keys = ["CARGO_TARGET_DIR", "GRADLE_USER_HOME", "DERIVED_DATA_PATH", "TMPDIR", "BUN_TMPDIR", "SCCACHE_DIR", "SCCACHE_CACHE_SIZE", "ANYAGENTS_STATE_DIR", "ANYAGENTS_WORKSPACES_DIR", "ANYAGENTS_SCRATCH_DIR", "ANYAGENTS_GIT_POOL_DIR"]
json.dump({{k: os.environ.get(k) for k in keys}}, open(sys.argv[1], "w"))
' "$CARGO_TARGET_DIR/env-receipt.json"
fi
if [ -n "${{TMPDIR:-}}" ]; then
  mkdir -p "$TMPDIR"
  printf 'tmp-artifact' > "$TMPDIR/rustc-scratch.tmp"
fi
exec "{scripted_bin}" "$@"
"#
    );
    assert!(
        Command::new("/bin/sh")
            .args(["-c", "printf '%s' \"$2\" > \"$1\"", "write-wrapper"])
            .arg(&wrapper)
            .arg(&wrapper_body)
            .status()
            .unwrap()
            .success()
    );
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();

    let sccache_dir = tier3_scratch.join("cache").join("sccache");
    std::fs::create_dir_all(&sccache_dir).unwrap();
    let remote_base = format!("file://{}", remotes_root.display());

    support::managed_harnesses(&tier1_state);

    // Create root parent agent using ANYAGENTS_STATE_DIR (without passing --data!)
    let create_parent = Command::new(bin)
        .env("HOME", &home_dir)
        .env("ANYAGENTS_HOST_HOME", &home_dir)
        .env("ANYAGENTS_OPERATOR_TOKEN_FILE", &token_path)
        .env("ANYAGENTS_CAPABILITY_KEY", support::CAPABILITY_KEY)
        .env("ANYAGENTS_TESTS", "contract-double")
        .env("ANYAGENTS_TEST_STORAGE_TIERS", "1")
        .env("ANYAGENTS_STATE_DIR", &tier1_state)
        .env("ANYAGENTS_WORKSPACES_DIR", &tier2_workspaces)
        .env("ANYAGENTS_SCRATCH_DIR", &tier3_scratch)
        .env("ANYAGENTS_GIT_POOL_DIR", &tier2_pool)
        .env("ANYAGENTS_GIT_REMOTE_BASE", &remote_base)
        .env_remove("ANYAGENTS_DATA")
        .env_remove("ANYAGENTS_WEBHOOK_LISTEN")
        .env_remove("ANYAGENTS_CHECKOUT_PROVIDER_BIN")
        .env_remove("ANYAGENTS_AGENTS")
        .env_remove("ANYAGENTS_AGENT_UID")
        .env_remove("ANYAGENTS_AGENT_GID")
        .env_remove("ANYAGENTS_SOCKET_GID")
        .env("ANYAGENTS_OPENCODE_BIN", &wrapper)
        .env("ANYAGENTS_FAKE_OPENCODE", "1")
        .args([
            "agents",
            "create",
            "coordinator",
            "--harness",
            "opencode",
            "--model",
            "opencode/big-pickle",
        ])
        .output()
        .expect("create coordinator");
    assert!(
        create_parent.status.success(),
        "create coordinator failed: {}",
        String::from_utf8_lossy(&create_parent.stderr)
    );
    let parent_id = String::from_utf8_lossy(&create_parent.stdout)
        .split_whitespace()
        .next()
        .unwrap()
        .to_string();

    // Start server relying on ANYAGENTS_STATE_DIR + tier env vars
    let server = Command::new(bin)
        .env("HOME", &home_dir)
        .env("ANYAGENTS_HOST_HOME", &home_dir)
        .env("ANYAGENTS_OPERATOR_TOKEN_FILE", &token_path)
        .env("ANYAGENTS_CAPABILITY_KEY", support::CAPABILITY_KEY)
        .env("ANYAGENTS_TESTS", "contract-double")
        .env("ANYAGENTS_TEST_STORAGE_TIERS", "1")
        .env("ANYAGENTS_STATE_DIR", &tier1_state)
        .env("ANYAGENTS_WORKSPACES_DIR", &tier2_workspaces)
        .env("ANYAGENTS_SCRATCH_DIR", &tier3_scratch)
        .env("ANYAGENTS_GIT_POOL_DIR", &tier2_pool)
        .env("ANYAGENTS_GIT_REMOTE_BASE", &remote_base)
        .env("SCCACHE_DIR", &sccache_dir)
        .env("SCCACHE_CACHE_SIZE", "120G")
        .env_remove("ANYAGENTS_DATA")
        .env_remove("ANYAGENTS_WEBHOOK_LISTEN")
        .env_remove("ANYAGENTS_CHECKOUT_PROVIDER_BIN")
        .env_remove("ANYAGENTS_AGENTS")
        .env_remove("ANYAGENTS_AGENT_UID")
        .env_remove("ANYAGENTS_AGENT_GID")
        .env_remove("ANYAGENTS_SOCKET_GID")
        .env_remove("ANYAGENTS_LAUNCHER")
        .env("ANYAGENTS_SETPRIV", tier1_state.join("nonexistent-setpriv"))
        .env("ANYAGENTS_OPENCODE_BIN", &wrapper)
        .env("ANYAGENTS_FAKE_OPENCODE", "1")
        .env("ANYAGENTS_FAKE_LOG", tier3_scratch.join("fake.log"))
        .arg("serve")
        .stdout(Stdio::null())
        .stderr(std::fs::File::create(tier1_state.join("server.err")).unwrap())
        .spawn()
        .expect("spawn tiered server");

    struct KillOnDrop(std::process::Child);
    impl Drop for KillOnDrop {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let _server_guard = KillOnDrop(server);

    let socket = tier1_state.join("anyagents.sock");
    let deadline = Instant::now() + Duration::from_secs(10);
    while !socket.exists() {
        assert!(
            Instant::now() < deadline,
            "server did not bind socket: {}",
            std::fs::read_to_string(tier1_state.join("server.err")).unwrap_or_default()
        );
        std::thread::sleep(Duration::from_millis(25));
    }

    let parent_cap = std::fs::read_to_string(
        tier1_state
            .join("agents")
            .join(&parent_id)
            .join("config")
            .join("mcp-capability"),
    )
    .expect("parent capability")
    .trim()
    .to_string();
    let parent_auth = format!("x-anyagents-agent: {parent_cap}\r\n");

    // Negative test: path traversal in repo slug must fail closed
    let bad_resp = support::call_raw(
        &socket,
        "POST",
        "/v1/agents",
        &json!({
            "name": "worker-bad",
            "harness": "opencode",
            "model": "opencode/big-pickle",
            "repo": "../escape/repo",
            "branch": "feat/bad"
        })
        .to_string(),
        &parent_auth,
    );
    assert!(
        !bad_resp.starts_with("HTTP/1.1 200") && !bad_resp.starts_with("HTTP/1.1 201"),
        "traversal repo slug unexpectedly succeeded: {bad_resp}"
    );
    assert!(!fixture.path().join("escape").exists());

    // Positive test 1: spawn worker-clean backed by Bare Git Pool + try spoofing ANYAGENTS_SCRATCH_DIR
    let spawn1_raw = support::call_raw(
        &socket,
        "POST",
        "/v1/agents",
        &json!({
            "name": "worker-clean",
            "harness": "opencode",
            "model": "opencode/big-pickle",
            "repo": "ExampleOrg/demo",
            "branch": "feat/clean-slice",
            "message": "build clean slice",
            "env": {
                "ANYAGENTS_SCRATCH_DIR": "/evil/spoofed-scratch",
                "ANYAGENTS_WORKSPACES_DIR": "/evil/spoofed-workspaces"
            }
        })
        .to_string(),
        &parent_auth,
    );
    assert!(
        spawn1_raw.starts_with("HTTP/1.1 200") || spawn1_raw.starts_with("HTTP/1.1 201"),
        "spawn worker-clean failed: {spawn1_raw}\nserver.err: {}",
        std::fs::read_to_string(tier1_state.join("server.err")).unwrap_or_default()
    );
    let worker1_id = json_body(&spawn1_raw)["id"]
        .as_str()
        .expect("worker1 id")
        .to_string();

    // Verify Tier 1 has config ONLY and zero `work` directory
    assert!(
        tier1_state
            .join("agents")
            .join(&worker1_id)
            .join("config")
            .is_dir()
    );
    assert!(
        !tier1_state
            .join("agents")
            .join(&worker1_id)
            .join("work")
            .exists(),
        "Tier 1 state dir must not contain worktree checkouts"
    );

    // Verify Tier 2 Bare Pool & linked worktree
    let pool_bare = tier2_pool.join("ExampleOrg").join("demo.git");
    assert!(pool_bare.join("HEAD").is_file(), "bare pool must exist");
    let wt1 = tier2_workspaces.join(&worker1_id);
    assert!(
        wt1.join("README.md").is_file(),
        "worktree must be checked out in Tier 2"
    );
    assert_eq!(
        git(
            &wt1,
            &["config", "--bool", "--get", "extensions.worktreeConfig"]
        ),
        "true"
    );
    // Pool checkouts are per-seat repositories, never linked worktrees:
    // private == common == the checkout's own .git directory, reading
    // pool objects through alternates.
    let private1 = std::fs::canonicalize(wt1.join(".git")).unwrap();
    let common1 = std::fs::canonicalize(git(
        &wt1,
        &["rev-parse", "--path-format=absolute", "--git-common-dir"],
    ))
    .unwrap();
    assert_eq!(private1, common1);
    assert!(wt1.join(".git").is_dir());
    let alternates = std::fs::read_to_string(wt1.join(".git/objects/info/alternates")).unwrap();
    assert!(
        alternates.contains(&format!("{}/objects\n", pool_bare.display())),
        "alternates: {alternates}"
    );

    // Wait for fake wrapper to run and write env-receipt.json in Tier 3 scratch
    let receipt_path = tier3_scratch
        .join("builds")
        .join(&worker1_id)
        .join("cargo")
        .join("env-receipt.json");
    let wait_receipt = Instant::now() + Duration::from_secs(10);
    while !receipt_path.is_file() {
        assert!(
            Instant::now() < wait_receipt,
            "worker-clean did not write scratch env receipt at {}: {}",
            receipt_path.display(),
            std::fs::read_to_string(tier1_state.join("server.err")).unwrap_or_default()
        );
        std::thread::sleep(Duration::from_millis(25));
    }
    let receipt: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&receipt_path).unwrap()).unwrap();
    assert_eq!(
        receipt["CARGO_TARGET_DIR"].as_str().unwrap(),
        tier3_scratch
            .join("builds")
            .join(&worker1_id)
            .join("cargo")
            .display()
            .to_string()
    );
    assert_eq!(
        receipt["TMPDIR"].as_str().unwrap(),
        tier3_scratch
            .join("tmp")
            .join(&worker1_id)
            .display()
            .to_string()
    );
    assert_eq!(
        receipt["SCCACHE_DIR"].as_str().unwrap(),
        sccache_dir.display().to_string()
    );
    assert_eq!(receipt["SCCACHE_CACHE_SIZE"].as_str().unwrap(), "120G");
    // Spoofed launch env must never override daemon storage tiers
    assert_eq!(
        receipt["ANYAGENTS_SCRATCH_DIR"].as_str().unwrap(),
        tier3_scratch.display().to_string()
    );

    // Positive test 2: warm Bare Pool second checkout + dirty source preservation vs scratch wipe
    let t0 = Instant::now();
    let spawn2_raw = support::call_raw(
        &socket,
        "POST",
        "/v1/agents",
        &json!({
            "name": "worker-dirty",
            "harness": "opencode",
            "model": "opencode/big-pickle",
            "repo": "ExampleOrg/demo",
            "branch": "feat/dirty-slice",
            "message": "build dirty slice"
        })
        .to_string(),
        &parent_auth,
    );
    let warm_ms = t0.elapsed().as_millis();
    assert!(
        spawn2_raw.starts_with("HTTP/1.1 200") || spawn2_raw.starts_with("HTTP/1.1 201"),
        "spawn worker-dirty failed: {spawn2_raw}"
    );
    assert!(
        warm_ms < 5_000,
        "warm bare-pool spawn took unexpectedly long: {warm_ms}ms"
    );
    let worker2_id = json_body(&spawn2_raw)["id"]
        .as_str()
        .expect("worker2 id")
        .to_string();
    let wt2 = tier2_workspaces.join(&worker2_id);
    let receipt2 = tier3_scratch
        .join("builds")
        .join(&worker2_id)
        .join("cargo")
        .join("env-receipt.json");
    let wait_r2 = Instant::now() + Duration::from_secs(10);
    while !receipt2.is_file() {
        assert!(Instant::now() < wait_r2, "worker-dirty did not run");
        std::thread::sleep(Duration::from_millis(25));
    }
    // Wait briefly for fake turn to finish so archive sees an idle process
    std::thread::sleep(Duration::from_millis(250));

    // Dirty uncommitted source change in worker-dirty's Tier 2 worktree
    std::fs::write(wt2.join("README.md"), "# Unpushed work in progress\n").unwrap();

    // Archive worker-clean -> Tier 2 clean worktree AND Tier 3 builds/tmp MUST be reaped immediately
    let arch1 = support::call_raw(
        &socket,
        "POST",
        "/v1/agents/worker-clean/archive",
        "{}",
        &parent_auth,
    );
    assert!(
        arch1.starts_with("HTTP/1.1 200"),
        "archive worker-clean failed: {arch1}"
    );
    assert!(
        !tier3_scratch.join("builds").join(&worker1_id).exists(),
        "archive must immediately reap <scratch>/builds/<id>"
    );
    assert!(
        !tier3_scratch.join("tmp").join(&worker1_id).exists(),
        "archive must immediately reap <scratch>/tmp/<id>"
    );
    assert!(
        !wt1.exists(),
        "archive must reap clean Tier 2 worktree <workspaces>/<id>"
    );
    assert!(
        tier1_state.join("state.db").is_file() && pool_bare.join("HEAD").is_file(),
        "Tier 1 state and Tier 2 bare pool must remain completely intact"
    );

    // Archive worker-dirty -> Tier 3 builds/tmp MUST be wiped immediately while dirty Tier 2 source stays safe!
    let arch2 = support::call_raw(
        &socket,
        "POST",
        "/v1/agents/worker-dirty/archive",
        "{}",
        &parent_auth,
    );
    assert!(
        arch2.starts_with("HTTP/1.1 200"),
        "archive worker-dirty failed: {arch2}"
    );
    assert!(
        !tier3_scratch.join("builds").join(&worker2_id).exists(),
        "archive must immediately reap <scratch>/builds/<id> even when source worktree is dirty"
    );
    assert!(
        !tier3_scratch.join("tmp").join(&worker2_id).exists(),
        "archive must immediately reap <scratch>/tmp/<id> even when source worktree is dirty"
    );
    assert_eq!(
        std::fs::read_to_string(wt2.join("README.md")).unwrap(),
        "# Unpushed work in progress\n",
        "dirty uncommitted source in Tier 2 worktree must never be lost"
    );
}
