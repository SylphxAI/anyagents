//! The CLI sign-in against a fake vendor: the agent's own config directory is
//! the one the vendor command is given, and no credential is printed.

use std::path::Path;
use std::process::Command;

fn bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_anyagents"))
}

fn recorded_cwd(recorded: &str) -> String {
    recorded
        .split_whitespace()
        .find_map(|field| field.strip_prefix("cwd="))
        .expect("the fake vendor recorded its working directory")
        .to_string()
}

fn create(data: &Path, name: &str, harness: &str) {
    let out = bin()
        .current_dir(data)
        .args([
            "--data",
            ".",
            "agents",
            "create",
            name,
            "--harness",
            harness,
        ])
        .env("ANYAGENTS_CLAUDE_BIN", "/bin/true")
        .env("ANYAGENTS_CODEX_BIN", "/bin/true")
        .output()
        .unwrap();
    assert!(out.status.success(), "{out:?}");
}

#[test]
fn sign_in_runs_the_vendor_command_in_the_agents_config_dir() {
    let data = tempfile::tempdir().unwrap();
    create(data.path(), "ada", "claude-code");
    let log = data.path().join("vendor.log");
    let out = bin()
        .args(["--data", ".", "sign-in", "ada"])
        .current_dir(data.path())
        // Not a real `claude`: prints a link and the outcome the collector reads.
        .env("ANYAGENTS_FAKE_VENDOR_MODE", ":")
        .env("PATH", format!("{}:/usr/bin:/bin", fake_dir().display()))
        .env("ANYAGENTS_SIGN_IN_LOG", &log)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "{stdout}{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        stdout.contains("open https://auth.example/device"),
        "{stdout}"
    );
    assert!(
        stdout.contains("signed in\tada\tclaude-code\tada@example"),
        "{stdout}"
    );
    assert!(
        !stdout.contains("SECRET"),
        "a credential reached the output: {stdout}"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !stderr.contains("SECRET"),
        "a credential reached stderr: {stderr}"
    );

    let recorded = std::fs::read_to_string(&log).unwrap();
    let agents = data.path().canonicalize().unwrap().join("agents");
    let id = std::fs::read_dir(&agents)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .file_name();
    let expected = agents.join(&id).join("config");
    assert!(recorded.contains("argv=auth login "), "{recorded}");
    // The vendor command runs in the agent's own config directory and is told
    // about that same directory; it may be relative to the data directory.

    assert_eq!(
        std::fs::canonicalize(recorded_cwd(&recorded)).unwrap(),
        expected
    );
    // The variable may be relative to the data directory; both point here.
    let variable = recorded
        .split_whitespace()
        .find_map(|field| field.strip_prefix("CLAUDE_CONFIG_DIR="))
        .unwrap();
    assert_eq!(
        std::fs::canonicalize(data.path().join(variable)).unwrap(),
        expected
    );
    assert!(
        !stdout.contains("token SECRET"),
        "a plain vendor line was echoed by the CLI: {stdout}"
    );
}

#[test]
fn harnesses_lists_the_signable_vendors_without_a_credential_shaped_field() {
    let out = bin().args(["harnesses", "list"]).output().unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success());
    assert!(
        stdout.contains("claude-code\tCLAUDE_CONFIG_DIR\t"),
        "{stdout}"
    );
    assert!(stdout.contains("codex\tCODEX_HOME\t"), "{stdout}");
}

/// A `claude` and a `codex` that record how they were invoked and print the lines
/// the installer's collector reads. `ANYAGENTS_FAKE_VENDOR_MODE` runs a shell body
/// first, so a test can script a vendor that wrote its store and printed nothing.
fn fake_dir() -> std::path::PathBuf {
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("fake-vendor");
    std::fs::create_dir_all(&dir).unwrap();
    let script = "#!/bin/sh\n\
        echo \"argv=$* cwd=$(pwd) CLAUDE_CONFIG_DIR=$CLAUDE_CONFIG_DIR CODEX_HOME=$CODEX_HOME KEY=SECRET\" >> \"$ANYAGENTS_SIGN_IN_LOG\"\n\
        if [ -n \"$ANYAGENTS_FAKE_VENDOR_MODE\" ]; then\n\
        eval \"$ANYAGENTS_FAKE_VENDOR_MODE\"\n\
        fi\n\
        echo 'LINK https://auth.example/device'\n\
        echo 'not logged in: run the vendor login first' 1>&2\n\
        echo 'SIGNED_IN ada@example'\n\
        echo 'token SECRET written to the vendor store'\n";
    for name in ["claude", "codex"] {
        let path = dir.join(name);
        std::fs::write(&path, script).unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    dir
}
