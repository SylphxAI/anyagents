use std::process::Command;

fn bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_anyagents"))
}

#[test]
fn serve_and_mcp_are_not_implemented() {
    let dir = tempfile::tempdir().unwrap();
    let serve = bin()
        .arg("--data")
        .arg(dir.path())
        .arg("serve")
        .output()
        .unwrap();
    assert!(!serve.status.success());
    let stderr = String::from_utf8(serve.stderr).unwrap();
    assert!(stderr.contains("not implemented yet"), "{stderr}");

    let mcp = bin()
        .arg("--data")
        .arg(dir.path())
        .arg("mcp")
        .arg("--agent")
        .arg("alpha")
        .output()
        .unwrap();
    assert!(!mcp.status.success());
    let stderr = String::from_utf8(mcp.stderr).unwrap();
    assert!(stderr.contains("not implemented yet"), "{stderr}");
}

#[test]
fn create_send_inbox_and_usage() {
    let dir = tempfile::tempdir().unwrap();
    let create = bin()
        .args(["--data"])
        .arg(dir.path())
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
        .args(["send", "alpha", "hello"])
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

    let group = bin()
        .arg("--data")
        .arg(dir.path())
        .args(["send", "group:crew", "hello"])
        .output()
        .unwrap();
    assert!(!group.status.success());
    let stderr = String::from_utf8(group.stderr).unwrap();
    assert!(stderr.contains("not implemented yet"), "{stderr}");
}
