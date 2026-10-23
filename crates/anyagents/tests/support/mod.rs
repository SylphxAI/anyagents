//! Shared helpers for integration tests: one operator token for the server a
//! test starts, so every call is made by the operator it is.

#![allow(dead_code)]

use std::ffi::OsString;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, OnceLock};

fn guard() -> &'static Mutex<()> {
    static GUARD: OnceLock<Mutex<()>> = OnceLock::new();
    GUARD.get_or_init(|| Mutex::new(()))
}

/// Held for the life of one test, so no other test sees this operator's
/// environment.
pub fn lock() -> MutexGuard<'static, ()> {
    guard().lock().unwrap_or_else(|err| err.into_inner())
}

/// The operator token a test deployment uses. It is not a real secret.
pub const TOKEN: &str = "test-operator-token-0123456789abcdef";

/// The token file a server reads, in the data directory every test names.
pub const TOKEN_FILE: &str = "operator-token";

/// The same token in a data directory, and the header that presents it with
/// its path: a test starts its own server by that path and every call it makes
/// presents the token from it, so no environment and no lock is shared.
pub fn operator_at(data: &Path) -> String {
    let path = data.join(TOKEN_FILE);
    std::fs::write(&path, format!("{TOKEN}\n")).expect("operator token");
    header_at(&path)
}

/// The deployment's own start line: `<agent>=<uid>` for the agents a test
/// deployment names, before every serve.
pub fn agents_line() -> String {
    [
        ("alpha", "2101"),
        ("beta", "2102"),
        ("ada", "2103"),
        ("bea", "2104"),
        ("cy", "2105"),
        ("kind", "2106"),
        ("gamma", "2107"),
        ("delta", "2108"),
    ]
    .iter()
    .map(|(name, seat)| format!("{name}={seat}"))
    .collect::<Vec<_>>()
    .join(",")
}

/// The operator header naming one token file. A digest of the token and of the
/// path is all the server needs: the file is read once, here.
pub fn header_at(path: &Path) -> String {
    let _ = path;
    format!("x-anyagents-operator: {TOKEN}\r\n")
}

/// The capability key a test server and its fixtures share.
pub const CAPABILITY_KEY: &str = "test-capability-key-0123456789abcdef";

/// The header an operator request carries.
pub fn header() -> String {
    format!("x-anyagents-operator: {TOKEN}\r\n")
}

/// Put the operator's token file in `data` and point this process at it.
pub fn operator(data: &Path) -> PathBuf {
    let path = data.join("operator-token");
    std::fs::write(&path, format!("{TOKEN}\n")).unwrap();
    // SAFETY: every test that starts a server takes `lock()` first, and no
    // thread reads these while another writes them.
    unsafe {
        std::env::set_var("ANYAGENTS_OPERATOR_TOKEN_FILE", &path);
        std::env::set_var("ANYAGENTS_CAPABILITY_KEY", CAPABILITY_KEY);
        std::env::remove_var("ANYAGENTS_AGENT_ROUTE");
        // No seat of a previous test, and the one launcher this deployment
        // starts its agents with: the test double that only runs the command.
        std::env::remove_var("ANYAGENTS_FAKE_MODE");
        std::env::set_var("ANYAGENTS_LAUNCHER", fake_launcher(data));
        std::env::set_var(
            "ANYAGENTS_AGENTS",
            [
                ("alpha", "2101"),
                ("beta", "2102"),
                ("ada", "2103"),
                ("bea", "2104"),
                ("cy", "2105"),
                ("kind", "2106"),
                ("gamma", "2107"),
                ("delta", "2108"),
            ]
            .iter()
            .map(|(name, seat)| format!("{name}={seat}"))
            .collect::<Vec<_>>()
            .join(","),
        );
    }
    path
}

/// The same operator, and the one OS seat this host really has: this test
/// process's own uid, which keeps the launcher and the server out of root.
pub fn operator_with_seat(data: &Path) -> PathBuf {
    let path = operator(data);
    // SAFETY: as `operator`.
    let (uid, gid) = (our_uid(), our_gid());
    unsafe {
        std::env::set_var("ANYAGENTS_AGENT_UID", uid.to_string());
        std::env::set_var("ANYAGENTS_AGENT_GID", gid.to_string());
        std::env::set_var("ANYAGENTS_SOCKET_GID", gid.to_string());
        // A test machine: its host accounts are not an agent's seats, so the
        // double-shaped seat this process's own uid stands in for them.
        std::env::set_var("ANYAGENTS_FAKE_MODE", "1");
    }
    path
}

/// The operator and a configured seat, with no test double: a deployment that
/// claims its boundary and whose seat is this machine's own accounts.
pub fn operator_with_configured_seat(data: &Path) -> PathBuf {
    let path = operator(data);
    let (uid, gid) = (our_uid(), our_gid());
    let launcher = fake_launcher(data);
    // SAFETY: as `operator`.
    unsafe {
        std::env::set_var("ANYAGENTS_AGENT_UID", uid.to_string());
        std::env::set_var("ANYAGENTS_AGENT_GID", gid.to_string());
        std::env::set_var("ANYAGENTS_SOCKET_GID", gid.to_string());
        std::env::set_var("ANYAGENTS_LAUNCHER", launcher);
        std::env::set_var("ANYAGENTS_AGENTS", agents_line());
        // A test machine is its own single seat: the deployment gives every
        // agent the uid this process runs as.
        std::env::set_var("ANYAGENTS_TESTS", "own-seat");
    }
    path
}

/// A privileged launcher that only exists for the test that asks for it: a
/// script this process's own user can run, so the boundary's shape is exercised
/// without any host change.
pub fn fake_launcher(data: &Path) -> std::path::PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let bin = data.join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    let launcher = bin.join("setpriv");
    std::fs::write(
        &launcher,
        "#!/bin/sh\n# anyagents-setpriv test double: the seat's groups, then the command.\n\
         while [ \"$1\" != \"--\" ]; do shift; done\nshift\nexec \"$@\"\n",
    )
    .unwrap();
    std::fs::set_permissions(&launcher, std::fs::Permissions::from_mode(0o755)).unwrap();
    launcher
}

/// Take the operator's environment back out of this process.
pub fn clear() {
    // SAFETY: under `lock()`, as `operator` is.
    unsafe {
        std::env::remove_var("ANYAGENTS_OPERATOR_TOKEN_FILE");
        std::env::remove_var("ANYAGENTS_CAPABILITY_KEY");
        std::env::remove_var("ANYAGENTS_AGENT_UID");
        std::env::remove_var("ANYAGENTS_AGENT_GID");
        std::env::remove_var("ANYAGENTS_SOCKET_GID");
    }
}

/// This process's own uid and gid, read through `id` so the test needs no
/// unsafe call of its own.
fn our_uid() -> u32 {
    id_field("-u")
}

fn our_gid() -> u32 {
    id_field("-g")
}

fn id_field(flag: &str) -> u32 {
    let out = std::process::Command::new("id")
        .arg(flag)
        .output()
        .expect("id");
    String::from_utf8_lossy(&out.stdout)
        .trim()
        .parse()
        .unwrap_or(0)
}

/// One call over the server's socket, answered in full: the raw answer.
pub fn call_raw(socket: &Path, method: &str, path: &str, body: &str, authority: &str) -> String {
    let mut stream = std::os::unix::net::UnixStream::connect(socket).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(20)))
        .unwrap();
    stream
        .write_all(
            format!(
                "{method} {path} HTTP/1.1\r\nHost: local\r\n{authority}content-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            )
            .as_bytes(),
        )
        .unwrap();
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).unwrap();
    String::from_utf8_lossy(&buf).to_string()
}

/// The JSON body of one call, and its status line.
pub fn call(socket: &Path, method: &str, path: &str, body: &str) -> (String, serde_json::Value) {
    let answer = call_raw(socket, method, path, body, &header());
    let status = answer.lines().next().unwrap_or_default().to_string();
    let at = answer.find("\r\n\r\n").map(|at| at + 4).expect(&answer);
    let value = serde_json::from_str(&answer[at..]).unwrap_or(serde_json::Value::Null);
    (status, value)
}

/// A `PATH` where creating an agent installs nothing: `npm` answers a version
/// and a no-op install, and `claude` is this binary.
pub fn machine_path(dir: &Path) -> OsString {
    use std::os::unix::fs::PermissionsExt;
    let bin = dir.join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    let launcher = bin.join("claude");
    if !launcher.exists() {
        std::os::unix::fs::symlink(env!("CARGO_BIN_EXE_anyagents"), &launcher).unwrap();
    }
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

/// How long one integration test waits before it gives up on a server.
pub const WAIT: std::time::Duration = std::time::Duration::from_secs(30);

/// Poll `ready` until it holds, or fail with `why`.
pub fn wait_for(mut ready: impl FnMut() -> bool, why: &str) {
    let deadline = std::time::Instant::now() + WAIT;
    while !ready() {
        assert!(std::time::Instant::now() < deadline, "{why}");
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}
