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
    }
    path
}

/// Take the operator's environment back out of this process.
pub fn clear() {
    // SAFETY: under `lock()`, as `operator` is.
    unsafe {
        std::env::remove_var("ANYAGENTS_OPERATOR_TOKEN_FILE");
        std::env::remove_var("ANYAGENTS_CAPABILITY_KEY");
    }
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
