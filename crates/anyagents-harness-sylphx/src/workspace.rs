//! Local workspace effects use the embedding runtime's trusted launch principal.
//! Tool arguments never select a seat. Without a configured seat, ordinary local
//! file IO and inherited-UID commands retain their existing behavior.

use std::io::Write;
use std::path::Path;
use std::process::{Command, Output, Stdio};

use anyagents_harness::{SEAT_LAUNCH_ENV, SEAT_PEERS_ENV, spawn_command};
use std::sync::Arc;

type PeerResolver = Arc<dyn Fn() -> Result<String, String> + Send + Sync>;

/// Launch values plus a host-owned authority reader, never tool-selected data.
#[derive(Clone, Default)]
pub(crate) struct Environment {
    values: Vec<(String, String)>,
    peers: Option<PeerResolver>,
}

impl Environment {
    pub(crate) fn set_peer_resolver(&mut self, resolver: PeerResolver) {
        self.peers = Some(resolver);
    }
}

impl From<Vec<(String, String)>> for Environment {
    fn from(values: Vec<(String, String)>) -> Self {
        Self {
            values,
            peers: None,
        }
    }
}

impl std::ops::Deref for Environment {
    type Target = [(String, String)];
    fn deref(&self) -> &Self::Target {
        &self.values
    }
}

pub(super) fn delegated(env: &Environment) -> bool {
    env.iter().any(|(key, _)| key == SEAT_LAUNCH_ENV)
}

pub(super) fn command(program: &Path, env: &Environment) -> Result<Command, String> {
    let Some(resolve) = env.peers.as_ref().filter(|_| delegated(env)) else {
        return spawn_command(program, env).map_err(|e| e.to_string());
    };
    // Resolve on every command, not on start or turn entry: agents can be
    // created by a tool while this harness/turn is already running. A failed
    // refresh refuses instead of reusing a stale allowlist.
    let peers = resolve()?;
    let mut values: Vec<_> = env
        .iter()
        .filter(|(k, _)| k != SEAT_PEERS_ENV)
        .cloned()
        .collect();
    values.push((SEAT_PEERS_ENV.to_owned(), peers));
    spawn_command(program, &values).map_err(|e| e.to_string())
}

/// Pipe inputs instead of putting file contents/patches in argv. Drain output
/// while writing so a refusing launcher or a large read cannot deadlock.
pub(super) fn output(mut cmd: Command, input: &[u8]) -> Result<Output, String> {
    let mut child = cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("cannot start workspace command: {e}"))?;
    let mut stdin = child
        .stdin
        .take()
        .ok_or("missing workspace command stdin")?;
    let (written, result) = std::thread::scope(|scope| {
        let writer = scope.spawn(move || stdin.write_all(input));
        let result = child.wait_with_output();
        (writer.join(), result)
    });
    let result = result.map_err(|e| format!("workspace command failed: {e}"))?;
    if !result.status.success() {
        return Ok(result); // Preserve the launcher's actual refusal/status.
    }
    written
        .map_err(|_| "workspace input writer panicked".to_string())?
        .map_err(|e| format!("workspace input was not delivered: {e}"))?;
    Ok(result)
}

fn file_command(
    path: &Path,
    env: &Environment,
    script: &str,
    input: &[u8],
) -> Result<Vec<u8>, String> {
    let mut cmd = command(Path::new("/bin/bash"), env)?;
    cmd.args(["-c", script, "workspace-file"]).arg(path);
    let out = output(cmd, input)?;
    if !out.status.success() {
        return Err(format!(
            "workspace file command refused ({}): {}",
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    Ok(out.stdout)
}

pub(super) fn read_bytes(path: &Path, env: &Environment) -> Result<Vec<u8>, String> {
    if !delegated(env) {
        return std::fs::read(path).map_err(|e| e.to_string());
    }
    file_command(path, env, "/bin/cat -- \"$1\"", b"")
}

pub(super) fn read_to_string(path: &Path, env: &Environment) -> Result<String, String> {
    String::from_utf8(read_bytes(path, env)?).map_err(|e| e.to_string())
}

pub(super) fn write(
    path: &Path,
    content: impl AsRef<[u8]>,
    env: &Environment,
) -> Result<(), String> {
    if !delegated(env) {
        return std::fs::write(path, content).map_err(|e| e.to_string());
    }
    file_command(path, env, "/bin/cat > \"$1\"", content.as_ref()).map(|_| ())
}

pub(super) fn create_dir_all(path: &Path, env: &Environment) -> Result<(), String> {
    if !delegated(env) {
        return std::fs::create_dir_all(path).map_err(|e| e.to_string());
    }
    file_command(path, env, "/bin/mkdir -p -- \"$1\"", b"").map(|_| ())
}

pub(super) fn remove_if_exists(path: &Path, env: &Environment) -> Result<(), String> {
    if !delegated(env) {
        return match std::fs::remove_file(path) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            result => result.map_err(|e| e.to_string()),
        };
    }
    file_command(path, env, "/bin/rm -f -- \"$1\"", b"").map(|_| ())
}
