//! Local workspace effects use the embedding runtime's trusted launch principal.
//! Tool arguments never select a seat. Without a configured seat, ordinary local
//! file IO and inherited-UID commands retain their existing behavior.

use std::io::Write;
use std::path::Path;
use std::process::{Command, Output, Stdio};

use anyagents_harness::{SEAT_LAUNCH_ENV, spawn_command};

pub(super) fn delegated(env: &[(String, String)]) -> bool {
    env.iter().any(|(key, _)| key == SEAT_LAUNCH_ENV)
}

pub(super) fn command(program: &Path, env: &[(String, String)]) -> Result<Command, String> {
    // The shared launcher historically treats malformed specs as unconfigured.
    // A configured workspace principal must instead fail closed, never run the
    // next tool as the server. Only the server-supplied launch environment counts.
    if let Some((_, spec)) = env.iter().find(|(key, _)| key == SEAT_LAUNCH_ENV) {
        let parts: Vec<_> = spec.split('\t').collect();
        if parts.len() != 4 || parts.iter().any(|part| part.is_empty()) {
            return Err("invalid trusted workspace launcher specification".into());
        }
    }
    Ok(spawn_command(program, env))
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
    env: &[(String, String)],
    script: &str,
    input: &[u8],
) -> Result<Vec<u8>, String> {
    let mut cmd = command(Path::new("bash"), env)?;
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

pub(super) fn read_to_string(path: &Path, env: &[(String, String)]) -> Result<String, String> {
    if !delegated(env) {
        return std::fs::read_to_string(path).map_err(|e| e.to_string());
    }
    String::from_utf8(file_command(path, env, "cat -- \"$1\"", b"")?).map_err(|e| e.to_string())
}

pub(super) fn write(
    path: &Path,
    content: impl AsRef<[u8]>,
    env: &[(String, String)],
) -> Result<(), String> {
    if !delegated(env) {
        return std::fs::write(path, content).map_err(|e| e.to_string());
    }
    file_command(path, env, "cat > \"$1\"", content.as_ref()).map(|_| ())
}

pub(super) fn create_dir_all(path: &Path, env: &[(String, String)]) -> Result<(), String> {
    if !delegated(env) {
        return std::fs::create_dir_all(path).map_err(|e| e.to_string());
    }
    file_command(path, env, "mkdir -p -- \"$1\"", b"").map(|_| ())
}

pub(super) fn remove_if_exists(path: &Path, env: &[(String, String)]) -> Result<(), String> {
    if !delegated(env) {
        return match std::fs::remove_file(path) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            result => result.map_err(|e| e.to_string()),
        };
    }
    file_command(path, env, "rm -f -- \"$1\"", b"").map(|_| ())
}
