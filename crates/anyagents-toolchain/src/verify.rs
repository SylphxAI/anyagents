//! Verification of one declared tool: canonical path, sha256 and exact version.

use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};

use crate::ToolchainError;
use crate::manifest::ToolSpec;

/// How long a tool may take to print its version.
const VERSION_TIMEOUT: Duration = Duration::from_secs(10);

/// What identifies the bytes on disk; a change re-runs verification.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Fingerprint {
    dev: u64,
    ino: u64,
    mtime_ns: i128,
    size: u64,
}

pub(crate) fn fingerprint(path: &Path) -> Option<(PathBuf, Fingerprint)> {
    let canonical = std::fs::canonicalize(path).ok()?;
    let meta = std::fs::metadata(&canonical).ok()?;
    #[cfg(unix)]
    let (dev, ino) = {
        use std::os::unix::fs::MetadataExt as _;
        (meta.dev(), meta.ino())
    };
    #[cfg(not(unix))]
    let (dev, ino) = (0, 0);
    let mtime_ns = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map_or(0, |d| d.as_nanos() as i128);
    Some((
        canonical,
        Fingerprint {
            dev,
            ino,
            mtime_ns,
            size: meta.len(),
        },
    ))
}

/// Lowercase hex sha256 of a file.
pub fn sha256_file(path: &Path) -> std::io::Result<String> {
    let mut file = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect())
}

/// Verify `spec` for tool `name`. Returns the canonical target on success.
pub(crate) fn verify(name: &str, spec: &ToolSpec) -> Result<PathBuf, ToolchainError> {
    let missing = || ToolchainError::Missing {
        tool: name.to_string(),
        path: spec.path.clone(),
    };
    let canonical = std::fs::canonicalize(&spec.path).map_err(|_| missing())?;
    let meta = std::fs::metadata(&canonical).map_err(|_| missing())?;
    if !meta.is_file() || !is_executable(&meta) {
        return Err(ToolchainError::NotExecutable {
            tool: name.to_string(),
            path: spec.path.clone(),
        });
    }
    let actual = sha256_file(&canonical).map_err(|_| missing())?;
    if actual != spec.sha256 {
        return Err(ToolchainError::ChecksumMismatch {
            tool: name.to_string(),
            path: spec.path.clone(),
            expected: spec.sha256.clone(),
            actual,
        });
    }
    let output = version_output(&canonical, &spec.version_args);
    if !output.contains(&spec.version) {
        return Err(ToolchainError::VersionMismatch {
            tool: name.to_string(),
            path: spec.path.clone(),
            expected: spec.version.clone(),
            actual: first_line(&output),
        });
    }
    Ok(canonical)
}

fn first_line(text: &str) -> String {
    let line = text.lines().find(|l| !l.trim().is_empty()).unwrap_or("");
    line.chars().take(200).collect()
}

#[cfg(unix)]
fn is_executable(meta: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt as _;
    meta.permissions().mode() & 0o111 != 0
}

#[cfg(not(unix))]
fn is_executable(_: &std::fs::Metadata) -> bool {
    true
}

/// Stdout and stderr of `program args`, empty when it cannot run in time.
fn version_output(program: &Path, args: &[String]) -> String {
    let Ok(mut child) = Command::new(program)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    else {
        return String::new();
    };
    let started = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(_)) => break,
            Ok(None) if started.elapsed() < VERSION_TIMEOUT => {
                std::thread::sleep(Duration::from_millis(20));
            }
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                return String::new();
            }
        }
    }
    let mut text = String::new();
    if let Some(mut out) = child.stdout.take() {
        let _ = out.read_to_string(&mut text);
    }
    if let Some(mut err) = child.stderr.take() {
        text.push('\n');
        let _ = err.read_to_string(&mut text);
    }
    text
}
