//! Shared capability core and pluggable workspace backend (`LocalHost` vs `RemoteSandbox`).
//!
//! When an agent has a local worktree on the orchestrator host, [`WorkspaceBackend::LocalHost`]
//! executes file and command operations directly against that directory. When an agent is bound
//! to a remote container sandbox via `<agent_config_dir>/sandbox.json` or `ANYAGENTS_SANDBOX_*`
//! environment variables, [`WorkspaceBackend::RemoteSandbox`] routes command execution and file
//! reads/writes/edits/listings to the remote sandbox HTTP API.

use std::io::Read as _;
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const DEFAULT_REMOTE_WORKDIR: &str = "/workspace";
const DEFAULT_EXEC_TIMEOUT_MS: u64 = 30_000;

fn default_remote_workdir() -> String {
    DEFAULT_REMOTE_WORKDIR.to_string()
}

/// Configuration for a remote container sandbox bound to an agent.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SandboxConfig {
    /// Base URL of the sandbox API (e.g. `https://sandbox.example.com` or `http://127.0.0.1:9090`).
    pub endpoint: String,
    /// Unique identifier of the remote sandbox instance.
    pub sandbox_id: String,
    /// Optional bearer token used to authenticate requests to the sandbox API.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    /// Default working directory inside the remote sandbox container.
    #[serde(default = "default_remote_workdir")]
    pub workdir: String,
}

impl SandboxConfig {
    /// Load [`SandboxConfig`] from `<config_dir>/sandbox.json` if present and valid.
    pub fn from_config_dir(config_dir: &Path) -> Option<Self> {
        let raw = std::fs::read_to_string(config_dir.join("sandbox.json")).ok()?;
        let mut cfg: Self = serde_json::from_str(&raw).ok()?;
        cfg.endpoint = cfg.endpoint.trim().trim_end_matches('/').to_string();
        cfg.sandbox_id = cfg.sandbox_id.trim().to_string();
        cfg.token = cfg
            .token
            .map(|t| t.trim().to_string())
            .filter(|t| !t.is_empty());
        let wd = cfg.workdir.trim().trim_end_matches('/');
        cfg.workdir = if wd.is_empty() {
            DEFAULT_REMOTE_WORKDIR.to_string()
        } else {
            wd.to_string()
        };
        if cfg.endpoint.is_empty() || cfg.sandbox_id.is_empty() {
            return None;
        }
        Some(cfg)
    }

    /// Load [`SandboxConfig`] from `ANYAGENTS_SANDBOX_API_URL` and `ANYAGENTS_SANDBOX_ID`.
    pub fn from_env() -> Option<Self> {
        let endpoint = std::env::var("ANYAGENTS_SANDBOX_API_URL")
            .ok()?
            .trim()
            .trim_end_matches('/')
            .to_string();
        let sandbox_id = std::env::var("ANYAGENTS_SANDBOX_ID")
            .ok()?
            .trim()
            .to_string();
        if endpoint.is_empty() || sandbox_id.is_empty() {
            return None;
        }
        let token = std::env::var("ANYAGENTS_SANDBOX_TOKEN")
            .ok()
            .map(|t| t.trim().to_string())
            .filter(|t| !t.is_empty());
        let workdir = std::env::var("ANYAGENTS_SANDBOX_WORKDIR")
            .ok()
            .map(|w| w.trim().to_string())
            .filter(|w| !w.is_empty())
            .unwrap_or_else(default_remote_workdir);
        Some(Self {
            endpoint,
            sandbox_id,
            token,
            workdir,
        })
    }

    /// Resolve [`SandboxConfig`] from `<config_dir>/sandbox.json` first, then environment variables.
    pub fn resolve(config_dir: Option<&Path>) -> Option<Self> {
        config_dir
            .and_then(Self::from_config_dir)
            .or_else(Self::from_env)
    }
}

/// Result of executing a command in a [`WorkspaceBackend`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecOutput {
    #[serde(default)]
    pub stdout: String,
    #[serde(default)]
    pub stderr: String,
    #[serde(default)]
    pub exit_code: i32,
}

/// Pluggable execution and file backend for an agent's workspace.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkspaceBackend {
    /// Local directory on the orchestrator host.
    LocalHost { root: PathBuf },
    /// Remote sandbox container reachable over HTTP.
    RemoteSandbox(SandboxConfig),
}

impl WorkspaceBackend {
    /// Resolve the active backend for an agent from its config directory and local workdir fallback.
    pub fn resolve(config_dir: Option<&Path>, local_root: PathBuf) -> Self {
        if let Some(cfg) = SandboxConfig::resolve(config_dir) {
            Self::RemoteSandbox(cfg)
        } else {
            Self::LocalHost { root: local_root }
        }
    }

    pub fn is_remote(&self) -> bool {
        matches!(self, Self::RemoteSandbox(_))
    }

    pub fn root_display(&self) -> String {
        match self {
            Self::LocalHost { root } => root.display().to_string(),
            Self::RemoteSandbox(cfg) => cfg.workdir.clone(),
        }
    }

    /// Resolve a user-supplied relative or absolute path against the backend's root directory.
    pub fn resolve_path(&self, raw: Option<&str>, required: bool) -> Result<String, String> {
        let base = match self {
            Self::LocalHost { root } => root.as_path(),
            Self::RemoteSandbox(cfg) => Path::new(&cfg.workdir),
        };
        let Some(raw_str) = raw else {
            if required {
                return Err("`path` is required for this `files` action".to_string());
            }
            return Ok(normalize_lexical_path(base).display().to_string());
        };
        if raw_str.contains('\0') {
            return Err("`path` must not contain null bytes".to_string());
        }
        let trimmed = raw_str.trim();
        if trimmed.is_empty() {
            if required {
                return Err("`path` must not be empty".to_string());
            }
            return Ok(normalize_lexical_path(base).display().to_string());
        }
        let candidate = Path::new(trimmed);
        let joined = if candidate.is_absolute() {
            candidate.to_path_buf()
        } else {
            base.join(candidate)
        };
        Ok(normalize_lexical_path(&joined).display().to_string())
    }

    /// Execute a shell command on the backend (`LocalHost` or `RemoteSandbox`).
    pub fn exec(
        &self,
        command: &str,
        cwd: Option<&str>,
        timeout_ms: Option<u64>,
    ) -> Result<ExecOutput, String> {
        let cmd_trimmed = command.trim();
        if cmd_trimmed.is_empty() {
            return Err("`workspace.exec` requires a non-empty `cmd`".to_string());
        }
        let effective_timeout = timeout_ms
            .unwrap_or(DEFAULT_EXEC_TIMEOUT_MS)
            .clamp(100, 120_000);
        let resolved_cwd = self.resolve_path(cwd, false)?;

        match self {
            Self::LocalHost { .. } => {
                let cwd_path = PathBuf::from(&resolved_cwd);
                if !cwd_path.is_dir() {
                    return Err(format!(
                        "exec working directory {} does not exist",
                        cwd_path.display()
                    ));
                }
                let output = std::process::Command::new("/bin/sh")
                    .arg("-c")
                    .arg(cmd_trimmed)
                    .current_dir(&cwd_path)
                    .output()
                    .map_err(|e| format!("failed to spawn command: {e}"))?;
                Ok(ExecOutput {
                    stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
                    stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
                    exit_code: output.status.code().unwrap_or(-1),
                })
            }
            Self::RemoteSandbox(cfg) => {
                remote_exec(cfg, cmd_trimmed, &resolved_cwd, effective_timeout)
            }
        }
    }

    /// Read raw file bytes from the backend (`LocalHost` or `RemoteSandbox`).
    /// Returns `(resolved_path, bytes)`.
    pub fn read_file_bytes(&self, path: Option<&str>) -> Result<(String, Vec<u8>), String> {
        let resolved = self.resolve_path(path, true)?;
        match self {
            Self::LocalHost { .. } => {
                let p = Path::new(&resolved);
                let meta =
                    std::fs::metadata(p).map_err(|e| format!("cannot read {resolved}: {e}"))?;
                if meta.is_dir() {
                    return Err(format!(
                        "{resolved} is a directory; use `action: \"list\"` or `action: \"glob\"` to list files"
                    ));
                }
                let bytes = std::fs::read(p).map_err(|e| format!("cannot read {resolved}: {e}"))?;
                Ok((resolved, bytes))
            }
            Self::RemoteSandbox(cfg) => {
                let bytes = remote_read_file(cfg, &resolved)?;
                Ok((resolved, bytes))
            }
        }
    }

    /// Write or append file bytes to the backend (`LocalHost` or `RemoteSandbox`).
    /// Returns `(resolved_path, total_bytes_after_write, sha256_hex)`.
    pub fn write_file_bytes(
        &self,
        path: Option<&str>,
        content: &[u8],
        append: bool,
        overwrite: bool,
    ) -> Result<(String, usize, String), String> {
        let resolved = self.resolve_path(path, true)?;
        match self {
            Self::LocalHost { .. } => {
                let p = Path::new(&resolved);
                if !append && !overwrite && p.exists() {
                    return Err(format!(
                        "file {resolved} already exists and `overwrite` is false"
                    ));
                }
                if let Some(parent) = p.parent()
                    && !parent.as_os_str().is_empty()
                {
                    std::fs::create_dir_all(parent).map_err(|e| {
                        format!("cannot create parent directory {}: {e}", parent.display())
                    })?;
                }
                if append {
                    use std::io::Write as _;
                    let mut f = std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(p)
                        .map_err(|e| format!("cannot open {resolved} for append: {e}"))?;
                    f.write_all(content)
                        .map_err(|e| format!("cannot append to {resolved}: {e}"))?;
                } else {
                    std::fs::write(p, content)
                        .map_err(|e| format!("cannot write {resolved}: {e}"))?;
                }
                let final_bytes =
                    std::fs::read(p).map_err(|e| format!("cannot read back {resolved}: {e}"))?;
                let sha = sha256_hex(&final_bytes);
                Ok((resolved, final_bytes.len(), sha))
            }
            Self::RemoteSandbox(cfg) => {
                let final_bytes = remote_write_file(cfg, &resolved, content, append, overwrite)?;
                let sha = sha256_hex(&final_bytes);
                Ok((resolved, final_bytes.len(), sha))
            }
        }
    }

    /// List entries in a directory on the backend.
    pub fn list_dir(
        &self,
        path: Option<&str>,
        include_hidden: bool,
        max_results: usize,
    ) -> Result<serde_json::Value, String> {
        let resolved = self.resolve_path(path, false)?;
        match self {
            Self::LocalHost { root } => {
                let dir_path = Path::new(&resolved);
                let rd = std::fs::read_dir(dir_path)
                    .map_err(|e| format!("cannot list directory {resolved}: {e}"))?;
                let mut raw_entries: Vec<(String, serde_json::Value)> = Vec::new();
                for entry_res in rd {
                    let Ok(entry) = entry_res else {
                        continue;
                    };
                    let name = entry.file_name().to_string_lossy().into_owned();
                    if !include_hidden && name.starts_with('.') {
                        continue;
                    }
                    let entry_path = entry.path();
                    let Ok(sym_meta) = std::fs::symlink_metadata(&entry_path) else {
                        continue;
                    };
                    let ft = sym_meta.file_type();
                    let (kind, bytes) = if ft.is_symlink() {
                        let target_len = std::fs::metadata(&entry_path)
                            .ok()
                            .filter(|m| m.is_file())
                            .map_or(0, |m| m.len());
                        ("symlink", target_len)
                    } else if ft.is_dir() {
                        ("dir", 0)
                    } else {
                        ("file", sym_meta.len())
                    };
                    let rel_path = entry_path
                        .strip_prefix(root)
                        .ok()
                        .filter(|p| !p.as_os_str().is_empty())
                        .map(|p| p.display().to_string())
                        .unwrap_or_else(|| entry_path.display().to_string());
                    raw_entries.push((
                        name.clone(),
                        serde_json::json!({
                            "name": name,
                            "path": rel_path,
                            "kind": kind,
                            "bytes": bytes,
                        }),
                    ));
                }
                raw_entries.sort_by(|a, b| a.0.cmp(&b.0));
                let truncated = raw_entries.len() > max_results;
                let entries: Vec<serde_json::Value> = raw_entries
                    .into_iter()
                    .take(max_results)
                    .map(|(_, v)| v)
                    .collect();
                let count = entries.len();
                Ok(serde_json::json!({
                    "action": "list",
                    "path": resolved,
                    "entries": entries,
                    "count": count,
                    "truncated": truncated,
                }))
            }
            Self::RemoteSandbox(cfg) => {
                remote_list_dir(cfg, &resolved, include_hidden, max_results)
            }
        }
    }
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

pub fn normalize_lexical_path(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for comp in path.components() {
        match comp {
            Component::Prefix(p) => out.push(p.as_os_str()),
            Component::RootDir => out.push(comp.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                if !out.pop() && !out.is_absolute() {
                    out.push("..");
                }
            }
            Component::Normal(c) => out.push(c),
        }
    }
    if out.as_os_str().is_empty() {
        PathBuf::from(".")
    } else {
        out
    }
}

fn url_encode_query_component(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len() + 8);
    for b in raw.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~' | b'/') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

fn shell_single_quote(raw: &str) -> String {
    format!("'{}'", raw.replace('\'', "'\"'\"'"))
}

pub fn base64_encode(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(TABLE[((n >> 18) & 0x3f) as usize] as char);
        out.push(TABLE[((n >> 12) & 0x3f) as usize] as char);
        if chunk.len() > 1 {
            out.push(TABLE[((n >> 6) & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(TABLE[(n & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

pub fn base64_decode(encoded: &str) -> Result<Vec<u8>, String> {
    let clean: Vec<u8> = encoded
        .bytes()
        .filter(|b| !b.is_ascii_whitespace())
        .collect();
    if clean.is_empty() {
        return Ok(Vec::new());
    }
    if !clean.len().is_multiple_of(4) {
        return Err("invalid base64 length".to_string());
    }
    let decode_val = |b: u8| -> Result<u32, String> {
        match b {
            b'A'..=b'Z' => Ok((b - b'A') as u32),
            b'a'..=b'z' => Ok((b - b'a' + 26) as u32),
            b'0'..=b'9' => Ok((b - b'0' + 52) as u32),
            b'+' | b'-' => Ok(62),
            b'/' | b'_' => Ok(63),
            b'=' => Ok(0),
            _ => Err(format!("invalid base64 character: {b}")),
        }
    };
    let mut out = Vec::with_capacity((clean.len() / 4) * 3);
    for chunk in clean.as_chunks::<4>().0 {
        let v0 = decode_val(chunk[0])?;
        let v1 = decode_val(chunk[1])?;
        let v2 = decode_val(chunk[2])?;
        let v3 = decode_val(chunk[3])?;
        let n = (v0 << 18) | (v1 << 12) | (v2 << 6) | v3;
        out.push(((n >> 16) & 0xff) as u8);
        if chunk[2] != b'=' {
            out.push(((n >> 8) & 0xff) as u8);
        }
        if chunk[3] != b'=' {
            out.push((n & 0xff) as u8);
        }
    }
    Ok(out)
}

fn ureq_agent(timeout_ms: u64) -> ureq::Agent {
    let config = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_millis(
            timeout_ms.saturating_add(5_000),
        )))
        .http_status_as_error(false)
        .build();
    config.into()
}

fn remote_exec(
    cfg: &SandboxConfig,
    command: &str,
    cwd: &str,
    timeout_ms: u64,
) -> Result<ExecOutput, String> {
    let url = format!(
        "{}/v1/sandboxes/{}/exec",
        cfg.endpoint.trim_end_matches('/'),
        url_encode_query_component(&cfg.sandbox_id)
    );
    let agent = ureq_agent(timeout_ms);
    let mut req = agent.post(&url).header("content-type", "application/json");
    if let Some(ref token) = cfg.token {
        req = req.header("authorization", &format!("Bearer {token}"));
    }
    let payload = serde_json::json!({
        "command": command,
        "cwd": cwd,
        "timeout_ms": timeout_ms,
    });
    let mut resp = req
        .send_json(&payload)
        .map_err(|e| format!("remote sandbox exec request failed ({url}): {e}"))?;
    let status = resp.status().as_u16();
    let mut body = Vec::new();
    let _ = resp.body_mut().as_reader().read_to_end(&mut body);
    if !(200..300).contains(&status) {
        let msg = String::from_utf8_lossy(&body);
        return Err(format!("remote sandbox exec returned HTTP {status}: {msg}"));
    }
    let parsed: serde_json::Value = serde_json::from_slice(&body)
        .map_err(|e| format!("invalid JSON from remote sandbox exec: {e}"))?;
    let stdout = parsed
        .get("stdout")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let stderr = parsed
        .get("stderr")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let exit_code = parsed
        .get("exit_code")
        .or_else(|| parsed.get("exitCode"))
        .and_then(|v| v.as_i64())
        .unwrap_or(0) as i32;
    Ok(ExecOutput {
        stdout,
        stderr,
        exit_code,
    })
}

fn remote_read_file(cfg: &SandboxConfig, resolved_path: &str) -> Result<Vec<u8>, String> {
    let url = format!(
        "{}/v1/sandboxes/{}/files?path={}",
        cfg.endpoint.trim_end_matches('/'),
        url_encode_query_component(&cfg.sandbox_id),
        url_encode_query_component(resolved_path)
    );
    let agent = ureq_agent(15_000);
    let mut req = agent.get(&url);
    if let Some(ref token) = cfg.token {
        req = req.header("authorization", &format!("Bearer {token}"));
    }
    if let Ok(mut resp) = req.call() {
        let status = resp.status().as_u16();
        if (200..300).contains(&status) {
            let content_type = resp
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_ascii_lowercase();
            let mut body = Vec::new();
            let _ = resp.body_mut().as_reader().read_to_end(&mut body);
            if content_type.contains("application/json")
                && let Ok(val) = serde_json::from_slice::<serde_json::Value>(&body)
                && let Some(content_str) = val.get("content").and_then(|v| v.as_str())
            {
                let is_b64 = val
                    .get("encoding")
                    .and_then(|v| v.as_str())
                    .is_some_and(|e| e.eq_ignore_ascii_case("base64"));
                if is_b64 {
                    return base64_decode(content_str);
                }
                return Ok(content_str.as_bytes().to_vec());
            }
            return Ok(body);
        }
        if status != 404 && status != 405 && status != 501 {
            let mut body = Vec::new();
            let _ = resp.body_mut().as_reader().read_to_end(&mut body);
            return Err(format!(
                "cannot read remote file {resolved_path} (HTTP {status}): {}",
                String::from_utf8_lossy(&body)
            ));
        }
    }

    // Fallback to `/exec` base64 read when `/files` returns 404/405/501.
    let quoted = shell_single_quote(resolved_path);
    let cmd = format!("if [ -d {quoted} ]; then exit 21; fi; base64 < {quoted} | tr -d '\\n\\r'");
    let exec_res = remote_exec(cfg, &cmd, &cfg.workdir, 15_000)?;
    if exec_res.exit_code == 21 {
        return Err(format!(
            "{resolved_path} is a directory; use `action: \"list\"` or `action: \"glob\"` to list files"
        ));
    }
    if exec_res.exit_code != 0 {
        return Err(format!(
            "cannot read remote file {resolved_path}: {}",
            exec_res.stderr.trim()
        ));
    }
    base64_decode(exec_res.stdout.trim())
}

fn remote_write_file(
    cfg: &SandboxConfig,
    resolved_path: &str,
    content: &[u8],
    append: bool,
    overwrite: bool,
) -> Result<Vec<u8>, String> {
    if !append && overwrite {
        let url = format!(
            "{}/v1/sandboxes/{}/files?path={}",
            cfg.endpoint.trim_end_matches('/'),
            url_encode_query_component(&cfg.sandbox_id),
            url_encode_query_component(resolved_path)
        );
        let agent = ureq_agent(15_000);
        let mut req = agent
            .put(&url)
            .header("content-type", "application/octet-stream");
        if let Some(ref token) = cfg.token {
            req = req.header("authorization", &format!("Bearer {token}"));
        }
        if let Ok(mut resp) = req.send(content) {
            let status = resp.status().as_u16();
            if (200..300).contains(&status) {
                return Ok(content.to_vec());
            }
            if status != 404 && status != 405 && status != 501 {
                let mut body = Vec::new();
                let _ = resp.body_mut().as_reader().read_to_end(&mut body);
                return Err(format!(
                    "cannot write remote file {resolved_path} (HTTP {status}): {}",
                    String::from_utf8_lossy(&body)
                ));
            }
        }
    }

    // Fallback to `/exec` atomic base64 write/append.
    let quoted = shell_single_quote(resolved_path);
    let b64 = base64_encode(content);
    let redirect = if append { ">>" } else { ">" };
    let exist_guard = if !append && !overwrite {
        format!("if [ -e {quoted} ]; then exit 17; fi; ")
    } else {
        String::new()
    };
    let cmd = format!(
        "{exist_guard}mkdir -p \"$(dirname {quoted})\" && printf '%s' '{b64}' | base64 -d {redirect} {quoted}"
    );
    let exec_res = remote_exec(cfg, &cmd, &cfg.workdir, 15_000)?;
    if exec_res.exit_code == 17 {
        return Err(format!(
            "file {resolved_path} already exists and `overwrite` is false"
        ));
    }
    if exec_res.exit_code != 0 {
        return Err(format!(
            "cannot write remote file {resolved_path}: {}",
            exec_res.stderr.trim()
        ));
    }
    if append {
        remote_read_file(cfg, resolved_path)
    } else {
        Ok(content.to_vec())
    }
}

fn remote_list_dir(
    cfg: &SandboxConfig,
    resolved_dir: &str,
    include_hidden: bool,
    max_results: usize,
) -> Result<serde_json::Value, String> {
    let url = format!(
        "{}/v1/sandboxes/{}/files?path={}&list=true",
        cfg.endpoint.trim_end_matches('/'),
        url_encode_query_component(&cfg.sandbox_id),
        url_encode_query_component(resolved_dir)
    );
    let agent = ureq_agent(15_000);
    let mut req = agent.get(&url);
    if let Some(ref token) = cfg.token {
        req = req.header("authorization", &format!("Bearer {token}"));
    }
    if let Ok(mut resp) = req.call() {
        let status = resp.status().as_u16();
        if (200..300).contains(&status) {
            let mut body = Vec::new();
            let _ = resp.body_mut().as_reader().read_to_end(&mut body);
            if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&body)
                && let Some(arr) = val.get("entries").and_then(|v| v.as_array())
            {
                let mut entries: Vec<serde_json::Value> = arr
                    .iter()
                    .filter(|item| {
                        include_hidden
                            || item
                                .get("name")
                                .and_then(|v| v.as_str())
                                .is_none_or(|n| !n.starts_with('.'))
                    })
                    .cloned()
                    .collect();
                let truncated = entries.len() > max_results;
                entries.truncate(max_results);
                let count = entries.len();
                return Ok(serde_json::json!({
                    "action": "list",
                    "path": resolved_dir,
                    "entries": entries,
                    "count": count,
                    "truncated": truncated,
                }));
            }
        }
    }

    // Fallback to `/exec` directory listing.
    let quoted_dir = shell_single_quote(resolved_dir);
    let cmd = format!(
        "cd {quoted_dir} && for f in .* *; do [ \"$f\" = \".\" ] || [ \"$f\" = \"..\" ] || [ ! -e \"$f\" ] && continue; if [ -L \"$f\" ]; then k=symlink; s=0; elif [ -d \"$f\" ]; then k=dir; s=0; else k=file; s=$(wc -c < \"$f\" 2>/dev/null || echo 0); fi; printf '%s\\t%s\\t%s\\n' \"$f\" \"$k\" \"$s\"; done"
    );
    let exec_res = remote_exec(cfg, &cmd, &cfg.workdir, 15_000)?;
    if exec_res.exit_code != 0 {
        return Err(format!(
            "cannot list remote directory {resolved_dir}: {}",
            exec_res.stderr.trim()
        ));
    }
    let root_prefix = format!("{}/", cfg.workdir.trim_end_matches('/'));
    let mut raw_entries: Vec<(String, serde_json::Value)> = Vec::new();
    for line in exec_res.stdout.lines() {
        let mut parts = line.splitn(3, '\t');
        let Some(name) = parts.next().map(str::trim).filter(|s| !s.is_empty()) else {
            continue;
        };
        if !include_hidden && name.starts_with('.') {
            continue;
        }
        let kind = parts.next().map(str::trim).unwrap_or("file");
        let bytes = parts
            .next()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(0);
        let full = format!("{}/{name}", resolved_dir.trim_end_matches('/'));
        let rel_path = full.strip_prefix(&root_prefix).unwrap_or(&full).to_string();
        raw_entries.push((
            name.to_string(),
            serde_json::json!({
                "name": name,
                "path": rel_path,
                "kind": kind,
                "bytes": bytes,
            }),
        ));
    }
    raw_entries.sort_by(|a, b| a.0.cmp(&b.0));
    let truncated = raw_entries.len() > max_results;
    let entries: Vec<serde_json::Value> = raw_entries
        .into_iter()
        .take(max_results)
        .map(|(_, v)| v)
        .collect();
    let count = entries.len();
    Ok(serde_json::json!({
        "action": "list",
        "path": resolved_dir,
        "entries": entries,
        "count": count,
        "truncated": truncated,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sandbox_config_loads_from_json_and_local_host_backend_executes() {
        let tmp = std::env::temp_dir().join(format!("anyagents-tools-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        let cfg_dir = tmp.join("cfg");
        let work_dir = tmp.join("work");
        std::fs::create_dir_all(&cfg_dir).unwrap();
        std::fs::create_dir_all(&work_dir).unwrap();

        assert!(SandboxConfig::from_config_dir(&cfg_dir).is_none());

        std::fs::write(
            cfg_dir.join("sandbox.json"),
            r#"{"endpoint":"https://sandbox.example.test/","sandbox_id":"sbx-1","token":"tok-1","workdir":"/workspace/"}"#,
        )
        .unwrap();

        let loaded = SandboxConfig::from_config_dir(&cfg_dir).expect("should parse sandbox.json");
        assert_eq!(loaded.endpoint, "https://sandbox.example.test");
        assert_eq!(loaded.sandbox_id, "sbx-1");
        assert_eq!(loaded.token.as_deref(), Some("tok-1"));
        assert_eq!(loaded.workdir, "/workspace");

        let local = WorkspaceBackend::LocalHost {
            root: work_dir.clone(),
        };
        let (_, written, _) = local
            .write_file_bytes(Some("sub/note.txt"), b"hello local", false, true)
            .unwrap();
        assert_eq!(written, 11);
        let (_, read_back) = local.read_file_bytes(Some("sub/note.txt")).unwrap();
        assert_eq!(read_back, b"hello local");
        let exec_res = local
            .exec("printf 'ok-%s' local", Some("sub"), Some(5_000))
            .unwrap();
        assert_eq!(exec_res.exit_code, 0);
        assert_eq!(exec_res.stdout, "ok-local");

        let _ = std::fs::remove_dir_all(&tmp);
    }
}
