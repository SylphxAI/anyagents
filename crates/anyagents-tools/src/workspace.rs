use std::io::Read as _;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::encoding::{base64_decode, base64_encode, sha256_hex, url_encode_query_component};
use crate::path::resolve_workspace_path;
use crate::sandbox::{DEFAULT_EXEC_TIMEOUT_MS, SandboxConfig, ureq_agent};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecOutput {
    #[serde(default)]
    pub stdout: String,
    #[serde(default)]
    pub stderr: String,
    #[serde(default)]
    pub exit_code: i32,
}

/// Pluggable execution and filesystem substrate for an agent's workspace (`Sandbox` axis).
///
/// Orthogonal to both the `Harness` axis (the external runtime loop) and the `Profile` axis
/// (the upstream provider credential pool). Built-in implementations cover local directories /
/// git worktrees ([`LocalWorkspace`]), remote HTTP / E2B cloud sandboxes
/// ([`RemoteSandboxWorkspace`]), and Docker containers ([`DockerWorkspace`]); external crates
/// may implement this trait for custom microVMs or VFS backends.
pub trait WorkspaceBackend: Send + Sync {
    /// Canonical backend driver identifier (e.g. `"local"`, `"cloud-sandbox"`, `"e2b"`, `"docker"`).
    fn kind(&self) -> &'static str;

    /// Whether file and command operations run outside the orchestrator host filesystem.
    fn is_remote(&self) -> bool {
        self.kind() != "local"
    }

    /// Display path of the workspace root inside this backend.
    fn root_display(&self) -> String {
        self.base_path().display().to_string()
    }

    /// Root path against which relative paths are resolved.
    fn base_path(&self) -> &Path;

    /// Resolve a user-supplied relative or absolute path against [`Self::base_path`].
    fn resolve_path(&self, raw: Option<&str>, required: bool) -> Result<String, String> {
        resolve_workspace_path(self.base_path(), raw, required)
    }

    /// Execute a shell command inside the workspace.
    fn exec(
        &self,
        command: &str,
        cwd: Option<&str>,
        timeout_ms: Option<u64>,
    ) -> Result<ExecOutput, String>;

    /// Read raw file bytes from the workspace, returning `(resolved_path, bytes)`.
    fn read_file_bytes(&self, path: Option<&str>) -> Result<(String, Vec<u8>), String>;

    /// Write or append file bytes in the workspace, returning `(resolved_path, total_bytes, sha256_hex)`.
    fn write_file_bytes(
        &self,
        path: Option<&str>,
        content: &[u8],
        append: bool,
        overwrite: bool,
    ) -> Result<(String, usize, String), String>;

    /// List entries in a directory inside the workspace.
    fn list_dir(
        &self,
        path: Option<&str>,
        include_hidden: bool,
        max_results: usize,
    ) -> Result<serde_json::Value, String>;
}

impl dyn WorkspaceBackend {
    /// Resolve the active workspace backend from `<config_dir>/sandbox.json` (or `ANYAGENTS_SANDBOX_*`),
    /// defaulting to [`LocalWorkspace`] rooted at `local_root`.
    pub fn resolve(config_dir: Option<&Path>, local_root: PathBuf) -> Box<dyn WorkspaceBackend> {
        resolve_workspace_backend(config_dir, local_root)
    }
}

/// Resolve the active [`WorkspaceBackend`] for an agent:
/// - `docker://<host>` endpoint in `sandbox.json` -> [`DockerWorkspace`]
/// - HTTP / E2B `sandbox.json` -> [`RemoteSandboxWorkspace`]
/// - Otherwise -> [`LocalWorkspace`] (local directory or git worktree)
pub fn resolve_workspace_backend(
    config_dir: Option<&Path>,
    local_root: PathBuf,
) -> Box<dyn WorkspaceBackend> {
    if let Some(cfg) = SandboxConfig::resolve(config_dir) {
        if let Some(rest) = cfg.endpoint.strip_prefix("docker://") {
            let docker_host = (!rest.is_empty() && rest != "local").then(|| rest.to_string());
            return Box::new(DockerWorkspace {
                container: cfg.sandbox_id,
                workdir: PathBuf::from(cfg.workdir),
                docker_host,
            });
        }
        return Box::new(RemoteSandboxWorkspace::new(cfg));
    }
    Box::new(LocalWorkspace::new(local_root))
}

/// The environment a local `workspace.exec` command runs in.
///
/// `Inherit` keeps today's behaviour (the server's environment). `Exact` is the
/// agent's materialized environment (credentials and the toolchain `PATH`),
/// which replaces the server's environment completely (ADR 0004 I12).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum ExecEnv {
    #[default]
    Inherit,
    Exact(Vec<(std::ffi::OsString, std::ffi::OsString)>),
}

impl ExecEnv {
    fn apply(&self, cmd: &mut std::process::Command) {
        if let ExecEnv::Exact(vars) = self {
            cmd.env_clear().envs(vars.iter().map(|(k, v)| (k, v)));
        }
    }
}

/// Local directory or git worktree workspace on the orchestrator host (default `Sandbox` backend).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalWorkspace {
    pub root: PathBuf,
    pub env: ExecEnv,
}

impl LocalWorkspace {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            env: ExecEnv::Inherit,
        }
    }

    /// Run commands in `env` instead of the server's environment.
    pub fn with_env(mut self, env: ExecEnv) -> Self {
        self.env = env;
        self
    }
}

impl WorkspaceBackend for LocalWorkspace {
    fn kind(&self) -> &'static str {
        "local"
    }

    fn base_path(&self) -> &Path {
        self.root.as_path()
    }

    fn exec(
        &self,
        command: &str,
        cwd: Option<&str>,
        _timeout_ms: Option<u64>,
    ) -> Result<ExecOutput, String> {
        let cmd_trimmed = command.trim();
        if cmd_trimmed.is_empty() {
            return Err("`workspace.exec` requires a non-empty `cmd`".to_string());
        }
        let resolved_cwd = self.resolve_path(cwd, false)?;
        let cwd_path = PathBuf::from(&resolved_cwd);
        if !cwd_path.is_dir() {
            return Err(format!(
                "exec working directory {} does not exist",
                cwd_path.display()
            ));
        }
        let mut cmd = std::process::Command::new("/bin/sh");
        cmd.arg("-c").arg(cmd_trimmed).current_dir(&cwd_path);
        self.env.apply(&mut cmd);
        let output = cmd
            .output()
            .map_err(|e| format!("failed to spawn command: {e}"))?;
        Ok(ExecOutput {
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            exit_code: output.status.code().unwrap_or(-1),
        })
    }

    fn read_file_bytes(&self, path: Option<&str>) -> Result<(String, Vec<u8>), String> {
        let resolved = self.resolve_path(path, true)?;
        let p = Path::new(&resolved);
        let meta = std::fs::metadata(p).map_err(|e| format!("cannot read {resolved}: {e}"))?;
        if meta.is_dir() {
            return Err(format!(
                "{resolved} is a directory; use `action: \"list\"` or `action: \"glob\"` to list files"
            ));
        }
        let bytes = std::fs::read(p).map_err(|e| format!("cannot read {resolved}: {e}"))?;
        Ok((resolved, bytes))
    }

    fn write_file_bytes(
        &self,
        path: Option<&str>,
        content: &[u8],
        append: bool,
        overwrite: bool,
    ) -> Result<(String, usize, String), String> {
        let resolved = self.resolve_path(path, true)?;
        let p = Path::new(&resolved);
        if !append && !overwrite && p.exists() {
            return Err(format!(
                "file {resolved} already exists and `overwrite` is false"
            ));
        }
        if let Some(parent) = p.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("cannot create parent directory {}: {e}", parent.display()))?;
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
            std::fs::write(p, content).map_err(|e| format!("cannot write {resolved}: {e}"))?;
        }
        let final_bytes =
            std::fs::read(p).map_err(|e| format!("cannot read back {resolved}: {e}"))?;
        let sha = sha256_hex(&final_bytes);
        Ok((resolved, final_bytes.len(), sha))
    }

    fn list_dir(
        &self,
        path: Option<&str>,
        include_hidden: bool,
        max_results: usize,
    ) -> Result<serde_json::Value, String> {
        let resolved = self.resolve_path(path, false)?;
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
                .strip_prefix(&self.root)
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
}

/// Remote HTTP / Cloud Sandbox / E2B workspace backend driven by [`SandboxConfig`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteSandboxWorkspace {
    pub config: SandboxConfig,
}

impl RemoteSandboxWorkspace {
    pub fn new(mut config: SandboxConfig) -> Self {
        if let Some(rest) = config.endpoint.strip_prefix("e2b://") {
            config.endpoint = format!("https://{rest}");
        }
        Self { config }
    }

    pub fn is_e2b(&self) -> bool {
        let lower = self.config.endpoint.to_ascii_lowercase();
        lower.contains(".e2b.") || lower.contains("//e2b.")
    }
}

impl WorkspaceBackend for RemoteSandboxWorkspace {
    fn kind(&self) -> &'static str {
        if self.is_e2b() {
            "e2b"
        } else {
            "cloud-sandbox"
        }
    }

    fn root_display(&self) -> String {
        self.config.workdir.clone()
    }

    fn base_path(&self) -> &Path {
        Path::new(&self.config.workdir)
    }

    fn exec(
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
        remote_exec(&self.config, cmd_trimmed, &resolved_cwd, effective_timeout)
    }

    fn read_file_bytes(&self, path: Option<&str>) -> Result<(String, Vec<u8>), String> {
        let resolved = self.resolve_path(path, true)?;
        let bytes = remote_read_file(&self.config, &resolved)?;
        Ok((resolved, bytes))
    }

    fn write_file_bytes(
        &self,
        path: Option<&str>,
        content: &[u8],
        append: bool,
        overwrite: bool,
    ) -> Result<(String, usize, String), String> {
        let resolved = self.resolve_path(path, true)?;
        let final_bytes = remote_write_file(&self.config, &resolved, content, append, overwrite)?;
        let sha = sha256_hex(&final_bytes);
        Ok((resolved, final_bytes.len(), sha))
    }

    fn list_dir(
        &self,
        path: Option<&str>,
        include_hidden: bool,
        max_results: usize,
    ) -> Result<serde_json::Value, String> {
        let resolved = self.resolve_path(path, false)?;
        remote_list_dir(&self.config, &resolved, include_hidden, max_results)
    }
}

/// Containerized Docker workspace backend (`docker exec` against a running container).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DockerWorkspace {
    pub container: String,
    pub workdir: PathBuf,
    pub docker_host: Option<String>,
}

impl DockerWorkspace {
    pub fn new(container: impl Into<String>, workdir: impl Into<PathBuf>) -> Self {
        Self {
            container: container.into(),
            workdir: workdir.into(),
            docker_host: None,
        }
    }

    fn docker_cmd(&self) -> Result<std::process::Command, String> {
        let docker = anyagents_toolchain::tool("docker", "workspace.docker")
            .map_err(|err| err.to_string())?;
        let mut cmd = docker.command();
        if let Some(ref host) = self.docker_host {
            cmd.env("DOCKER_HOST", host);
        }
        Ok(cmd)
    }
}

impl WorkspaceBackend for DockerWorkspace {
    fn kind(&self) -> &'static str {
        "docker"
    }

    fn base_path(&self) -> &Path {
        self.workdir.as_path()
    }

    fn exec(
        &self,
        command: &str,
        cwd: Option<&str>,
        _timeout_ms: Option<u64>,
    ) -> Result<ExecOutput, String> {
        let cmd_trimmed = command.trim();
        if cmd_trimmed.is_empty() {
            return Err("`workspace.exec` requires a non-empty `cmd`".to_string());
        }
        let resolved_cwd = self.resolve_path(cwd, false)?;
        let output = self
            .docker_cmd()?
            .args([
                "exec",
                "-w",
                &resolved_cwd,
                &self.container,
                "/bin/sh",
                "-c",
                cmd_trimmed,
            ])
            .output()
            .map_err(|e| format!("failed to spawn docker exec: {e}"))?;
        Ok(ExecOutput {
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            exit_code: output.status.code().unwrap_or(-1),
        })
    }

    fn read_file_bytes(&self, path: Option<&str>) -> Result<(String, Vec<u8>), String> {
        let resolved = self.resolve_path(path, true)?;
        let output = self
            .docker_cmd()?
            .args([
                "exec",
                &self.container,
                "/bin/sh",
                "-c",
                r#"test ! -d "$1" && cat -- "$1""#,
                "sh",
                &resolved,
            ])
            .output()
            .map_err(|e| format!("cannot read {resolved} from docker container: {e}"))?;
        if !output.status.success() {
            return Err(format!(
                "cannot read {resolved} in docker container ({}): {}",
                self.container,
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }
        Ok((resolved, output.stdout))
    }

    fn write_file_bytes(
        &self,
        path: Option<&str>,
        content: &[u8],
        append: bool,
        overwrite: bool,
    ) -> Result<(String, usize, String), String> {
        use std::io::Write as _;
        use std::process::Stdio;
        let resolved = self.resolve_path(path, true)?;
        let script = if append {
            r#"mkdir -p -- "$(dirname -- "$1")" && cat >> "$1""#
        } else if overwrite {
            r#"mkdir -p -- "$(dirname -- "$1")" && cat > "$1""#
        } else {
            r#"test ! -e "$1" && mkdir -p -- "$(dirname -- "$1")" && cat > "$1""#
        };
        let mut child = self
            .docker_cmd()?
            .args([
                "exec",
                "-i",
                &self.container,
                "/bin/sh",
                "-c",
                script,
                "sh",
                &resolved,
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| format!("cannot write {resolved} in docker container: {e}"))?;
        if let Some(mut stdin) = child.stdin.take() {
            stdin
                .write_all(content)
                .map_err(|e| format!("cannot stream bytes to docker container: {e}"))?;
        }
        let out = child
            .wait_with_output()
            .map_err(|e| format!("docker write wait failed: {e}"))?;
        if !out.status.success() {
            return Err(format!(
                "cannot write {resolved} in docker container ({}): {}",
                self.container,
                String::from_utf8_lossy(&out.stderr).trim()
            ));
        }
        let (_, final_bytes) = self.read_file_bytes(Some(&resolved))?;
        let sha = sha256_hex(&final_bytes);
        Ok((resolved, final_bytes.len(), sha))
    }

    fn list_dir(
        &self,
        path: Option<&str>,
        include_hidden: bool,
        max_results: usize,
    ) -> Result<serde_json::Value, String> {
        let resolved = self.resolve_path(path, false)?;
        let flag = if include_hidden { "-1Ap" } else { "-1p" };
        let out = self.exec(
            &format!("ls {flag} -- {}", shell_single_quote(&resolved)),
            None,
            None,
        )?;
        if out.exit_code != 0 {
            return Err(format!(
                "cannot list directory {resolved}: {}",
                out.stderr.trim()
            ));
        }
        let mut raw_entries: Vec<serde_json::Value> = out
            .stdout
            .lines()
            .filter(|l| !l.is_empty())
            .map(|line| {
                let (name, kind) = match line.strip_suffix('/') {
                    Some(dir_name) => (dir_name, "dir"),
                    None => (line, "file"),
                };
                serde_json::json!({
                    "name": name,
                    "path": format!("{resolved}/{name}"),
                    "kind": kind,
                    "bytes": 0,
                })
            })
            .collect();
        let truncated = raw_entries.len() > max_results;
        raw_entries.truncate(max_results);
        let count = raw_entries.len();
        Ok(serde_json::json!({
            "action": "list",
            "path": resolved,
            "entries": raw_entries,
            "count": count,
            "truncated": truncated,
        }))
    }
}

fn shell_single_quote(raw: &str) -> String {
    format!("'{}'", raw.replace('\'', "'\"'\"'"))
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

#[cfg(all(test, unix))]
mod exec_env_tests {
    use super::*;

    #[test]
    fn an_exact_environment_replaces_the_servers() {
        let dir = tempfile::tempdir().unwrap();
        let ws = LocalWorkspace::new(dir.path()).with_env(ExecEnv::Exact(vec![
            ("PATH".into(), "/usr/bin:/bin".into()),
            ("EXAMPLE_ONLY".into(), "materialized".into()),
        ]));
        let out = ws
            .exec("printf '%s|%s' \"$EXAMPLE_ONLY\" \"${HOME-unset}\"", None, None)
            .unwrap();
        assert_eq!(out.stdout, "materialized|unset");
    }

    #[test]
    fn the_default_environment_is_inherited() {
        let dir = tempfile::tempdir().unwrap();
        let out = LocalWorkspace::new(dir.path())
            .exec("printf '%s' \"${PATH:+set}\"", None, None)
            .unwrap();
        assert_eq!(out.stdout, "set");
    }
}
