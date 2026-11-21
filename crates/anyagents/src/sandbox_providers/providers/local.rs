//! Zero-config local host sandbox provider (`LocalHostProvider`).

use std::collections::BTreeMap;
use std::io::Write as _;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use async_trait::async_trait;

use super::{
    EnsureSandboxRequest, ExecOutput, ExecRequest, ProviderHealth, ProviderKind,
    SandboxCapabilities, SandboxHandle, SandboxProvider, extract_bundle_tar_gz, run_blocking,
    sanitize_name,
};

/// Configuration for [`LocalHostProvider`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalHostConfig {
    /// Root directory under which per-agent `agent-data/{state,workspace}` directories live.
    pub root_dir: PathBuf,
}

impl Default for LocalHostConfig {
    fn default() -> Self {
        let root_dir = std::env::var_os("ANYAGENTS_SANDBOX_LOCAL_ROOT")
            .filter(|v| !v.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| std::env::temp_dir().join("anyagents-sandboxes"));
        Self { root_dir }
    }
}

/// Zero-config local host execution provider.
///
/// Preserves `<root_dir>/<agent>/agent-data/{state,workspace}` across [`SandboxProvider::release`]
/// calls and removes it only on [`SandboxProvider::destroy`].
#[derive(Clone, Debug, Default)]
pub struct LocalHostProvider {
    config: LocalHostConfig,
}

impl LocalHostProvider {
    pub fn new(config: LocalHostConfig) -> Self {
        Self { config }
    }

    pub fn with_root(root_dir: impl Into<PathBuf>) -> Self {
        Self {
            config: LocalHostConfig {
                root_dir: root_dir.into(),
            },
        }
    }

    pub fn config(&self) -> &LocalHostConfig {
        &self.config
    }
}

#[async_trait]
impl SandboxProvider for LocalHostProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Local
    }

    fn capabilities(&self) -> SandboxCapabilities {
        SandboxCapabilities {
            oci_image: false,
            templates: false,
            persistent_volumes: true,
            volume_survives_release: true,
            network_policy: false,
            live_network_mutation: false,
            tar_bundle_sync: true,
            streaming_exec: true,
            pause_resume: false,
        }
    }

    async fn ensure(&self, req: &EnsureSandboxRequest) -> Result<SandboxHandle, String> {
        let root_dir = self.config.root_dir.clone();
        let agent_slug = sanitize_name(&req.agent_id);
        let volume_name = req
            .volume
            .as_ref()
            .map(|v| sanitize_name(&v.id_or_name))
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| format!("anyagents-{agent_slug}"));
        let persistent = req.volume.is_some();
        let explicit_workdir = req.workdir.clone();
        let existing_handle = req.existing.clone();

        run_blocking(move || {
            let agent_root = if let Some(ref existing) = existing_handle
                && let Some(prev_root) = existing.metadata.get("local_agent_root")
            {
                PathBuf::from(prev_root)
            } else {
                root_dir.join(&volume_name)
            };
            let mount_dir = agent_root.join("agent-data");
            let state_dir = mount_dir.join("state");
            let workdir = explicit_workdir
                .as_deref()
                .filter(|s| !s.trim().is_empty())
                .map(PathBuf::from)
                .unwrap_or_else(|| mount_dir.join("workspace"));

            std::fs::create_dir_all(&state_dir).map_err(|err| {
                format!(
                    "cannot create local state dir {}: {err}",
                    state_dir.display()
                )
            })?;
            std::fs::create_dir_all(&workdir).map_err(|err| {
                format!("cannot create local workdir {}: {err}", workdir.display())
            })?;

            let mut metadata = BTreeMap::new();
            metadata.insert(
                "local_agent_root".to_string(),
                agent_root.display().to_string(),
            );

            Ok(SandboxHandle {
                provider: ProviderKind::Local,
                sandbox_id: format!("local-{agent_slug}"),
                volume_id: persistent.then(|| mount_dir.display().to_string()),
                mount_path: mount_dir.display().to_string(),
                state_dir: state_dir.display().to_string(),
                workdir: workdir.display().to_string(),
                transport_url: None,
                transport_token: None,
                expires_at: None,
                metadata,
            })
        })
        .await
    }

    async fn exec(&self, handle: &SandboxHandle, req: &ExecRequest) -> Result<ExecOutput, String> {
        if req.command.is_empty() {
            return Err("local exec requires a non-empty command vector".to_string());
        }
        let command = req.command.clone();
        let cwd = req
            .cwd
            .clone()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| handle.workdir.clone());
        let env = req.env.clone();
        let stdin_bytes = req.stdin.clone();
        let timeout_ms = req.timeout_ms;

        run_blocking(move || {
            let program = &command[0];
            let args = &command[1..];
            let mut cmd = Command::new(program);
            cmd.args(args)
                .current_dir(&cwd)
                .envs(&env)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            if stdin_bytes.is_some() {
                cmd.stdin(Stdio::piped());
            } else {
                cmd.stdin(Stdio::null());
            }
            let mut child = cmd
                .spawn()
                .map_err(|err| format!("failed to spawn local command `{program}`: {err}"))?;
            if let Some(data) = stdin_bytes
                && let Some(mut child_stdin) = child.stdin.take()
            {
                child_stdin
                    .write_all(&data)
                    .map_err(|err| format!("failed writing stdin to `{program}`: {err}"))?;
            }
            let out = if let Some(limit_ms) = timeout_ms {
                wait_with_timeout(child, Duration::from_millis(limit_ms.max(10)))?
            } else {
                child
                    .wait_with_output()
                    .map_err(|err| format!("failed waiting on local command `{program}`: {err}"))?
            };
            Ok(ExecOutput {
                stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
                stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
                exit_code: out.status.code().unwrap_or(-1),
            })
        })
        .await
    }

    async fn sync_bundle_tar(
        &self,
        handle: &SandboxHandle,
        dest_dir: &str,
        tar_gz: &[u8],
    ) -> Result<(), String> {
        let dest = if dest_dir.trim().is_empty() {
            PathBuf::from(&handle.state_dir)
        } else {
            PathBuf::from(dest_dir)
        };
        let payload = tar_gz.to_vec();
        run_blocking(move || {
            let _ = extract_bundle_tar_gz(&payload, &dest)?;
            Ok(())
        })
        .await
    }

    async fn release(&self, _handle: &SandboxHandle) -> Result<(), String> {
        // Local directories preserve both `state/` and `workspace/` across release.
        Ok(())
    }

    async fn destroy(&self, handle: &SandboxHandle) -> Result<(), String> {
        let target = handle
            .metadata
            .get("local_agent_root")
            .map(PathBuf::from)
            .or_else(|| handle.volume_id.as_ref().map(PathBuf::from));
        run_blocking(move || {
            if let Some(path) = target
                && path.exists()
            {
                std::fs::remove_dir_all(&path).map_err(|err| {
                    format!("cannot remove local sandbox {}: {err}", path.display())
                })?;
            }
            Ok(())
        })
        .await
    }

    async fn health(&self) -> Result<ProviderHealth, String> {
        let root_dir = self.config.root_dir.clone();
        run_blocking(move || {
            let started = Instant::now();
            std::fs::create_dir_all(&root_dir).map_err(|err| {
                format!(
                    "local sandbox root {} is not writable: {err}",
                    root_dir.display()
                )
            })?;
            Ok(ProviderHealth {
                provider: ProviderKind::Local,
                healthy: true,
                message: format!("local root ready: {}", root_dir.display()),
                latency_ms: Some(started.elapsed().as_millis() as u64),
            })
        })
        .await
    }
}

fn wait_with_timeout(
    mut child: std::process::Child,
    timeout: Duration,
) -> Result<std::process::Output, String> {
    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => {
                return child
                    .wait_with_output()
                    .map_err(|err| format!("cannot collect local command output: {err}"));
            }
            Ok(None) => {
                if Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(format!(
                        "local command timed out after {}ms",
                        timeout.as_millis()
                    ));
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(err) => return Err(format!("failed polling local command status: {err}")),
        }
    }
}
