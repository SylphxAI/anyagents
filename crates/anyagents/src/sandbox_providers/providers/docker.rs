//! Docker Engine CLI provider (`DockerProvider`).
//!
//! Runs any OCI image (`ImageSpec.image`), mounts a named Docker volume (or host
//! bind path) at `/home/user/agent-data`, enforces `--network none` on
//! [`NetworkPolicySpec::Deny`], streams `.tar.gz` bundles directly over `docker exec -i`
//! stdin with zero temp files, removes the container on `release()` while keeping the
//! named volume intact, and deletes both container and named volume on `destroy()`.

use std::collections::BTreeMap;
use std::io::Write as _;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::Instant;

use async_trait::async_trait;

use super::{
    EnsureSandboxRequest, ExecOutput, ExecRequest, NetworkPolicySpec, ProviderHealth, ProviderKind,
    SandboxCapabilities, SandboxHandle, SandboxProvider, persistent_layout_init_script,
    resolve_layout, run_blocking, sanitize_name, shell_single_quote,
};

const DEFAULT_DOCKER_IMAGE: &str = "ghcr.io/sylphxai/anyagents-sandbox:base";

/// Configuration for [`DockerProvider`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DockerConfig {
    /// Optional `DOCKER_HOST` override (e.g. `unix:///var/run/docker.sock` or `tcp://...`).
    pub docker_host: Option<String>,
    /// Optional explicit path to the `docker` executable (otherwise resolved via `anyagents-toolchain`).
    pub docker_executable: Option<PathBuf>,
    /// Default OCI image when [`EnsureSandboxRequest::image`] omits `image`.
    pub default_image: String,
    /// Optional custom Docker network name when egress is allowed.
    pub default_network: Option<String>,
    /// Optional container memory limit (e.g. `"4g"`).
    pub memory_limit: Option<String>,
    /// Optional container CPU limit (e.g. `"2"`).
    pub cpus: Option<String>,
}

impl Default for DockerConfig {
    fn default() -> Self {
        Self {
            docker_host: None,
            docker_executable: None,
            default_image: DEFAULT_DOCKER_IMAGE.to_string(),
            default_network: None,
            memory_limit: None,
            cpus: None,
        }
    }
}

/// First-class [`SandboxProvider`] targeting Docker Engine CLI.
#[derive(Clone, Debug, Default)]
pub struct DockerProvider {
    config: DockerConfig,
}

impl DockerProvider {
    pub fn new(mut config: DockerConfig) -> Self {
        if config.default_image.trim().is_empty() {
            config.default_image = DEFAULT_DOCKER_IMAGE.to_string();
        }
        Self { config }
    }

    pub fn config(&self) -> &DockerConfig {
        &self.config
    }

    fn build_cmd(&self) -> Result<Command, String> {
        let mut cmd = if let Some(ref exe) = self.config.docker_executable {
            Command::new(exe)
        } else {
            let resolved = anyagents_toolchain::tool("docker", "providers.docker")
                .map_err(|err| err.to_string())?;
            resolved.command()
        };
        if let Some(ref host) = self.config.docker_host {
            cmd.env("DOCKER_HOST", host);
        }
        Ok(cmd)
    }
}

#[async_trait]
impl SandboxProvider for DockerProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Docker
    }

    fn capabilities(&self) -> SandboxCapabilities {
        SandboxCapabilities {
            oci_image: true,
            templates: false,
            persistent_volumes: true,
            volume_survives_release: true,
            network_policy: true,
            live_network_mutation: false,
            tar_bundle_sync: true,
            streaming_exec: true,
            pause_resume: false,
        }
    }

    async fn ensure(&self, req: &EnsureSandboxRequest) -> Result<SandboxHandle, String> {
        let this = self.clone();
        let req = req.clone();
        run_blocking(move || {
            let (mount_path, state_dir, workdir) = resolve_layout(&req);
            let agent_slug = sanitize_name(&req.agent_id);
            let volume_spec = req
                .existing
                .as_ref()
                .and_then(|h| h.volume_id.clone())
                .or_else(|| req.volume.as_ref().map(|v| v.id_or_name.clone()))
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| format!("anyagents-vol-{agent_slug}"));
            let is_bind_mount = volume_spec.starts_with('/');

            if !is_bind_mount {
                let out = this
                    .build_cmd()?
                    .args(["volume", "create", &volume_spec])
                    .output()
                    .map_err(|err| format!("failed to run `docker volume create`: {err}"))?;
                if !out.status.success() {
                    return Err(format!(
                        "docker volume create `{volume_spec}` failed: {}",
                        String::from_utf8_lossy(&out.stderr).trim()
                    ));
                }
            }

            // Reuse existing running container if still alive.
            if let Some(ref existing) = req.existing
                && !existing.sandbox_id.trim().is_empty()
            {
                let inspect = this
                    .build_cmd()?
                    .args(["inspect", "-f", "{{.State.Running}}", &existing.sandbox_id])
                    .output();
                if let Ok(out) = inspect
                    && out.status.success()
                    && String::from_utf8_lossy(&out.stdout).trim() == "true"
                {
                    let mut h = existing.clone();
                    h.volume_id = Some(volume_spec);
                    h.mount_path = mount_path;
                    h.state_dir = state_dir;
                    h.workdir = workdir;
                    return Ok(h);
                }
            }

            let image = req
                .image
                .image
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or(&this.config.default_image);
            let container_name = format!(
                "anyagents-{}-{}",
                agent_slug,
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() % 1_000_000)
                    .unwrap_or(0)
            );
            let volume_arg = format!("{volume_spec}:{mount_path}");

            let mut run_cmd = this.build_cmd()?;
            run_cmd.args([
                "run",
                "-d",
                "--name",
                &container_name,
                "--label",
                &format!("anyagents.agent_id={}", req.agent_id),
                "--label",
                "anyagents.managed=true",
                "-v",
                &volume_arg,
                "-w",
                &workdir,
            ]);
            match &req.network {
                NetworkPolicySpec::Deny => {
                    run_cmd.args(["--network", "none"]);
                }
                NetworkPolicySpec::Allow | NetworkPolicySpec::Allowlist(_) => {
                    if let Some(ref net) = this.config.default_network {
                        run_cmd.args(["--network", net]);
                    }
                }
            }
            if let Some(ref mem) = this.config.memory_limit {
                run_cmd.args(["--memory", mem]);
            }
            if let Some(ref cpus) = this.config.cpus {
                run_cmd.args(["--cpus", cpus]);
            }
            for (k, v) in &req.image.env {
                run_cmd.arg("-e").arg(format!("{k}={v}"));
            }
            run_cmd.arg(image).args(["sleep", "infinity"]);

            let run_out = run_cmd
                .output()
                .map_err(|err| format!("failed to run `docker run`: {err}"))?;
            if !run_out.status.success() {
                return Err(format!(
                    "docker run `{image}` failed: {}",
                    String::from_utf8_lossy(&run_out.stderr).trim()
                ));
            }
            let container_id = String::from_utf8_lossy(&run_out.stdout)
                .lines()
                .next()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or(&container_name)
                .to_string();

            // Create `/home/user/agent-data/{state,workspace}` and legacy symlinks.
            let init_script = persistent_layout_init_script(&mount_path, &state_dir, &workdir);
            let _ = this
                .build_cmd()?
                .args(["exec", &container_id, "/bin/sh", "-lc", &init_script])
                .output();

            let mut metadata = BTreeMap::new();
            metadata.insert("container_name".to_string(), container_name);
            metadata.insert("image".to_string(), image.to_string());
            metadata.insert("bind_mount".to_string(), is_bind_mount.to_string());

            Ok(SandboxHandle {
                provider: ProviderKind::Docker,
                sandbox_id: container_id,
                volume_id: Some(volume_spec),
                mount_path,
                state_dir,
                workdir,
                transport_url: this.config.docker_host.clone(),
                transport_token: None,
                expires_at: None,
                metadata,
            })
        })
        .await
    }

    async fn exec(&self, handle: &SandboxHandle, req: &ExecRequest) -> Result<ExecOutput, String> {
        if req.command.is_empty() {
            return Err("Docker exec requires a non-empty command vector".to_string());
        }
        let this = self.clone();
        let container = handle.sandbox_id.clone();
        let cwd = req
            .cwd
            .clone()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| handle.workdir.clone());
        let env = req.env.clone();
        let command = req.command.clone();
        let stdin_bytes = req.stdin.clone();

        run_blocking(move || {
            let mut cmd = this.build_cmd()?;
            cmd.arg("exec");
            if stdin_bytes.is_some() {
                cmd.arg("-i");
            }
            cmd.args(["-w", &cwd]);
            for (k, v) in &env {
                cmd.arg("-e").arg(format!("{k}={v}"));
            }
            cmd.arg(&container).args(&command);
            cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
            if stdin_bytes.is_some() {
                cmd.stdin(Stdio::piped());
            } else {
                cmd.stdin(Stdio::null());
            }
            let mut child = cmd
                .spawn()
                .map_err(|err| format!("failed to spawn `docker exec`: {err}"))?;
            if let Some(bytes) = stdin_bytes
                && let Some(mut stdin) = child.stdin.take()
            {
                stdin
                    .write_all(&bytes)
                    .map_err(|err| format!("failed writing stdin to `docker exec`: {err}"))?;
            }
            let out = child
                .wait_with_output()
                .map_err(|err| format!("failed waiting on `docker exec`: {err}"))?;
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
        let this = self.clone();
        let container = handle.sandbox_id.clone();
        let dest = if dest_dir.trim().is_empty() {
            handle.state_dir.clone()
        } else {
            dest_dir.trim().to_string()
        };
        let payload = tar_gz.to_vec();

        run_blocking(move || {
            let script = format!(
                "set -e; mkdir -p {dst}; tar -xzf - -C {dst}",
                dst = shell_single_quote(&dest),
            );
            let mut child = this
                .build_cmd()?
                .args(["exec", "-i", &container, "/bin/sh", "-lc", &script])
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .map_err(|err| format!("failed to spawn `docker exec` for tar sync: {err}"))?;
            if let Some(mut stdin) = child.stdin.take() {
                stdin
                    .write_all(&payload)
                    .map_err(|err| format!("failed streaming bundle tar to docker: {err}"))?;
            }
            let out = child
                .wait_with_output()
                .map_err(|err| format!("docker tar sync wait failed: {err}"))?;
            if !out.status.success() {
                return Err(format!(
                    "docker bundle tar extraction into `{dest}` failed (exit {}): {}",
                    out.status.code().unwrap_or(-1),
                    String::from_utf8_lossy(&out.stderr).trim()
                ));
            }
            Ok(())
        })
        .await
    }

    async fn release(&self, handle: &SandboxHandle) -> Result<(), String> {
        let this = self.clone();
        let container = handle.sandbox_id.clone();
        run_blocking(move || {
            // Remove ephemeral container while keeping the persistent named volume intact.
            let out = this
                .build_cmd()?
                .args(["rm", "-f", &container])
                .output()
                .map_err(|err| format!("failed to run `docker rm -f`: {err}"))?;
            if !out.status.success() {
                let stderr = String::from_utf8_lossy(&out.stderr);
                if !stderr.to_ascii_lowercase().contains("no such container") {
                    return Err(format!(
                        "docker rm -f `{container}` failed: {}",
                        stderr.trim()
                    ));
                }
            }
            Ok(())
        })
        .await
    }

    async fn destroy(&self, handle: &SandboxHandle) -> Result<(), String> {
        let this = self.clone();
        let container = handle.sandbox_id.clone();
        let volume_id = handle.volume_id.clone();
        let is_bind = handle
            .metadata
            .get("bind_mount")
            .is_some_and(|v| v == "true")
            || volume_id.as_deref().is_some_and(|v| v.starts_with('/'));

        run_blocking(move || {
            let _ = this.build_cmd()?.args(["rm", "-f", &container]).output();
            if !is_bind && let Some(vol) = volume_id.filter(|s| !s.trim().is_empty()) {
                let out = this
                    .build_cmd()?
                    .args(["volume", "rm", "-f", &vol])
                    .output()
                    .map_err(|err| format!("failed to run `docker volume rm -f`: {err}"))?;
                if !out.status.success() {
                    let stderr = String::from_utf8_lossy(&out.stderr);
                    if !stderr.to_ascii_lowercase().contains("no such volume") {
                        return Err(format!(
                            "docker volume rm -f `{vol}` failed: {}",
                            stderr.trim()
                        ));
                    }
                }
            }
            Ok(())
        })
        .await
    }

    async fn health(&self) -> Result<ProviderHealth, String> {
        let this = self.clone();
        run_blocking(move || {
            let started = Instant::now();
            let out = this
                .build_cmd()?
                .args(["info", "--format", "{{.ServerVersion}}"])
                .output()
                .map_err(|err| format!("failed to run `docker info`: {err}"))?;
            let healthy = out.status.success();
            let version = String::from_utf8_lossy(&out.stdout).trim().to_string();
            let message = if healthy {
                format!("Docker daemon reachable (version {version})")
            } else {
                format!(
                    "Docker daemon unhealthy: {}",
                    String::from_utf8_lossy(&out.stderr).trim()
                )
            };
            Ok(ProviderHealth {
                provider: ProviderKind::Docker,
                healthy,
                message,
                latency_ms: Some(started.elapsed().as_millis() as u64),
            })
        })
        .await
    }
}
