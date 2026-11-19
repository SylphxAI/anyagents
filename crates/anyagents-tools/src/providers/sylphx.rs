//! Native Sylphx Sandboxes v1 provider (`SylphxProvider`).
//!
//! Supports BOTH digest-pinned OCI images (`ImageSpec.image`) AND built-in templates
//! (`ImageSpec.template`), mounts a single persistent volume at `/home/user/agent-data`
//! containing `{state,workspace}` with backwards-compatible symlinks, syncs bundles via
//! a single `.tar.gz` archive upload + extract, supports live `:setNetwork` egress policy
//! mutation, and decouples compute leases (default 5m warm idle / 15m safety TTL) from
//! persistent volume lifetimes.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use async_trait::async_trait;

use crate::encoding::base64_encode;
use crate::sylphx::{DEFAULT_SHAPE, DEFAULT_TEMPLATE, SylphxSandboxClient};

use super::{
    DEFAULT_MOUNT_PATH, EnsureSandboxRequest, ExecOutput, ExecRequest, LeasePolicy,
    NetworkPolicySpec, ProviderHealth, ProviderKind, SandboxCapabilities, SandboxHandle,
    SandboxProvider, http_call_json, normalize_unix_dir, persistent_layout_init_script,
    resolve_layout, run_blocking, sanitize_name, shell_single_quote,
};

const DEFAULT_SYLPHX_ENDPOINT: &str = "https://api.sylphx.com";
const LEASE_READY_TIMEOUT: Duration = Duration::from_secs(120);

/// Configuration for [`SylphxProvider`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SylphxConfig {
    pub endpoint: String,
    pub token: Option<String>,
    pub parent: String,
    pub default_image: Option<String>,
    pub default_template: String,
    pub default_shape: String,
    pub default_volume_size_gib: Option<u32>,
    pub default_lease: LeasePolicy,
}

impl SylphxConfig {
    pub fn new(parent: impl Into<String>) -> Self {
        Self {
            endpoint: DEFAULT_SYLPHX_ENDPOINT.to_string(),
            token: None,
            parent: parent.into().trim().trim_matches('/').to_string(),
            default_image: None,
            default_template: DEFAULT_TEMPLATE.to_string(),
            default_shape: DEFAULT_SHAPE.to_string(),
            default_volume_size_gib: None,
            default_lease: LeasePolicy::default(),
        }
    }
}

/// First-class [`SandboxProvider`] for Sylphx Sandboxes v1.
#[derive(Clone, Debug)]
pub struct SylphxProvider {
    config: SylphxConfig,
}

impl SylphxProvider {
    pub fn new(mut config: SylphxConfig) -> Result<Self, String> {
        config.parent = config.parent.trim().trim_matches('/').to_string();
        if config.parent.is_empty() {
            return Err(
                "SylphxProvider requires `parent` (`orgs/{o}/projects/{p}/envs/{e}`)".to_string(),
            );
        }
        config.endpoint = config
            .endpoint
            .trim()
            .trim_end_matches('/')
            .trim_end_matches("/v1")
            .to_string();
        if config.endpoint.is_empty() {
            config.endpoint = DEFAULT_SYLPHX_ENDPOINT.to_string();
        }
        Ok(Self { config })
    }

    pub fn config(&self) -> &SylphxConfig {
        &self.config
    }

    fn client(&self) -> SylphxSandboxClient {
        SylphxSandboxClient::new(self.config.endpoint.clone(), self.config.token.clone())
    }
}

#[async_trait]
impl SandboxProvider for SylphxProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Sylphx
    }

    fn capabilities(&self) -> SandboxCapabilities {
        SandboxCapabilities {
            oci_image: true,
            templates: true,
            persistent_volumes: true,
            volume_survives_release: true,
            network_policy: true,
            live_network_mutation: true,
            tar_bundle_sync: true,
            streaming_exec: true,
            pause_resume: false,
        }
    }

    async fn ensure(&self, req: &EnsureSandboxRequest) -> Result<SandboxHandle, String> {
        let cfg = self.config.clone();
        let req = req.clone();
        let client = self.client();

        run_blocking(move || {
            let (mount_path, state_dir, workdir) = resolve_layout(&req);
            let preferred_vol_name = req
                .volume
                .as_ref()
                .map(|v| v.id_or_name.trim().to_string())
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| format!("anyagents-{}", sanitize_name(&req.agent_id)));
            let existing_vol = req.existing.as_ref().and_then(|h| {
                h.metadata
                    .get("volume_resource")
                    .cloned()
                    .or_else(|| h.volume_id.clone())
            });
            let size_gib = req
                .volume
                .as_ref()
                .and_then(|v| v.size_gib)
                .or(cfg.default_volume_size_gib)
                .map(u64::from);

            let volume = client.ensure_volume(
                &cfg.parent,
                existing_vol.as_deref(),
                &preferred_vol_name,
                size_gib,
            )?;

            let ttl_str = req.lease.ttl_duration_string();
            let mut reused_lease = None;
            if let Some(prev_ref) = req.existing.as_ref().and_then(|h| {
                h.metadata
                    .get("lease_resource")
                    .map(String::as_str)
                    .or_else(|| (!h.sandbox_id.is_empty()).then_some(h.sandbox_id.as_str()))
            }) && let Some(fetched) = client.get_lease(&cfg.parent, prev_ref)?
                && !fetched.is_terminal()
            {
                reused_lease =
                    Some(client.wait_lease_ready(&cfg.parent, fetched, LEASE_READY_TIMEOUT)?);
            }

            let (lease, freshly_created) = match reused_lease {
                Some(active) => {
                    let renewed = client
                        .renew_lease(&cfg.parent, &active.resource_name, Some(&ttl_str))
                        .unwrap_or(active);
                    (renewed, false)
                }
                None => {
                    let image = req
                        .image
                        .image
                        .as_deref()
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .or_else(|| {
                            cfg.default_image
                                .as_deref()
                                .map(str::trim)
                                .filter(|s| !s.is_empty())
                        });
                    let template = req
                        .image
                        .template
                        .as_deref()
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .unwrap_or(&cfg.default_template);
                    let shape = req
                        .shape
                        .as_deref()
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .unwrap_or(&cfg.default_shape);

                    let created = client.create_lease_with_options(
                        &cfg.parent,
                        &volume.resource_name,
                        &mount_path,
                        image,
                        Some(template),
                        shape,
                        &req.image.env,
                        Some(&ttl_str),
                        Some(&req.network),
                    )?;
                    let ready =
                        client.wait_lease_ready(&cfg.parent, created, LEASE_READY_TIMEOUT)?;
                    (ready, true)
                }
            };

            let data_plane_resource = lease
                .data_plane_resource
                .clone()
                .unwrap_or_else(|| lease.resource_name.clone());

            if freshly_created {
                let script = persistent_layout_init_script(&mount_path, &state_dir, &workdir);
                let _ = client.exec(
                    &data_plane_resource,
                    &["/bin/sh".to_string(), "-lc".to_string(), script],
                    Some("/"),
                    &BTreeMap::new(),
                    Some(20_000),
                );
            }

            let transport_url = lease.envd_url.clone().or_else(|| {
                Some(format!(
                    "{}/v1/{}/envd",
                    cfg.endpoint.trim_end_matches('/'),
                    data_plane_resource.trim_matches('/')
                ))
            });

            let mut metadata = BTreeMap::new();
            metadata.insert("parent".to_string(), cfg.parent.clone());
            metadata.insert("volume_resource".to_string(), volume.resource_name.clone());
            metadata.insert("lease_resource".to_string(), lease.resource_name.clone());
            metadata.insert("data_plane_resource".to_string(), data_plane_resource);
            metadata.insert("idle_ttl_ms".to_string(), req.lease.idle_ttl_ms.to_string());
            metadata.insert(
                "max_lease_ttl_ms".to_string(),
                req.lease.max_lease_ttl_ms.to_string(),
            );

            Ok(SandboxHandle {
                provider: ProviderKind::Sylphx,
                sandbox_id: lease.id,
                volume_id: Some(volume.id),
                mount_path: normalize_unix_dir(&mount_path, DEFAULT_MOUNT_PATH),
                state_dir,
                workdir,
                transport_url,
                transport_token: lease.envd_token,
                expires_at: lease.expires_at,
                metadata,
            })
        })
        .await
    }

    async fn exec(&self, handle: &SandboxHandle, req: &ExecRequest) -> Result<ExecOutput, String> {
        if req.command.is_empty() {
            return Err("Sylphx exec requires a non-empty command vector".to_string());
        }
        let client = self.client();
        let resource = resolve_data_plane_resource(&self.config.parent, handle);
        let cwd = req
            .cwd
            .clone()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| handle.workdir.clone());
        let env = req.env.clone();
        let timeout_ms = req.timeout_ms;
        let command = if let Some(ref stdin_bytes) = req.stdin {
            let b64 = base64_encode(stdin_bytes);
            let inner = req
                .command
                .iter()
                .map(|a| shell_single_quote(a))
                .collect::<Vec<_>>()
                .join(" ");
            vec![
                "/bin/sh".to_string(),
                "-lc".to_string(),
                format!("printf '%s' '{b64}' | base64 -d | {inner}"),
            ]
        } else {
            req.command.clone()
        };

        run_blocking(move || client.exec(&resource, &command, Some(&cwd), &env, timeout_ms)).await
    }

    async fn sync_bundle_tar(
        &self,
        handle: &SandboxHandle,
        dest_dir: &str,
        tar_gz: &[u8],
    ) -> Result<(), String> {
        let client = self.client();
        let resource = resolve_data_plane_resource(&self.config.parent, handle);
        let dest = if dest_dir.trim().is_empty() {
            handle.state_dir.clone()
        } else {
            dest_dir.trim().to_string()
        };
        let payload = tar_gz.to_vec();

        run_blocking(move || {
            let tmp_archive = format!("/tmp/.anyagents-bundle-{}.tar.gz", std::process::id());
            client.write_file(&resource, &tmp_archive, &payload)?;
            let script = format!(
                "set -e; mkdir -p {dst}; tar -xzf {tmp} -C {dst}; rm -f {tmp}",
                dst = shell_single_quote(&dest),
                tmp = shell_single_quote(&tmp_archive),
            );
            let out = client.exec(
                &resource,
                &["/bin/sh".to_string(), "-lc".to_string(), script],
                Some("/"),
                &BTreeMap::new(),
                Some(45_000),
            )?;
            if out.exit_code != 0 {
                return Err(format!(
                    "Sylphx bundle tar extraction into `{dest}` failed (exit {}): {}",
                    out.exit_code,
                    out.stderr.trim()
                ));
            }
            Ok(())
        })
        .await
    }

    async fn set_network(
        &self,
        handle: &SandboxHandle,
        policy: &NetworkPolicySpec,
    ) -> Result<(), String> {
        let client = self.client();
        let parent = self.config.parent.clone();
        let lease_ref = resolve_lease_resource(&parent, handle);
        let policy = policy.clone();
        run_blocking(move || client.set_network(&parent, &lease_ref, &policy)).await
    }

    async fn release(&self, handle: &SandboxHandle) -> Result<(), String> {
        let client = self.client();
        let parent = self.config.parent.clone();
        let lease_ref = resolve_lease_resource(&parent, handle);
        run_blocking(move || client.release_lease(&parent, &lease_ref)).await
    }

    async fn destroy(&self, handle: &SandboxHandle) -> Result<(), String> {
        let client = self.client();
        let parent = self.config.parent.clone();
        let lease_ref = resolve_lease_resource(&parent, handle);
        let volume_ref = handle
            .metadata
            .get("volume_resource")
            .cloned()
            .or_else(|| handle.volume_id.clone());

        run_blocking(move || {
            if !lease_ref.is_empty() {
                let _ = client.release_lease(&parent, &lease_ref);
            }
            if let Some(vol) = volume_ref.filter(|s| !s.trim().is_empty()) {
                client.delete_volume(&parent, &vol)?;
            }
            Ok(())
        })
        .await
    }

    async fn health(&self) -> Result<ProviderHealth, String> {
        let endpoint = self.config.endpoint.clone();
        let parent = self.config.parent.clone();
        let token = self.config.token.clone();

        run_blocking(move || {
            let started = Instant::now();
            let mut headers = Vec::new();
            if let Some(ref tok) = token {
                headers.push(("authorization".to_string(), format!("Bearer {tok}")));
            }
            let shapes_url = format!("{endpoint}/v1/{parent}/shapes");
            let (status, body) = http_call_json("GET", &shapes_url, &headers, None, 10_000)?;
            let healthy = (200..300).contains(&status);
            Ok(ProviderHealth {
                provider: ProviderKind::Sylphx,
                healthy,
                message: if healthy {
                    "Sylphx Sandboxes API reachable".to_string()
                } else {
                    format!("Sylphx API returned HTTP {status}: {body}")
                },
                latency_ms: Some(started.elapsed().as_millis() as u64),
            })
        })
        .await
    }
}

fn resolve_lease_resource(parent: &str, handle: &SandboxHandle) -> String {
    handle
        .metadata
        .get("lease_resource")
        .cloned()
        .unwrap_or_else(|| {
            if handle.sandbox_id.contains('/') {
                handle.sandbox_id.clone()
            } else {
                format!("{parent}/leases/{}", handle.sandbox_id)
            }
        })
}

fn resolve_data_plane_resource(parent: &str, handle: &SandboxHandle) -> String {
    handle
        .metadata
        .get("data_plane_resource")
        .cloned()
        .unwrap_or_else(|| resolve_lease_resource(parent, handle))
}
