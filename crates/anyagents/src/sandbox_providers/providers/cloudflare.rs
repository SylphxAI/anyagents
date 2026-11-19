//! Cloudflare Sandbox Worker HTTP provider (`CloudflareProvider`).
//!
//! Targets a Cloudflare Containers / `@cloudflare/sandbox` Worker HTTP gateway:
//! - `POST /v1/sandboxes` (ensure/wake Durable Object + Container with `/home/user/agent-data`)
//! - `POST /v1/sandboxes/{id}/exec`
//! - `PUT  /v1/sandboxes/{id}/fs/tar?dest=...` (raw `.tar.gz` bundle sync, with exec fallback)
//! - `POST /v1/sandboxes/{id}/sleep` (`release()`, preserving persistent state)
//! - `DELETE /v1/sandboxes/{id}` (`destroy()`)
//! - `GET /health`

use std::collections::BTreeMap;
use std::time::Instant;

use async_trait::async_trait;

use super::super::url_encode_query_component;
use agent_tools::base64_encode;

use super::{
    EnsureSandboxRequest, ExecOutput, ExecRequest, ProviderHealth, ProviderKind,
    SandboxCapabilities, SandboxHandle, SandboxProvider, http_call_bytes, http_call_json,
    persistent_layout_init_script, resolve_layout, run_blocking, sanitize_name, shell_single_quote,
};

/// Configuration for [`CloudflareProvider`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloudflareConfig {
    /// Base URL of the Cloudflare Sandbox Worker (e.g. `https://sandbox.example.workers.dev`).
    pub worker_url: String,
    /// Optional Bearer API token protecting the Worker endpoint.
    pub api_token: Option<String>,
    /// Optional Cloudflare account identifier forwarded via `X-Cloudflare-Account-Id`.
    pub account_id: Option<String>,
    /// Optional default OCI image reference.
    pub default_image: Option<String>,
}

impl CloudflareConfig {
    pub fn new(worker_url: impl Into<String>) -> Self {
        Self {
            worker_url: worker_url.into().trim().trim_end_matches('/').to_string(),
            api_token: None,
            account_id: None,
            default_image: None,
        }
    }
}

/// First-class [`SandboxProvider`] targeting a Cloudflare Sandbox Worker.
#[derive(Clone, Debug)]
pub struct CloudflareProvider {
    config: CloudflareConfig,
}

impl CloudflareProvider {
    pub fn new(mut config: CloudflareConfig) -> Result<Self, String> {
        config.worker_url = config.worker_url.trim().trim_end_matches('/').to_string();
        if config.worker_url.is_empty() {
            return Err(
                "CloudflareProvider requires `worker_url` (`ANYAGENTS_CLOUDFLARE_SANDBOX_URL`)"
                    .to_string(),
            );
        }
        Ok(Self { config })
    }

    pub fn config(&self) -> &CloudflareConfig {
        &self.config
    }

    fn auth_headers(&self) -> Vec<(String, String)> {
        let mut h = Vec::new();
        if let Some(ref tok) = self.config.api_token {
            h.push(("authorization".to_string(), format!("Bearer {tok}")));
        }
        if let Some(ref acct) = self.config.account_id {
            h.push(("x-cloudflare-account-id".to_string(), acct.clone()));
        }
        h
    }
}

#[async_trait]
impl SandboxProvider for CloudflareProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Cloudflare
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
            pause_resume: true,
        }
    }

    async fn ensure(&self, req: &EnsureSandboxRequest) -> Result<SandboxHandle, String> {
        let this = self.clone();
        let req = req.clone();
        run_blocking(move || {
            let (mount_path, state_dir, workdir) = resolve_layout(&req);
            let headers = this.auth_headers();
            let preferred_id = req
                .existing
                .as_ref()
                .map(|h| h.sandbox_id.clone())
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| format!("anyagents-{}", sanitize_name(&req.agent_id)));
            let vol_name = req
                .volume
                .as_ref()
                .map(|v| v.id_or_name.clone())
                .unwrap_or_else(|| preferred_id.clone());

            let image = req
                .image
                .image
                .as_deref()
                .or(this.config.default_image.as_deref())
                .map(str::trim)
                .filter(|s| !s.is_empty());

            let mut payload = serde_json::Map::new();
            payload.insert("id".into(), serde_json::json!(preferred_id));
            payload.insert("agent_id".into(), serde_json::json!(req.agent_id));
            payload.insert("volume_id".into(), serde_json::json!(vol_name));
            payload.insert("mount_path".into(), serde_json::json!(mount_path));
            payload.insert("workdir".into(), serde_json::json!(workdir));
            payload.insert(
                "sleep_after_ms".into(),
                serde_json::json!(req.lease.idle_ttl_ms),
            );
            payload.insert(
                "max_lease_ttl_ms".into(),
                serde_json::json!(req.lease.max_lease_ttl_ms),
            );
            payload.insert("env".into(), serde_json::json!(req.image.env));
            payload.insert("network".into(), serde_json::json!(req.network));
            if let Some(img) = image {
                payload.insert("image".into(), serde_json::json!(img));
            }

            let url = format!("{}/v1/sandboxes", this.config.worker_url);
            let (status, val) = http_call_json(
                "POST",
                &url,
                &headers,
                Some(&serde_json::Value::Object(payload)),
                30_000,
            )?;
            if !(200..300).contains(&status) {
                return Err(format!(
                    "Cloudflare Sandbox ensure `{url}` returned HTTP {status}: {val}"
                ));
            }
            let sandbox_id = val
                .get("id")
                .or_else(|| val.get("sandbox_id"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
                .unwrap_or(preferred_id);
            let volume_id = val
                .get("volume_id")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
                .or(Some(vol_name));

            let init_script = persistent_layout_init_script(&mount_path, &state_dir, &workdir);
            let _ = cf_exec(
                &this.config.worker_url,
                &headers,
                &sandbox_id,
                &["/bin/sh".to_string(), "-lc".to_string(), init_script],
                Some("/"),
                &BTreeMap::new(),
                Some(20_000),
                None,
            );

            Ok(SandboxHandle {
                provider: ProviderKind::Cloudflare,
                sandbox_id,
                volume_id,
                mount_path,
                state_dir,
                workdir,
                transport_url: Some(this.config.worker_url.clone()),
                transport_token: None,
                expires_at: val
                    .get("expires_at")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string),
                metadata: BTreeMap::new(),
            })
        })
        .await
    }

    async fn exec(&self, handle: &SandboxHandle, req: &ExecRequest) -> Result<ExecOutput, String> {
        if req.command.is_empty() {
            return Err("Cloudflare exec requires a non-empty command vector".to_string());
        }
        let worker_url = self.config.worker_url.clone();
        let headers = self.auth_headers();
        let sandbox_id = handle.sandbox_id.clone();
        let command = req.command.clone();
        let cwd = req
            .cwd
            .clone()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| handle.workdir.clone());
        let env = req.env.clone();
        let timeout_ms = req.timeout_ms;
        let stdin = req.stdin.clone();

        run_blocking(move || {
            cf_exec(
                &worker_url,
                &headers,
                &sandbox_id,
                &command,
                Some(&cwd),
                &env,
                timeout_ms,
                stdin.as_deref(),
            )
        })
        .await
    }

    async fn sync_bundle_tar(
        &self,
        handle: &SandboxHandle,
        dest_dir: &str,
        tar_gz: &[u8],
    ) -> Result<(), String> {
        let worker_url = self.config.worker_url.clone();
        let headers = self.auth_headers();
        let sandbox_id = handle.sandbox_id.clone();
        let dest = if dest_dir.trim().is_empty() {
            handle.state_dir.clone()
        } else {
            dest_dir.trim().to_string()
        };
        let payload = tar_gz.to_vec();

        run_blocking(move || {
            let tar_url = format!(
                "{worker_url}/v1/sandboxes/{}/fs/tar?dest={}",
                url_encode_query_component(&sandbox_id),
                url_encode_query_component(&dest)
            );
            let mut tar_headers = headers.clone();
            tar_headers.push(("content-type".to_string(), "application/gzip".to_string()));
            let (status, body) =
                http_call_bytes("PUT", &tar_url, &tar_headers, Some(&payload), 45_000)?;
            if (200..300).contains(&status) {
                return Ok(());
            }
            if matches!(status, 404 | 405 | 501) {
                let b64 = base64_encode(&payload);
                let script = format!(
                    "set -e; mkdir -p {dst}; printf '%s' '{b64}' | base64 -d | tar -xzf - -C {dst}",
                    dst = shell_single_quote(&dest),
                );
                let out = cf_exec(
                    &worker_url,
                    &headers,
                    &sandbox_id,
                    &["/bin/sh".to_string(), "-lc".to_string(), script],
                    Some("/"),
                    &BTreeMap::new(),
                    Some(45_000),
                    None,
                )?;
                if out.exit_code != 0 {
                    return Err(format!(
                        "Cloudflare bundle fallback extraction failed (exit {}): {}",
                        out.exit_code,
                        out.stderr.trim()
                    ));
                }
                return Ok(());
            }
            Err(format!(
                "Cloudflare bundle tar upload `{tar_url}` returned HTTP {status}: {}",
                String::from_utf8_lossy(&body)
            ))
        })
        .await
    }

    async fn release(&self, handle: &SandboxHandle) -> Result<(), String> {
        let url = format!(
            "{}/v1/sandboxes/{}/sleep",
            self.config.worker_url,
            url_encode_query_component(&handle.sandbox_id)
        );
        let headers = self.auth_headers();
        run_blocking(move || {
            let (status, val) =
                http_call_json("POST", &url, &headers, Some(&serde_json::json!({})), 30_000)?;
            if status == 404 || (200..300).contains(&status) {
                return Ok(());
            }
            Err(format!(
                "Cloudflare sleep `{url}` returned HTTP {status}: {val}"
            ))
        })
        .await
    }

    async fn destroy(&self, handle: &SandboxHandle) -> Result<(), String> {
        let url = format!(
            "{}/v1/sandboxes/{}",
            self.config.worker_url,
            url_encode_query_component(&handle.sandbox_id)
        );
        let headers = self.auth_headers();
        run_blocking(move || {
            let (status, val) = http_call_json("DELETE", &url, &headers, None, 30_000)?;
            if status == 404 || (200..300).contains(&status) {
                return Ok(());
            }
            Err(format!(
                "Cloudflare destroy `{url}` returned HTTP {status}: {val}"
            ))
        })
        .await
    }

    async fn health(&self) -> Result<ProviderHealth, String> {
        let url = format!("{}/health", self.config.worker_url);
        let headers = self.auth_headers();
        run_blocking(move || {
            let started = Instant::now();
            let (status, val) = http_call_json("GET", &url, &headers, None, 10_000)?;
            let healthy = (200..300).contains(&status);
            Ok(ProviderHealth {
                provider: ProviderKind::Cloudflare,
                healthy,
                message: if healthy {
                    "Cloudflare Sandbox Worker reachable".to_string()
                } else {
                    format!("Cloudflare Worker returned HTTP {status}: {val}")
                },
                latency_ms: Some(started.elapsed().as_millis() as u64),
            })
        })
        .await
    }
}

#[allow(clippy::too_many_arguments)]
fn cf_exec(
    worker_url: &str,
    headers: &[(String, String)],
    sandbox_id: &str,
    command: &[String],
    cwd: Option<&str>,
    env: &BTreeMap<String, String>,
    timeout_ms: Option<u64>,
    stdin: Option<&[u8]>,
) -> Result<ExecOutput, String> {
    let url = format!(
        "{worker_url}/v1/sandboxes/{}/exec",
        url_encode_query_component(sandbox_id)
    );
    let effective_timeout = timeout_ms.unwrap_or(30_000);
    let mut payload = serde_json::Map::new();
    payload.insert("command".into(), serde_json::json!(command));
    if let Some(dir) = cwd.filter(|s| !s.trim().is_empty()) {
        payload.insert("cwd".into(), serde_json::json!(dir));
    }
    if !env.is_empty() {
        payload.insert("env".into(), serde_json::json!(env));
    }
    payload.insert("timeout_ms".into(), serde_json::json!(effective_timeout));
    if let Some(bytes) = stdin {
        payload.insert(
            "stdin_base64".into(),
            serde_json::json!(base64_encode(bytes)),
        );
    }
    let (status, val) = http_call_json(
        "POST",
        &url,
        headers,
        Some(&serde_json::Value::Object(payload)),
        effective_timeout.saturating_add(10_000),
    )?;
    if !(200..300).contains(&status) {
        return Err(format!(
            "Cloudflare exec `{url}` returned HTTP {status}: {val}"
        ));
    }
    Ok(ExecOutput {
        stdout: val
            .get("stdout")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("")
            .to_string(),
        stderr: val
            .get("stderr")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("")
            .to_string(),
        exit_code: val
            .get("exit_code")
            .or_else(|| val.get("exitCode"))
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0) as i32,
    })
}
