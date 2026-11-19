//! Vendor-neutral Generic HTTP OCI sandbox provider (`GenericHttpOciProvider`).
//!
//! Lets ANY external sandbox controller (Kubernetes, Firecracker, Fly Machines, Nomad,
//! or custom infrastructure) integrate with AnyAgents with zero Rust edits by serving:
//! - `POST /v1/sandboxes/ensure`
//! - `POST /v1/sandboxes/{id}/exec`
//! - `POST /v1/sandboxes/{id}/bundle?dest=<path>` (raw `application/gzip`, with `/exec` fallback)
//! - `POST /v1/sandboxes/{id}/network`
//! - `POST /v1/sandboxes/{id}/release`
//! - `DELETE /v1/sandboxes/{id}?volume_id=<vol>`
//! - `GET /v1/health`

use std::collections::BTreeMap;
use std::time::Instant;

use async_trait::async_trait;

use super::super::url_encode_query_component;
use agent_tools::base64_encode;

use super::{
    EnsureSandboxRequest, ExecOutput, ExecRequest, NetworkPolicySpec, ProviderHealth, ProviderKind,
    SandboxCapabilities, SandboxHandle, SandboxProvider, http_call_bytes, http_call_json,
    resolve_layout, run_blocking, sanitize_name, shell_single_quote,
};

/// Configuration for [`GenericHttpOciProvider`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GenericHttpOciConfig {
    pub endpoint: String,
    pub token: Option<String>,
    pub default_image: Option<String>,
    pub default_template: Option<String>,
    pub extra_headers: BTreeMap<String, String>,
}

impl GenericHttpOciConfig {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into().trim().trim_end_matches('/').to_string(),
            token: None,
            default_image: None,
            default_template: None,
            extra_headers: BTreeMap::new(),
        }
    }
}

/// First-class [`SandboxProvider`] implementing the documented vendor-neutral HTTP OCI contract.
#[derive(Clone, Debug)]
pub struct GenericHttpOciProvider {
    config: GenericHttpOciConfig,
}

impl GenericHttpOciProvider {
    pub fn new(mut config: GenericHttpOciConfig) -> Result<Self, String> {
        config.endpoint = config.endpoint.trim().trim_end_matches('/').to_string();
        if config.endpoint.is_empty() {
            return Err(
                "GenericHttpOciProvider requires `endpoint` (`ANYAGENTS_SANDBOX_HTTP_URL`)"
                    .to_string(),
            );
        }
        Ok(Self { config })
    }

    pub fn config(&self) -> &GenericHttpOciConfig {
        &self.config
    }

    fn auth_headers(&self) -> Vec<(String, String)> {
        let mut h = Vec::new();
        if let Some(ref tok) = self.config.token {
            h.push(("authorization".to_string(), format!("Bearer {tok}")));
        }
        for (k, v) in &self.config.extra_headers {
            h.push((k.clone(), v.clone()));
        }
        h
    }
}

#[async_trait]
impl SandboxProvider for GenericHttpOciProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Http
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
            streaming_exec: false,
            pause_resume: false,
        }
    }

    async fn ensure(&self, req: &EnsureSandboxRequest) -> Result<SandboxHandle, String> {
        let this = self.clone();
        let req = req.clone();
        run_blocking(move || {
            let (mount_path, state_dir, workdir) = resolve_layout(&req);
            let headers = this.auth_headers();
            let vol_name = req
                .existing
                .as_ref()
                .and_then(|h| h.volume_id.clone())
                .or_else(|| req.volume.as_ref().map(|v| v.id_or_name.clone()))
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| format!("anyagents-{}", sanitize_name(&req.agent_id)));

            let image = req
                .image
                .image
                .as_deref()
                .or(this.config.default_image.as_deref())
                .map(str::trim)
                .filter(|s| !s.is_empty());
            let template = req
                .image
                .template
                .as_deref()
                .or(this.config.default_template.as_deref())
                .map(str::trim)
                .filter(|s| !s.is_empty());

            let payload = serde_json::json!({
                "agent_id": req.agent_id,
                "agent_name": req.agent_name,
                "image": image,
                "template": template,
                "env": req.image.env,
                "volume": {
                    "id_or_name": vol_name,
                    "mount_path": mount_path,
                    "size_gib": req.volume.as_ref().and_then(|v| v.size_gib),
                },
                "shape": req.shape,
                "network": req.network,
                "lease": req.lease,
                "existing_sandbox_id": req.existing.as_ref().map(|h| h.sandbox_id.as_str()),
                "workdir": workdir,
            });

            let url = format!("{}/v1/sandboxes/ensure", this.config.endpoint);
            let (status, val) = http_call_json("POST", &url, &headers, Some(&payload), 45_000)?;
            if !(200..300).contains(&status) {
                return Err(format!(
                    "Generic HTTP OCI ensure `{url}` returned HTTP {status}: {val}"
                ));
            }
            let sandbox_id = val
                .get("sandbox_id")
                .or_else(|| val.get("id"))
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| format!("Generic HTTP OCI ensure missing `sandbox_id`: {val}"))?
                .to_string();

            let resp_mount = val
                .get("mount_path")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
                .unwrap_or(mount_path);
            let resp_state = val
                .get("state_dir")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
                .unwrap_or(state_dir);
            let resp_workdir = val
                .get("workdir")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
                .unwrap_or(workdir);
            let volume_id = val
                .get("volume_id")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
                .or(Some(vol_name));

            let mut metadata = BTreeMap::new();
            if let Some(obj) = val.get("metadata").and_then(serde_json::Value::as_object) {
                for (k, v) in obj {
                    if let Some(s) = v.as_str() {
                        metadata.insert(k.clone(), s.to_string());
                    }
                }
            }

            Ok(SandboxHandle {
                provider: ProviderKind::Http,
                sandbox_id,
                volume_id,
                mount_path: resp_mount,
                state_dir: resp_state,
                workdir: resp_workdir,
                transport_url: val
                    .get("transport_url")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string),
                transport_token: val
                    .get("transport_token")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string),
                expires_at: val
                    .get("expires_at")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string),
                metadata,
            })
        })
        .await
    }

    async fn exec(&self, handle: &SandboxHandle, req: &ExecRequest) -> Result<ExecOutput, String> {
        if req.command.is_empty() {
            return Err("Generic HTTP OCI exec requires a non-empty command vector".to_string());
        }
        let endpoint = self.config.endpoint.clone();
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
            http_oci_exec(
                &endpoint,
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
        let endpoint = self.config.endpoint.clone();
        let headers = self.auth_headers();
        let sandbox_id = handle.sandbox_id.clone();
        let dest = if dest_dir.trim().is_empty() {
            handle.state_dir.clone()
        } else {
            dest_dir.trim().to_string()
        };
        let payload = tar_gz.to_vec();

        run_blocking(move || {
            let bundle_url = format!(
                "{endpoint}/v1/sandboxes/{}/bundle?dest={}",
                url_encode_query_component(&sandbox_id),
                url_encode_query_component(&dest)
            );
            let mut bundle_headers = headers.clone();
            bundle_headers.push(("content-type".to_string(), "application/gzip".to_string()));
            let (status, body) =
                http_call_bytes("POST", &bundle_url, &bundle_headers, Some(&payload), 45_000)?;
            if (200..300).contains(&status) {
                return Ok(());
            }
            if matches!(status, 404 | 405 | 501) {
                let b64 = base64_encode(&payload);
                let script = format!(
                    "set -e; mkdir -p {dst}; printf '%s' '{b64}' | base64 -d | tar -xzf - -C {dst}",
                    dst = shell_single_quote(&dest),
                );
                let out = http_oci_exec(
                    &endpoint,
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
                        "Generic HTTP OCI bundle fallback extraction failed (exit {}): {}",
                        out.exit_code,
                        out.stderr.trim()
                    ));
                }
                return Ok(());
            }
            Err(format!(
                "Generic HTTP OCI bundle upload `{bundle_url}` returned HTTP {status}: {}",
                String::from_utf8_lossy(&body)
            ))
        })
        .await
    }

    async fn set_network(
        &self,
        handle: &SandboxHandle,
        policy: &NetworkPolicySpec,
    ) -> Result<(), String> {
        let url = format!(
            "{}/v1/sandboxes/{}/network",
            self.config.endpoint,
            url_encode_query_component(&handle.sandbox_id)
        );
        let headers = self.auth_headers();
        let body = serde_json::json!({ "network": policy });
        run_blocking(move || {
            let (status, val) = http_call_json("POST", &url, &headers, Some(&body), 20_000)?;
            if (200..300).contains(&status) {
                return Ok(());
            }
            Err(format!(
                "Generic HTTP OCI set_network `{url}` returned HTTP {status}: {val}"
            ))
        })
        .await
    }

    async fn release(&self, handle: &SandboxHandle) -> Result<(), String> {
        let url = format!(
            "{}/v1/sandboxes/{}/release",
            self.config.endpoint,
            url_encode_query_component(&handle.sandbox_id)
        );
        let headers = self.auth_headers();
        let body = serde_json::json!({ "volume_id": handle.volume_id });
        run_blocking(move || {
            let (status, val) = http_call_json("POST", &url, &headers, Some(&body), 30_000)?;
            if status == 404 || (200..300).contains(&status) {
                return Ok(());
            }
            Err(format!(
                "Generic HTTP OCI release `{url}` returned HTTP {status}: {val}"
            ))
        })
        .await
    }

    async fn destroy(&self, handle: &SandboxHandle) -> Result<(), String> {
        let mut url = format!(
            "{}/v1/sandboxes/{}",
            self.config.endpoint,
            url_encode_query_component(&handle.sandbox_id)
        );
        if let Some(ref vol) = handle.volume_id {
            url.push_str(&format!("?volume_id={}", url_encode_query_component(vol)));
        }
        let headers = self.auth_headers();
        run_blocking(move || {
            let (status, val) = http_call_json("DELETE", &url, &headers, None, 30_000)?;
            if status == 404 || (200..300).contains(&status) {
                return Ok(());
            }
            Err(format!(
                "Generic HTTP OCI destroy `{url}` returned HTTP {status}: {val}"
            ))
        })
        .await
    }

    async fn health(&self) -> Result<ProviderHealth, String> {
        let url = format!("{}/v1/health", self.config.endpoint);
        let headers = self.auth_headers();
        run_blocking(move || {
            let started = Instant::now();
            let (status, val) = http_call_json("GET", &url, &headers, None, 10_000)?;
            let healthy = (200..300).contains(&status)
                && val
                    .get("healthy")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(true);
            let message = val
                .get("message")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
                .unwrap_or_else(|| {
                    if healthy {
                        "Generic HTTP OCI sandbox service healthy".to_string()
                    } else {
                        format!("Generic HTTP OCI service returned HTTP {status}: {val}")
                    }
                });
            Ok(ProviderHealth {
                provider: ProviderKind::Http,
                healthy,
                message,
                latency_ms: Some(started.elapsed().as_millis() as u64),
            })
        })
        .await
    }
}

#[allow(clippy::too_many_arguments)]
fn http_oci_exec(
    endpoint: &str,
    headers: &[(String, String)],
    sandbox_id: &str,
    command: &[String],
    cwd: Option<&str>,
    env: &BTreeMap<String, String>,
    timeout_ms: Option<u64>,
    stdin: Option<&[u8]>,
) -> Result<ExecOutput, String> {
    let url = format!(
        "{endpoint}/v1/sandboxes/{}/exec",
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
            "Generic HTTP OCI exec `{url}` returned HTTP {status}: {val}"
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
