//! Modal Sandboxes HTTP/bridge provider (`ModalProvider`).
//!
//! Supports OCI images (`ImageSpec.image`), persistent Modal Volumes mounted at
//! `/home/user/agent-data`, per-sandbox network isolation (`block_network` /
//! `cidr_allowlist`), single-tarball bundle sync, compute termination preserving
//! the persistent Volume on `release()`, and full cleanup on `destroy()`.

use std::collections::BTreeMap;
use std::time::Instant;

use async_trait::async_trait;

use super::super::url_encode_query_component;
use agent_tools::base64_encode;

use super::{
    EnsureSandboxRequest, ExecOutput, ExecRequest, NetworkPolicySpec, ProviderHealth, ProviderKind,
    SandboxCapabilities, SandboxHandle, SandboxProvider, http_call_bytes, http_call_json,
    persistent_layout_init_script, resolve_layout, run_blocking, sanitize_name, shell_single_quote,
    split_allowlist_domains_and_cidrs,
};

const DEFAULT_MODAL_ENDPOINT: &str = "https://api.modal.com";
const DEFAULT_MODAL_APP: &str = "anyagents-sandboxes";
const DEFAULT_MODAL_IMAGE: &str = "debian:bookworm-slim";

/// Configuration for [`ModalProvider`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModalConfig {
    pub endpoint: String,
    pub token_id: Option<String>,
    pub token_secret: Option<String>,
    pub bearer_token: Option<String>,
    pub app_name: String,
    pub default_image: String,
}

impl Default for ModalConfig {
    fn default() -> Self {
        Self {
            endpoint: DEFAULT_MODAL_ENDPOINT.to_string(),
            token_id: None,
            token_secret: None,
            bearer_token: None,
            app_name: DEFAULT_MODAL_APP.to_string(),
            default_image: DEFAULT_MODAL_IMAGE.to_string(),
        }
    }
}

/// First-class [`SandboxProvider`] targeting Modal Sandboxes (HTTP API / bridge).
#[derive(Clone, Debug)]
pub struct ModalProvider {
    config: ModalConfig,
}

impl ModalProvider {
    pub fn new(mut config: ModalConfig) -> Self {
        config.endpoint = config.endpoint.trim().trim_end_matches('/').to_string();
        if config.endpoint.is_empty() {
            config.endpoint = DEFAULT_MODAL_ENDPOINT.to_string();
        }
        if config.app_name.trim().is_empty() {
            config.app_name = DEFAULT_MODAL_APP.to_string();
        }
        if config.default_image.trim().is_empty() {
            config.default_image = DEFAULT_MODAL_IMAGE.to_string();
        }
        Self { config }
    }

    pub fn config(&self) -> &ModalConfig {
        &self.config
    }

    fn auth_headers(&self) -> Vec<(String, String)> {
        let mut h = Vec::new();
        if let Some(ref key) = self.config.token_id {
            h.push(("modal-key".to_string(), key.clone()));
        }
        if let Some(ref secret) = self.config.token_secret {
            h.push(("modal-secret".to_string(), secret.clone()));
        }
        if let Some(ref tok) = self.config.bearer_token {
            h.push(("authorization".to_string(), format!("Bearer {tok}")));
        }
        h
    }
}

#[async_trait]
impl SandboxProvider for ModalProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Modal
    }

    fn capabilities(&self) -> SandboxCapabilities {
        SandboxCapabilities {
            oci_image: true,
            templates: true,
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
            let headers = this.auth_headers();

            // 1. Ensure persistent Modal Volume.
            let vol_name = req
                .existing
                .as_ref()
                .and_then(|h| h.volume_id.clone())
                .or_else(|| req.volume.as_ref().map(|v| v.id_or_name.clone()))
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| format!("anyagents-{}", sanitize_name(&req.agent_id)));

            let vol_url = format!("{}/v1/volumes", this.config.endpoint);
            let (vol_status, vol_val) = http_call_json(
                "POST",
                &vol_url,
                &headers,
                Some(&serde_json::json!({
                    "name": vol_name,
                    "create_if_missing": true,
                })),
                30_000,
            )?;
            if !(200..300).contains(&vol_status) {
                return Err(format!(
                    "Modal ensure volume `{vol_url}` returned HTTP {vol_status}: {vol_val}"
                ));
            }
            let volume_id = vol_val
                .get("volume_id")
                .or_else(|| vol_val.get("id"))
                .or_else(|| vol_val.get("name"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
                .unwrap_or(vol_name);

            // 2. Reuse active sandbox if still running.
            if let Some(ref existing) = req.existing
                && !existing.sandbox_id.trim().is_empty()
            {
                let get_url = format!(
                    "{}/v1/sandboxes/{}",
                    this.config.endpoint,
                    url_encode_query_component(&existing.sandbox_id)
                );
                let (status, val) = http_call_json("GET", &get_url, &headers, None, 15_000)?;
                if (200..300).contains(&status) {
                    let st = val
                        .get("status")
                        .or_else(|| val.get("state"))
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("running")
                        .to_ascii_lowercase();
                    if matches!(st.as_str(), "running" | "ready" | "active") {
                        let mut reused = existing.clone();
                        reused.volume_id = Some(volume_id);
                        reused.mount_path = mount_path;
                        reused.state_dir = state_dir;
                        reused.workdir = workdir;
                        return Ok(reused);
                    }
                }
            }

            // 3. Create Modal Sandbox with OCI image + persistent Volume mounted at `/home/user/agent-data`.
            let image = req
                .image
                .image
                .as_deref()
                .or(req.image.template.as_deref())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or(&this.config.default_image);
            let (block_network, cidr_allowlist, allowed_domains) = match &req.network {
                NetworkPolicySpec::Allow => (false, Vec::new(), Vec::new()),
                NetworkPolicySpec::Deny => (true, Vec::new(), Vec::new()),
                NetworkPolicySpec::Allowlist(entries) => {
                    let (domains, cidrs) = split_allowlist_domains_and_cidrs(entries);
                    (false, cidrs, domains)
                }
            };
            let mut volumes = serde_json::Map::new();
            volumes.insert(mount_path.clone(), serde_json::json!(volume_id));

            let mut payload = serde_json::Map::new();
            payload.insert("app_name".into(), serde_json::json!(this.config.app_name));
            payload.insert("agent_id".into(), serde_json::json!(req.agent_id));
            payload.insert("image".into(), serde_json::json!(image));
            payload.insert("volumes".into(), serde_json::Value::Object(volumes));
            payload.insert(
                "timeout".into(),
                serde_json::json!(req.lease.max_lease_ttl_secs()),
            );
            payload.insert(
                "idle_timeout".into(),
                serde_json::json!(req.lease.idle_ttl_ms / 1_000),
            );
            payload.insert("env".into(), serde_json::json!(req.image.env));
            payload.insert("workdir".into(), serde_json::json!(workdir));
            payload.insert("block_network".into(), serde_json::json!(block_network));
            if !cidr_allowlist.is_empty() {
                payload.insert("cidr_allowlist".into(), serde_json::json!(cidr_allowlist));
            }
            if !allowed_domains.is_empty() {
                payload.insert("allowed_domains".into(), serde_json::json!(allowed_domains));
            }

            let create_url = format!("{}/v1/sandboxes", this.config.endpoint);
            let (status, val) = http_call_json(
                "POST",
                &create_url,
                &headers,
                Some(&serde_json::Value::Object(payload)),
                45_000,
            )?;
            if !(200..300).contains(&status) {
                return Err(format!(
                    "Modal create sandbox `{create_url}` returned HTTP {status}: {val}"
                ));
            }
            let sandbox_id = val
                .get("sandbox_id")
                .or_else(|| val.get("object_id"))
                .or_else(|| val.get("id"))
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| format!("Modal create response missing `sandbox_id`: {val}"))?
                .to_string();

            // Initialize `/home/user/agent-data/{state,workspace}` + symlinks.
            let init_script = persistent_layout_init_script(&mount_path, &state_dir, &workdir);
            let _ = modal_exec(
                &this.config.endpoint,
                &headers,
                &sandbox_id,
                &["/bin/sh".to_string(), "-lc".to_string(), init_script],
                Some("/"),
                &BTreeMap::new(),
                Some(20_000),
                None,
            );

            let mut metadata = BTreeMap::new();
            metadata.insert("app_name".to_string(), this.config.app_name.clone());
            metadata.insert("image".to_string(), image.to_string());

            Ok(SandboxHandle {
                provider: ProviderKind::Modal,
                sandbox_id,
                volume_id: Some(volume_id),
                mount_path,
                state_dir,
                workdir,
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
            return Err("Modal exec requires a non-empty command vector".to_string());
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
            modal_exec(
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
            let tmp_path = format!("/tmp/.anyagents-bundle-{}.tar.gz", std::process::id());
            let put_url = format!(
                "{}/v1/sandboxes/{}/files?path={}",
                endpoint,
                url_encode_query_component(&sandbox_id),
                url_encode_query_component(&tmp_path)
            );
            let mut put_headers = headers.clone();
            put_headers.push(("content-type".to_string(), "application/gzip".to_string()));
            let (status, _) =
                http_call_bytes("PUT", &put_url, &put_headers, Some(&payload), 45_000)?;

            let out = if (200..300).contains(&status) {
                let script = format!(
                    "set -e; mkdir -p {dst}; tar -xzf {tmp} -C {dst}; rm -f {tmp}",
                    dst = shell_single_quote(&dest),
                    tmp = shell_single_quote(&tmp_path),
                );
                modal_exec(
                    &endpoint,
                    &headers,
                    &sandbox_id,
                    &["/bin/sh".to_string(), "-lc".to_string(), script],
                    Some("/"),
                    &BTreeMap::new(),
                    Some(45_000),
                    None,
                )?
            } else if matches!(status, 404 | 405 | 501) {
                let b64 = base64_encode(&payload);
                let script = format!(
                    "set -e; mkdir -p {dst}; printf '%s' '{b64}' | base64 -d | tar -xzf - -C {dst}",
                    dst = shell_single_quote(&dest),
                );
                modal_exec(
                    &endpoint,
                    &headers,
                    &sandbox_id,
                    &["/bin/sh".to_string(), "-lc".to_string(), script],
                    Some("/"),
                    &BTreeMap::new(),
                    Some(45_000),
                    None,
                )?
            } else {
                return Err(format!(
                    "Modal bundle upload `{put_url}` returned HTTP {status}"
                ));
            };

            if out.exit_code != 0 {
                return Err(format!(
                    "Modal bundle extraction into `{dest}` failed (exit {}): {}",
                    out.exit_code,
                    out.stderr.trim()
                ));
            }
            Ok(())
        })
        .await
    }

    async fn release(&self, handle: &SandboxHandle) -> Result<(), String> {
        let url = format!(
            "{}/v1/sandboxes/{}/terminate",
            self.config.endpoint,
            url_encode_query_component(&handle.sandbox_id)
        );
        let headers = self.auth_headers();
        run_blocking(move || {
            let (status, val) =
                http_call_json("POST", &url, &headers, Some(&serde_json::json!({})), 30_000)?;
            if status == 404 || status == 409 || (200..300).contains(&status) {
                return Ok(());
            }
            Err(format!(
                "Modal terminate `{url}` returned HTTP {status}: {val}"
            ))
        })
        .await
    }

    async fn destroy(&self, handle: &SandboxHandle) -> Result<(), String> {
        let endpoint = self.config.endpoint.clone();
        let headers = self.auth_headers();
        if !handle.sandbox_id.trim().is_empty() {
            self.release(handle).await?;
        }
        let volume_id = handle.volume_id.clone();

        run_blocking(move || {
            if let Some(vol) = volume_id.filter(|s| !s.trim().is_empty()) {
                let vol_url = format!("{endpoint}/v1/volumes/{}", url_encode_query_component(&vol));
                let (status, val) = http_call_json("DELETE", &vol_url, &headers, None, 30_000)?;
                if status != 404 && !(200..300).contains(&status) {
                    return Err(format!(
                        "Modal delete volume `{vol_url}` returned HTTP {status}: {val}"
                    ));
                }
            }
            Ok(())
        })
        .await
    }

    async fn health(&self) -> Result<ProviderHealth, String> {
        let url = format!("{}/v1/health", self.config.endpoint);
        let headers = self.auth_headers();
        run_blocking(move || {
            let started = Instant::now();
            let (status, val) = http_call_json("GET", &url, &headers, None, 10_000)?;
            let healthy = (200..300).contains(&status);
            Ok(ProviderHealth {
                provider: ProviderKind::Modal,
                healthy,
                message: if healthy {
                    "Modal Sandbox API reachable".to_string()
                } else {
                    format!("Modal API returned HTTP {status}: {val}")
                },
                latency_ms: Some(started.elapsed().as_millis() as u64),
            })
        })
        .await
    }
}

#[allow(clippy::too_many_arguments)]
fn modal_exec(
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
        payload.insert("workdir".into(), serde_json::json!(dir));
    }
    if !env.is_empty() {
        payload.insert("env".into(), serde_json::json!(env));
    }
    payload.insert(
        "timeout".into(),
        serde_json::json!(effective_timeout.div_ceil(1_000)),
    );
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
        return Err(format!("Modal exec `{url}` returned HTTP {status}: {val}"));
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
            .or_else(|| val.get("returncode"))
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0) as i32,
    })
}
