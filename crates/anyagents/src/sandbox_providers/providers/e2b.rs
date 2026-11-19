//! E2B Sandboxes REST API + `envd` Connect-RPC provider (`E2bProvider`).
//!
//! Supports creating sandboxes from templates (or custom OCI builds on compatible
//! E2B servers), pausing (`POST /sandboxes/{id}/pause`) to preserve filesystem state
//! across releases, resuming (`POST /sandboxes/{id}/resume`), killing
//! (`DELETE /sandboxes/{id}`), executing commands over `envd` Connect-RPC
//! (`process.Process/Start`), and single-tarball file synchronization via `envd` `/files`.

use std::collections::BTreeMap;
use std::time::Instant;

use async_trait::async_trait;

use super::super::sylphx::{EnvdProcessEvent, SylphxSandboxClient};
use super::super::url_encode_query_component;
use agent_tools::base64_encode;

use super::{
    EnsureSandboxRequest, ExecOutput, ExecRequest, NetworkPolicySpec, ProviderHealth, ProviderKind,
    SandboxCapabilities, SandboxHandle, SandboxProvider, http_call_bytes, http_call_json,
    persistent_layout_init_script, resolve_layout, run_blocking, sanitize_name, shell_single_quote,
};

const DEFAULT_E2B_API_URL: &str = "https://api.e2b.app";
const DEFAULT_E2B_DOMAIN: &str = "e2b.app";
const DEFAULT_E2B_TEMPLATE: &str = "base";

/// Configuration for [`E2bProvider`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct E2bConfig {
    pub api_url: String,
    pub domain: String,
    pub api_key: Option<String>,
    pub default_template: String,
    /// Optional explicit `envd` base URL override (useful for self-hosted E2B proxies or tests).
    pub envd_base_url: Option<String>,
}

impl Default for E2bConfig {
    fn default() -> Self {
        Self {
            api_url: DEFAULT_E2B_API_URL.to_string(),
            domain: DEFAULT_E2B_DOMAIN.to_string(),
            api_key: None,
            default_template: DEFAULT_E2B_TEMPLATE.to_string(),
            envd_base_url: None,
        }
    }
}

/// First-class [`SandboxProvider`] targeting the E2B Sandboxes REST + `envd` API.
#[derive(Clone, Debug)]
pub struct E2bProvider {
    config: E2bConfig,
}

impl E2bProvider {
    pub fn new(mut config: E2bConfig) -> Self {
        config.api_url = config.api_url.trim().trim_end_matches('/').to_string();
        if config.api_url.is_empty() {
            config.api_url = DEFAULT_E2B_API_URL.to_string();
        }
        config.domain = config.domain.trim().trim_matches('/').to_string();
        if config.domain.is_empty() {
            config.domain = DEFAULT_E2B_DOMAIN.to_string();
        }
        if config.default_template.trim().is_empty() {
            config.default_template = DEFAULT_E2B_TEMPLATE.to_string();
        }
        Self { config }
    }

    pub fn config(&self) -> &E2bConfig {
        &self.config
    }

    fn auth_headers(&self) -> Vec<(String, String)> {
        let mut h = Vec::new();
        if let Some(ref key) = self.config.api_key {
            h.push(("x-api-key".to_string(), key.clone()));
            h.push(("authorization".to_string(), format!("Bearer {key}")));
        }
        h
    }

    fn resolve_envd_url(
        &self,
        sandbox_id: &str,
        domain: Option<&str>,
        explicit: Option<&str>,
    ) -> String {
        if let Some(url) = explicit.map(str::trim).filter(|s| !s.is_empty()) {
            return url.trim_end_matches('/').to_string();
        }
        if let Some(ref override_base) = self.config.envd_base_url {
            return format!(
                "{}/{}",
                override_base.trim_end_matches('/'),
                url_encode_query_component(sandbox_id)
            );
        }
        let dom = domain
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or(&self.config.domain);
        format!("https://49983-{sandbox_id}.{dom}")
    }
}

#[async_trait]
impl SandboxProvider for E2bProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::E2b
    }

    fn capabilities(&self) -> SandboxCapabilities {
        SandboxCapabilities {
            oci_image: false,
            templates: true,
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
            let timeout_secs = req.lease.max_lease_ttl_secs();

            // 1. Resume paused sandbox when an existing sandbox ID is supplied.
            if let Some(ref existing) = req.existing
                && !existing.sandbox_id.trim().is_empty()
            {
                let resume_url = format!(
                    "{}/sandboxes/{}/resume",
                    this.config.api_url,
                    url_encode_query_component(&existing.sandbox_id)
                );
                let body = serde_json::json!({
                    "timeout": timeout_secs,
                });
                let (status, val) =
                    http_call_json("POST", &resume_url, &headers, Some(&body), 30_000)?;
                if (200..300).contains(&status) {
                    let sbx_id = val
                        .get("sandboxID")
                        .or_else(|| val.get("sandbox_id"))
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or(&existing.sandbox_id)
                        .to_string();
                    let envd_token = val
                        .get("envdAccessToken")
                        .or_else(|| val.get("envd_access_token"))
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string)
                        .or_else(|| existing.transport_token.clone());
                    let domain = val.get("domain").and_then(serde_json::Value::as_str);
                    let explicit_envd = val
                        .get("envdUrl")
                        .or_else(|| val.get("envd_url"))
                        .and_then(serde_json::Value::as_str);
                    let envd_url = existing
                        .transport_url
                        .clone()
                        .unwrap_or_else(|| this.resolve_envd_url(&sbx_id, domain, explicit_envd));
                    let expires_at = val
                        .get("endAt")
                        .or_else(|| val.get("expires_at"))
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string);
                    return Ok(SandboxHandle {
                        provider: ProviderKind::E2b,
                        sandbox_id: sbx_id.clone(),
                        volume_id: existing.volume_id.clone().or(Some(sbx_id)),
                        mount_path,
                        state_dir,
                        workdir,
                        transport_url: Some(envd_url),
                        transport_token: envd_token,
                        expires_at,
                        metadata: existing.metadata.clone(),
                    });
                }
                if status != 404 && status != 410 {
                    return Err(format!(
                        "E2B resume `{resume_url}` returned HTTP {status}: {val}"
                    ));
                }
            }

            // 2. Create fresh E2B sandbox.
            let template = req
                .image
                .template
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or(&this.config.default_template);
            let vol_name = req
                .volume
                .as_ref()
                .map(|v| v.id_or_name.clone())
                .unwrap_or_else(|| format!("anyagents-{}", sanitize_name(&req.agent_id)));
            let mut payload = serde_json::Map::new();
            payload.insert("templateID".into(), serde_json::json!(template));
            payload.insert("timeout".into(), serde_json::json!(timeout_secs));
            payload.insert("envVars".into(), serde_json::json!(req.image.env));
            payload.insert(
                "allowInternetAccess".into(),
                serde_json::json!(!matches!(req.network, NetworkPolicySpec::Deny)),
            );
            payload.insert(
                "metadata".into(),
                serde_json::json!({
                    "agent_id": req.agent_id,
                    "volume_name": vol_name,
                    "mount_path": mount_path,
                }),
            );
            if let Some(img) = req
                .image
                .image
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                payload.insert("image".into(), serde_json::json!(img));
            }
            if let NetworkPolicySpec::Allowlist(ref list) = req.network {
                payload.insert("networkAllowlist".into(), serde_json::json!(list));
            }

            let create_url = format!("{}/sandboxes", this.config.api_url);
            let (status, val) = http_call_json(
                "POST",
                &create_url,
                &headers,
                Some(&serde_json::Value::Object(payload)),
                45_000,
            )?;
            if !(200..300).contains(&status) {
                return Err(format!(
                    "E2B POST `{create_url}` returned HTTP {status}: {val}"
                ));
            }
            let sandbox_id = val
                .get("sandboxID")
                .or_else(|| val.get("sandbox_id"))
                .or_else(|| val.get("id"))
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| format!("E2B create response missing `sandboxID`: {val}"))?
                .to_string();
            let envd_token = val
                .get("envdAccessToken")
                .or_else(|| val.get("envd_access_token"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_string);
            let domain = val.get("domain").and_then(serde_json::Value::as_str);
            let explicit_envd = val
                .get("envdUrl")
                .or_else(|| val.get("envd_url"))
                .and_then(serde_json::Value::as_str);
            let envd_url = this.resolve_envd_url(&sandbox_id, domain, explicit_envd);
            let expires_at = val
                .get("endAt")
                .or_else(|| val.get("expires_at"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_string);

            // Best-effort persistent layout init inside sandbox.
            let init_script = persistent_layout_init_script(&mount_path, &state_dir, &workdir);
            let _ = run_envd_exec(
                &envd_url,
                envd_token.as_deref(),
                &["/bin/sh".to_string(), "-lc".to_string(), init_script],
                Some("/"),
                &BTreeMap::new(),
            );

            let mut metadata = BTreeMap::new();
            metadata.insert("template".to_string(), template.to_string());
            if let Some(cid) = val.get("clientID").and_then(serde_json::Value::as_str) {
                metadata.insert("client_id".to_string(), cid.to_string());
            }

            Ok(SandboxHandle {
                provider: ProviderKind::E2b,
                sandbox_id: sandbox_id.clone(),
                volume_id: Some(vol_name),
                mount_path,
                state_dir,
                workdir,
                transport_url: Some(envd_url),
                transport_token: envd_token,
                expires_at,
                metadata,
            })
        })
        .await
    }

    async fn exec(&self, handle: &SandboxHandle, req: &ExecRequest) -> Result<ExecOutput, String> {
        if req.command.is_empty() {
            return Err("E2B exec requires a non-empty command vector".to_string());
        }
        let envd_url = handle
            .transport_url
            .clone()
            .unwrap_or_else(|| self.resolve_envd_url(&handle.sandbox_id, None, None));
        let token = handle.transport_token.clone();
        let cwd = req
            .cwd
            .clone()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| handle.workdir.clone());
        let env = req.env.clone();
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

        run_blocking(move || run_envd_exec(&envd_url, token.as_deref(), &command, Some(&cwd), &env))
            .await
    }

    async fn sync_bundle_tar(
        &self,
        handle: &SandboxHandle,
        dest_dir: &str,
        tar_gz: &[u8],
    ) -> Result<(), String> {
        let envd_url = handle
            .transport_url
            .clone()
            .unwrap_or_else(|| self.resolve_envd_url(&handle.sandbox_id, None, None));
        let token = handle.transport_token.clone();
        let dest = if dest_dir.trim().is_empty() {
            handle.state_dir.clone()
        } else {
            dest_dir.trim().to_string()
        };
        let payload = tar_gz.to_vec();

        run_blocking(move || {
            let tmp_path = format!("/tmp/.anyagents-bundle-{}.tar.gz", std::process::id());
            let upload_url = format!(
                "{}/files?path={}&username=user",
                envd_url.trim_end_matches('/'),
                url_encode_query_component(&tmp_path)
            );
            let mut headers = vec![(
                "content-type".to_string(),
                "application/octet-stream".to_string(),
            )];
            if let Some(ref tok) = token {
                headers.push(("x-access-token".to_string(), tok.clone()));
                headers.push(("authorization".to_string(), format!("Bearer {tok}")));
            }
            let (status, resp_body) =
                http_call_bytes("POST", &upload_url, &headers, Some(&payload), 45_000)?;
            if !(200..300).contains(&status) {
                return Err(format!(
                    "E2B envd file upload `{upload_url}` returned HTTP {status}: {}",
                    String::from_utf8_lossy(&resp_body)
                ));
            }
            let script = format!(
                "set -e; mkdir -p {dst}; tar -xzf {tmp} -C {dst}; rm -f {tmp}",
                dst = shell_single_quote(&dest),
                tmp = shell_single_quote(&tmp_path),
            );
            let out = run_envd_exec(
                &envd_url,
                token.as_deref(),
                &["/bin/sh".to_string(), "-lc".to_string(), script],
                Some("/"),
                &BTreeMap::new(),
            )?;
            if out.exit_code != 0 {
                return Err(format!(
                    "E2B bundle tar extraction into `{dest}` failed (exit {}): {}",
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
            "{}/sandboxes/{}/pause",
            self.config.api_url,
            url_encode_query_component(&handle.sandbox_id)
        );
        let headers = self.auth_headers();
        run_blocking(move || {
            let (status, val) =
                http_call_json("POST", &url, &headers, Some(&serde_json::json!({})), 30_000)?;
            if status == 404 || status == 409 || (200..300).contains(&status) {
                return Ok(());
            }
            Err(format!("E2B pause `{url}` returned HTTP {status}: {val}"))
        })
        .await
    }

    async fn destroy(&self, handle: &SandboxHandle) -> Result<(), String> {
        let url = format!(
            "{}/sandboxes/{}",
            self.config.api_url,
            url_encode_query_component(&handle.sandbox_id)
        );
        let headers = self.auth_headers();
        run_blocking(move || {
            let (status, val) = http_call_json("DELETE", &url, &headers, None, 30_000)?;
            if status == 404 || (200..300).contains(&status) {
                return Ok(());
            }
            Err(format!("E2B delete `{url}` returned HTTP {status}: {val}"))
        })
        .await
    }

    async fn health(&self) -> Result<ProviderHealth, String> {
        let url = format!("{}/sandboxes", self.config.api_url);
        let headers = self.auth_headers();
        run_blocking(move || {
            let started = Instant::now();
            let (status, val) = http_call_json("GET", &url, &headers, None, 10_000)?;
            let healthy = (200..300).contains(&status);
            Ok(ProviderHealth {
                provider: ProviderKind::E2b,
                healthy,
                message: if healthy {
                    "E2B Sandboxes API reachable".to_string()
                } else {
                    format!("E2B API returned HTTP {status}: {val}")
                },
                latency_ms: Some(started.elapsed().as_millis() as u64),
            })
        })
        .await
    }
}

fn run_envd_exec(
    envd_url: &str,
    envd_token: Option<&str>,
    command: &[String],
    cwd: Option<&str>,
    env: &BTreeMap<String, String>,
) -> Result<ExecOutput, String> {
    let client = SylphxSandboxClient::new(envd_url, envd_token.map(str::to_string));
    let mut stdout_buf = Vec::new();
    let mut stderr_buf = Vec::new();
    let exit_code = client.start_envd_stream(
        envd_url,
        envd_token,
        &command[0],
        &command[1..],
        cwd,
        env,
        false,
        |ev| {
            if let EnvdProcessEvent::Data { stdout, stderr, .. } = ev {
                stdout_buf.extend_from_slice(&stdout);
                stderr_buf.extend_from_slice(&stderr);
            }
        },
    )?;
    Ok(ExecOutput {
        stdout: String::from_utf8_lossy(&stdout_buf).into_owned(),
        stderr: String::from_utf8_lossy(&stderr_buf).into_owned(),
        exit_code: exit_code.unwrap_or(0),
    })
}
