//! Daytona Sandboxes REST API + Toolbox provider (`DaytonaProvider`).
//!
//! Supports OCI images (`ImageSpec.image`) and Daytona snapshots (`ImageSpec.template`),
//! persistent Daytona Volumes mounted at `/home/user/agent-data`, toolbox command exec
//! and file upload, `stop` on `release()` (preserving workspace & volume), `start` on
//! re-ensure, and `DELETE` cleanup on `destroy()`.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use async_trait::async_trait;

use super::super::url_encode_query_component;
use agent_tools::base64_encode;

use super::{
    EnsureSandboxRequest, ExecOutput, ExecRequest, NetworkPolicySpec, ProviderHealth, ProviderKind,
    SandboxCapabilities, SandboxHandle, SandboxProvider, argv_to_shell_command, http_call_bytes,
    http_call_json, persistent_layout_init_script, resolve_layout, run_blocking, sanitize_name,
    shell_single_quote,
};

const DEFAULT_DAYTONA_API_URL: &str = "https://app.daytona.io/api";

/// Configuration for [`DaytonaProvider`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DaytonaConfig {
    pub api_url: String,
    pub api_key: Option<String>,
    pub target: Option<String>,
    pub default_image: Option<String>,
    pub default_snapshot: Option<String>,
}

impl Default for DaytonaConfig {
    fn default() -> Self {
        Self {
            api_url: DEFAULT_DAYTONA_API_URL.to_string(),
            api_key: None,
            target: None,
            default_image: None,
            default_snapshot: None,
        }
    }
}

/// First-class [`SandboxProvider`] targeting the Daytona Sandboxes REST API.
#[derive(Clone, Debug)]
pub struct DaytonaProvider {
    config: DaytonaConfig,
}

impl DaytonaProvider {
    pub fn new(mut config: DaytonaConfig) -> Self {
        config.api_url = config.api_url.trim().trim_end_matches('/').to_string();
        if config.api_url.is_empty() {
            config.api_url = DEFAULT_DAYTONA_API_URL.to_string();
        }
        Self { config }
    }

    pub fn config(&self) -> &DaytonaConfig {
        &self.config
    }

    fn auth_headers(&self) -> Vec<(String, String)> {
        let mut h = Vec::new();
        if let Some(ref key) = self.config.api_key {
            h.push(("authorization".to_string(), format!("Bearer {key}")));
        }
        h
    }
}

#[async_trait]
impl SandboxProvider for DaytonaProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Daytona
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
            streaming_exec: false,
            pause_resume: true,
        }
    }

    async fn ensure(&self, req: &EnsureSandboxRequest) -> Result<SandboxHandle, String> {
        let this = self.clone();
        let req = req.clone();
        run_blocking(move || {
            let (mount_path, state_dir, workdir) = resolve_layout(&req);
            let headers = this.auth_headers();

            // 1. Ensure persistent Daytona Volume.
            let vol_name = req
                .volume
                .as_ref()
                .map(|v| v.id_or_name.clone())
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| format!("anyagents-{}", sanitize_name(&req.agent_id)));
            let volume_id = if let Some(prev_vol) = req
                .existing
                .as_ref()
                .and_then(|h| h.volume_id.as_deref())
                .filter(|s| !s.trim().is_empty())
            {
                prev_vol.to_string()
            } else {
                ensure_daytona_volume(&this.config.api_url, &headers, &vol_name)?
            };

            // 2. Restart existing stopped sandbox if provided.
            if let Some(ref existing) = req.existing
                && !existing.sandbox_id.trim().is_empty()
            {
                let start_url = format!(
                    "{}/sandbox/{}/start",
                    this.config.api_url,
                    url_encode_query_component(&existing.sandbox_id)
                );
                let (status, val) = http_call_json(
                    "POST",
                    &start_url,
                    &headers,
                    Some(&serde_json::json!({})),
                    30_000,
                )?;
                if (200..300).contains(&status) {
                    wait_daytona_started(&this.config.api_url, &headers, &existing.sandbox_id)?;
                    let mut h = existing.clone();
                    h.volume_id = Some(volume_id);
                    h.mount_path = mount_path;
                    h.state_dir = state_dir;
                    h.workdir = workdir;
                    return Ok(h);
                }
                if status != 404 && status != 410 {
                    return Err(format!(
                        "Daytona start `{start_url}` returned HTTP {status}: {val}"
                    ));
                }
            }

            // 3. Create Daytona Sandbox attaching the persistent Volume at `/home/user/agent-data`.
            let mut payload = serde_json::Map::new();
            payload.insert(
                "name".into(),
                serde_json::json!(format!("anyagents-{}", sanitize_name(&req.agent_id))),
            );
            if let Some(img) = req
                .image
                .image
                .as_deref()
                .or(this.config.default_image.as_deref())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                payload.insert("image".into(), serde_json::json!(img));
            }
            if let Some(snap) = req
                .image
                .template
                .as_deref()
                .or(this.config.default_snapshot.as_deref())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                payload.insert("snapshot".into(), serde_json::json!(snap));
            }
            if let Some(ref target) = this.config.target {
                payload.insert("target".into(), serde_json::json!(target));
            }
            if !req.image.env.is_empty() {
                payload.insert("env".into(), serde_json::json!(req.image.env));
            }
            let auto_stop_mins = req.lease.idle_ttl_ms.div_ceil(60_000);
            payload.insert("autoStopInterval".into(), serde_json::json!(auto_stop_mins));
            payload.insert(
                "volumes".into(),
                serde_json::json!([{
                    "volumeId": volume_id,
                    "mountPath": mount_path,
                }]),
            );
            match &req.network {
                NetworkPolicySpec::Allow => {}
                NetworkPolicySpec::Deny => {
                    payload.insert("networkBlockAll".into(), serde_json::json!(true));
                }
                NetworkPolicySpec::Allowlist(list) => {
                    payload.insert("networkAllowList".into(), serde_json::json!(list.join(",")));
                }
            }

            let create_url = format!("{}/sandbox", this.config.api_url);
            let (status, val) = http_call_json(
                "POST",
                &create_url,
                &headers,
                Some(&serde_json::Value::Object(payload)),
                45_000,
            )?;
            if !(200..300).contains(&status) {
                return Err(format!(
                    "Daytona create sandbox `{create_url}` returned HTTP {status}: {val}"
                ));
            }
            let sandbox_id = val
                .get("id")
                .or_else(|| val.get("sandboxId"))
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| format!("Daytona create response missing `id`: {val}"))?
                .to_string();

            let initial_state = val
                .get("state")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("")
                .to_ascii_lowercase();
            if !matches!(initial_state.as_str(), "started" | "running" | "ready") {
                wait_daytona_started(&this.config.api_url, &headers, &sandbox_id)?;
            }

            let init_script = persistent_layout_init_script(&mount_path, &state_dir, &workdir);
            let _ = daytona_toolbox_exec(
                &this.config.api_url,
                &headers,
                &sandbox_id,
                &init_script,
                Some("/"),
                Some(20_000),
            );

            Ok(SandboxHandle {
                provider: ProviderKind::Daytona,
                sandbox_id,
                volume_id: Some(volume_id),
                mount_path,
                state_dir,
                workdir,
                transport_url: Some(format!("{}/toolbox", this.config.api_url)),
                transport_token: None,
                expires_at: None,
                metadata: BTreeMap::new(),
            })
        })
        .await
    }

    async fn exec(&self, handle: &SandboxHandle, req: &ExecRequest) -> Result<ExecOutput, String> {
        if req.command.is_empty() {
            return Err("Daytona exec requires a non-empty command vector".to_string());
        }
        let api_url = self.config.api_url.clone();
        let headers = self.auth_headers();
        let sandbox_id = handle.sandbox_id.clone();
        let cwd = req
            .cwd
            .clone()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| handle.workdir.clone());
        let timeout_ms = req.timeout_ms;
        let cmd_str = if let Some(ref stdin_bytes) = req.stdin {
            let b64 = base64_encode(stdin_bytes);
            let inner = argv_to_shell_command(&req.command, &req.env);
            format!("printf '%s' '{b64}' | base64 -d | {inner}")
        } else {
            argv_to_shell_command(&req.command, &req.env)
        };

        run_blocking(move || {
            daytona_toolbox_exec(
                &api_url,
                &headers,
                &sandbox_id,
                &cmd_str,
                Some(&cwd),
                timeout_ms,
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
        let api_url = self.config.api_url.clone();
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
            let upload_url = format!(
                "{api_url}/toolbox/{}/toolbox/files/upload?path={}",
                url_encode_query_component(&sandbox_id),
                url_encode_query_component(&tmp_path)
            );
            let mut upload_headers = headers.clone();
            upload_headers.push((
                "content-type".to_string(),
                "application/octet-stream".to_string(),
            ));
            let (status, resp_body) =
                http_call_bytes("POST", &upload_url, &upload_headers, Some(&payload), 45_000)?;
            if !(200..300).contains(&status) {
                return Err(format!(
                    "Daytona file upload `{upload_url}` returned HTTP {status}: {}",
                    String::from_utf8_lossy(&resp_body)
                ));
            }
            let script = format!(
                "set -e; mkdir -p {dst}; tar -xzf {tmp} -C {dst}; rm -f {tmp}",
                dst = shell_single_quote(&dest),
                tmp = shell_single_quote(&tmp_path),
            );
            let out = daytona_toolbox_exec(
                &api_url,
                &headers,
                &sandbox_id,
                &script,
                Some("/"),
                Some(45_000),
            )?;
            if out.exit_code != 0 {
                return Err(format!(
                    "Daytona bundle extraction into `{dest}` failed (exit {}): {}",
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
            "{}/sandbox/{}/stop",
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
            Err(format!(
                "Daytona stop `{url}` returned HTTP {status}: {val}"
            ))
        })
        .await
    }

    async fn destroy(&self, handle: &SandboxHandle) -> Result<(), String> {
        let api_url = self.config.api_url.clone();
        let headers = self.auth_headers();
        let sandbox_id = handle.sandbox_id.clone();
        let volume_id = handle.volume_id.clone();
        run_blocking(move || {
            if !sandbox_id.trim().is_empty() {
                let sbx_url = format!(
                    "{api_url}/sandbox/{}",
                    url_encode_query_component(&sandbox_id)
                );
                let (status, val) = http_call_json("DELETE", &sbx_url, &headers, None, 30_000)?;
                if status != 404 && !(200..300).contains(&status) {
                    return Err(format!(
                        "Daytona delete sandbox `{sbx_url}` returned HTTP {status}: {val}"
                    ));
                }
            }
            if let Some(vol) = volume_id.filter(|s| !s.trim().is_empty()) {
                let vol_url = format!("{api_url}/volumes/{}", url_encode_query_component(&vol));
                let (vstatus, vval) = http_call_json("DELETE", &vol_url, &headers, None, 30_000)?;
                if vstatus != 404 && !(200..300).contains(&vstatus) {
                    return Err(format!(
                        "Daytona delete volume `{vol_url}` returned HTTP {vstatus}: {vval}"
                    ));
                }
            }
            Ok(())
        })
        .await
    }

    async fn health(&self) -> Result<ProviderHealth, String> {
        let url = format!("{}/health", self.config.api_url);
        let headers = self.auth_headers();
        run_blocking(move || {
            let started = Instant::now();
            let (status, val) = http_call_json("GET", &url, &headers, None, 10_000)?;
            let healthy = (200..300).contains(&status);
            Ok(ProviderHealth {
                provider: ProviderKind::Daytona,
                healthy,
                message: if healthy {
                    "Daytona API reachable".to_string()
                } else {
                    format!("Daytona API returned HTTP {status}: {val}")
                },
                latency_ms: Some(started.elapsed().as_millis() as u64),
            })
        })
        .await
    }
}

fn ensure_daytona_volume(
    api_url: &str,
    headers: &[(String, String)],
    name: &str,
) -> Result<String, String> {
    let lookup_url = format!(
        "{api_url}/volumes/by-name/{}",
        url_encode_query_component(name)
    );
    let (status, val) = http_call_json("GET", &lookup_url, headers, None, 15_000)?;
    if (200..300).contains(&status)
        && let Some(id) = val.get("id").and_then(serde_json::Value::as_str)
    {
        return Ok(id.to_string());
    }
    let create_url = format!("{api_url}/volumes");
    let (cstatus, cval) = http_call_json(
        "POST",
        &create_url,
        headers,
        Some(&serde_json::json!({ "name": name })),
        30_000,
    )?;
    if !(200..300).contains(&cstatus) {
        return Err(format!(
            "Daytona create volume `{create_url}` returned HTTP {cstatus}: {cval}"
        ));
    }
    cval.get("id")
        .or_else(|| cval.get("volumeId"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| format!("Daytona volume create response missing `id`: {cval}"))
}

fn wait_daytona_started(
    api_url: &str,
    headers: &[(String, String)],
    sandbox_id: &str,
) -> Result<(), String> {
    let url = format!(
        "{api_url}/sandbox/{}",
        url_encode_query_component(sandbox_id)
    );
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut backoff_ms = 25u64;
    loop {
        let (status, val) = http_call_json("GET", &url, headers, None, 15_000)?;
        if !(200..300).contains(&status) {
            return Err(format!(
                "Daytona poll `{url}` returned HTTP {status}: {val}"
            ));
        }
        let state = val
            .get("state")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("started")
            .to_ascii_lowercase();
        if matches!(state.as_str(), "started" | "running" | "ready") {
            return Ok(());
        }
        if matches!(state.as_str(), "error" | "failed" | "destroyed" | "deleted") {
            return Err(format!(
                "Daytona sandbox `{sandbox_id}` entered terminal state `{state}`"
            ));
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for Daytona sandbox `{sandbox_id}` to start (last state: `{state}`)"
            ));
        }
        std::thread::sleep(Duration::from_millis(backoff_ms));
        backoff_ms = (backoff_ms * 2).min(200);
    }
}

fn daytona_toolbox_exec(
    api_url: &str,
    headers: &[(String, String)],
    sandbox_id: &str,
    command: &str,
    cwd: Option<&str>,
    timeout_ms: Option<u64>,
) -> Result<ExecOutput, String> {
    let url = format!(
        "{api_url}/toolbox/{}/toolbox/process/execute",
        url_encode_query_component(sandbox_id)
    );
    let effective_timeout = timeout_ms.unwrap_or(30_000);
    let mut payload = serde_json::Map::new();
    payload.insert("command".into(), serde_json::json!(command));
    if let Some(dir) = cwd.filter(|s| !s.trim().is_empty()) {
        payload.insert("cwd".into(), serde_json::json!(dir));
    }
    payload.insert(
        "timeout".into(),
        serde_json::json!(effective_timeout.div_ceil(1_000)),
    );
    let (status, val) = http_call_json(
        "POST",
        &url,
        headers,
        Some(&serde_json::Value::Object(payload)),
        effective_timeout.saturating_add(10_000),
    )?;
    if !(200..300).contains(&status) {
        return Err(format!(
            "Daytona exec `{url}` returned HTTP {status}: {val}"
        ));
    }
    let stdout = val
        .get("stdout")
        .or_else(|| val.get("result"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .to_string();
    let stderr = val
        .get("stderr")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .to_string();
    let exit_code = val
        .get("exitCode")
        .or_else(|| val.get("exit_code"))
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0) as i32;
    Ok(ExecOutput {
        stdout,
        stderr,
        exit_code,
    })
}
