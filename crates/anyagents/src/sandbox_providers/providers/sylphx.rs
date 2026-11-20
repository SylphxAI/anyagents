//! Native Sylphx Sandboxes v1 provider (`SylphxProvider`).
//!
//! Supports BOTH digest-pinned OCI images (`ImageSpec.image`) AND built-in templates
//! (`ImageSpec.template`), mounts a single persistent volume at `/home/user/agent-data`
//! containing `{state,workspace}` with backwards-compatible symlinks, syncs bundles via
//! a single `.tar.gz` archive upload + extract, supports live `:setNetwork` egress policy
//! mutation, and decouples compute leases (default 5m warm idle / 15m safety TTL) from
//! persistent volume lifetimes.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;

use super::super::sylphx::{DEFAULT_SHAPE, DEFAULT_TEMPLATE, SylphxSandboxClient};
use agent_tools::base64_encode;

static TAR_UPLOAD_SEQ: AtomicU64 = AtomicU64::new(1);
static VOLUME_ENSURE_LOCKS: OnceLock<Mutex<HashMap<String, Arc<Mutex<()>>>>> = OnceLock::new();

fn volume_ensure_lock(key: &str) -> Arc<Mutex<()>> {
    let map = VOLUME_ENSURE_LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = map.lock().unwrap_or_else(|e| e.into_inner());
    Arc::clone(
        guard
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(()))),
    )
}

fn is_lease_collision_or_timeout(err: &str) -> bool {
    let lower = err.to_ascii_lowercase();
    lower.contains("409")
        || lower.contains("resource_in_use")
        || lower.contains("attached to another lease")
        || lower.contains("timeout")
        || lower.contains("timed out")
}

use super::{
    DEFAULT_MOUNT_PATH, EnsureSandboxRequest, ExecOutput, ExecRequest, LeasePolicy,
    NetworkPolicySpec, ProviderHealth, ProviderKind, SandboxCapabilities, SandboxHandle,
    SandboxProvider, http_call_json, normalize_unix_dir, resolve_layout, run_blocking,
    sanitize_name, shell_single_quote,
};

const DEFAULT_SYLPHX_ENDPOINT: &str = "https://api.sylphx.com";
const LEASE_READY_TIMEOUT: Duration = Duration::from_secs(120);
/// Maximum raw stdin size inlined via `printf '%s' '<b64>'` in `argv`.
/// Larger payloads are uploaded via `:writeFile` to stay well below Linux's
/// 128 KiB `MAX_ARG_STRLEN` limit per `argv`/`envp` string.
const INLINE_STDIN_MAX_BYTES: usize = 24 * 1024;

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

            // Serialize concurrent `ensure()` calls for the same volume within this
            // coordinator replica so two simultaneous requests never race `POST /leases`.
            let vol_lock_key = format!("{}::{}", cfg.parent, volume.resource_name);
            let vol_lock = volume_ensure_lock(&vol_lock_key);
            let _vol_guard = vol_lock.lock().unwrap_or_else(|e| e.into_inner());

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

            // If persisted binding had no active lease (or another replica created/owned
            // a live lease for this volume), discover and reuse any active lease attached
            // to `volume.resource_name` before attempting `POST /leases`.
            if reused_lease.is_none()
                && let Ok(Some(discovered)) =
                    client.find_active_lease_for_volume(&cfg.parent, &volume.resource_name)
            {
                reused_lease =
                    Some(client.wait_lease_ready(&cfg.parent, discovered, LEASE_READY_TIMEOUT)?);
            }

            let lease = match reused_lease {
                Some(active) => client
                    .renew_lease(&cfg.parent, &active.resource_name, Some(&ttl_str))
                    .unwrap_or(active),
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

                    const COLLISION_BACKOFF_MS: &[u64] = &[1_500, 3_000, 5_000, 8_000];
                    let mut attempt = 0usize;
                    loop {
                        match client.create_lease_with_options(
                            &cfg.parent,
                            &volume.resource_name,
                            &mount_path,
                            image,
                            Some(template),
                            shape,
                            &req.image.env,
                            Some(&ttl_str),
                            Some(&req.network),
                        ) {
                            Ok(created) => {
                                break client.wait_lease_ready(
                                    &cfg.parent,
                                    created,
                                    LEASE_READY_TIMEOUT,
                                )?;
                            }
                            Err(err) if is_lease_collision_or_timeout(&err) => {
                                // Recover when a concurrent replica or a timed-out POST
                                // already attached `volume.resource_name` to a live lease.
                                if let Ok(Some(discovered)) = client.find_active_lease_for_volume(
                                    &cfg.parent,
                                    &volume.resource_name,
                                ) {
                                    let ready = client.wait_lease_ready(
                                        &cfg.parent,
                                        discovered,
                                        LEASE_READY_TIMEOUT,
                                    )?;
                                    break client
                                        .renew_lease(
                                            &cfg.parent,
                                            &ready.resource_name,
                                            Some(&ttl_str),
                                        )
                                        .unwrap_or(ready);
                                }
                                if attempt < COLLISION_BACKOFF_MS.len() {
                                    std::thread::sleep(Duration::from_millis(
                                        COLLISION_BACKOFF_MS[attempt],
                                    ));
                                    attempt += 1;
                                    continue;
                                }
                                return Err(err);
                            }
                            Err(err) => return Err(err),
                        }
                    }
                }
            };

            let data_plane_resource = lease
                .data_plane_resource
                .clone()
                .unwrap_or_else(|| lease.resource_name.clone());

            // ALWAYS run the idempotent layout init on every attach (fresh, reused, or recovered
            // after a POST timeout / 409): a lease whose creator never finished init would
            // otherwise leave a root-owned volume without `state/`. Failure fails closed.
            client.init_persistent_layout(
                &data_plane_resource,
                &mount_path,
                &state_dir,
                &workdir,
            )?;

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
        let stdin_bytes = req.stdin.clone();
        let base_command = req.command.clone();

        run_blocking(move || {
            let command = match stdin_bytes {
                Some(bytes) if bytes.len() > INLINE_STDIN_MAX_BYTES => {
                    let seq = TAR_UPLOAD_SEQ.fetch_add(1, Ordering::Relaxed);
                    let tmp_stdin = format!("/tmp/.anyagents-stdin-{}-{seq}", std::process::id());
                    client.write_file(&resource, &tmp_stdin, &bytes)?;
                    let inner = base_command
                        .iter()
                        .map(|a| shell_single_quote(a))
                        .collect::<Vec<_>>()
                        .join(" ");
                    let q = shell_single_quote(&tmp_stdin);
                    vec![
                        "/bin/sh".to_string(),
                        "-lc".to_string(),
                        format!("exec 3<{q}; rm -f {q}; {inner} <&3 3<&-"),
                    ]
                }
                Some(bytes) => {
                    let b64 = base64_encode(&bytes);
                    let inner = base_command
                        .iter()
                        .map(|a| shell_single_quote(a))
                        .collect::<Vec<_>>()
                        .join(" ");
                    vec![
                        "/bin/sh".to_string(),
                        "-lc".to_string(),
                        format!("printf '%s' '{b64}' | base64 -d | {inner}"),
                    ]
                }
                None => base_command,
            };
            client.exec(&resource, &command, Some(&cwd), &env, timeout_ms)
        })
        .await
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
            let seq = TAR_UPLOAD_SEQ.fetch_add(1, Ordering::Relaxed);
            let tmp_archive = format!("/tmp/.anyagents-bundle-{}-{seq}.tar.gz", std::process::id());
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
        let token = self.config.token.clone();

        run_blocking(move || {
            let started = Instant::now();
            let mut headers = Vec::new();
            if let Some(ref tok) = token {
                headers.push(("authorization".to_string(), format!("Bearer {tok}")));
            }
            let shapes_url = format!("{endpoint}/v1/sandbox_shapes");
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Bytes;
    use axum::extract::State;
    use axum::http::{Method, StatusCode, Uri};
    use axum::{Json, Router};
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD;
    use serde_json::{Value, json};

    type RecordedBytes = Arc<Mutex<Vec<(String, Vec<u8>)>>>;

    #[derive(Clone, Default)]
    struct MockSylphxState {
        write_files: RecordedBytes,
        exec_commands: Arc<Mutex<Vec<Vec<String>>>>,
    }

    async fn mock_sylphx_handler(
        State(state): State<MockSylphxState>,
        uri: Uri,
        Json(body): Json<Value>,
    ) -> Json<Value> {
        let path = uri.path();
        if path.ends_with(":writeFile") {
            let file_path = body
                .get("path")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let b64 = body
                .get("content")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let decoded = STANDARD
                .decode(b64)
                .expect("valid base64 writeFile payload");
            state.write_files.lock().unwrap().push((file_path, decoded));
            return Json(json!({}));
        }
        if path.ends_with(":exec") {
            let cmd = body
                .get("command")
                .and_then(Value::as_array)
                .map(|arr| {
                    arr.iter()
                        .filter_map(Value::as_str)
                        .map(str::to_string)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            state.exec_commands.lock().unwrap().push(cmd);
            return Json(json!({ "exit_code": 0, "stdout": "", "stderr": "" }));
        }
        Json(json!({}))
    }

    #[tokio::test]
    async fn sylphx_exec_stages_oversized_stdin_via_write_file_instead_of_argv() {
        let state = MockSylphxState::default();
        let app = Router::new()
            .fallback(mock_sylphx_handler)
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let mut cfg = SylphxConfig::new("orgs/o/projects/p/envs/e");
        cfg.endpoint = format!("http://{addr}");
        let provider = SylphxProvider::new(cfg).unwrap();
        let handle = SandboxHandle {
            provider: ProviderKind::Sylphx,
            sandbox_id: "sbx-stdin-test".to_string(),
            volume_id: Some("vol-stdin-test".to_string()),
            mount_path: DEFAULT_MOUNT_PATH.to_string(),
            state_dir: "/home/user/agent-data/state".to_string(),
            workdir: "/home/user/agent-data/workspace".to_string(),
            transport_url: None,
            transport_token: None,
            expires_at: None,
            metadata: BTreeMap::new(),
        };

        // 1. Small stdin (<= 24 KiB) stays inline without an extra :writeFile round-trip.
        let small_req = ExecRequest::argv(["/usr/local/bin/anyagents-guest", "turn"])
            .with_stdin(b"small prompt".to_vec());
        let out_small = provider.exec(&handle, &small_req).await.unwrap();
        assert_eq!(out_small.exit_code, 0);
        assert!(state.write_files.lock().unwrap().is_empty());
        {
            let cmds = state.exec_commands.lock().unwrap();
            assert_eq!(cmds.len(), 1);
            assert!(cmds[0][2].contains("printf '%s'"));
            assert!(cmds[0][2].contains("base64 -d"));
        }

        // 2. Oversized stdin (> 24 KiB) uploads via :writeFile and keeps argv tiny.
        let big_payload = vec![b'X'; INLINE_STDIN_MAX_BYTES + 8 * 1024];
        let big_req = ExecRequest::argv(["/usr/local/bin/anyagents-guest", "turn"])
            .with_stdin(big_payload.clone());
        let out_big = provider.exec(&handle, &big_req).await.unwrap();
        assert_eq!(out_big.exit_code, 0);
        {
            let writes = state.write_files.lock().unwrap();
            assert_eq!(writes.len(), 1);
            assert!(writes[0].0.starts_with("/tmp/.anyagents-stdin-"));
            assert_eq!(writes[0].1, big_payload);
        }
        {
            let cmds = state.exec_commands.lock().unwrap();
            assert_eq!(cmds.len(), 2);
            let script = &cmds[1][2];
            assert!(script.contains("exec 3<"), "{script}");
            assert!(script.contains("rm -f"), "{script}");
            assert!(script.contains("<&3"), "{script}");
            assert!(
                script.len() < 1024,
                "oversized stdin must not be inlined into argv (got {} bytes)",
                script.len()
            );
        }

        server.abort();
    }

    const PARENT: &str = "orgs/o/projects/p/envs/e";

    /// Stateful lease-lifecycle mock: volume lookup, lease list/get/renew, a configurable
    /// `POST /leases` failure (simulated HTTP timeout / `409 Conflict`), and `:exec`.
    #[derive(Clone)]
    struct LeaseMockState {
        create_failure: (u16, Value),
        exec_exit_code: i64,
        list_calls: Arc<AtomicU64>,
        create_calls: Arc<AtomicU64>,
        exec_bodies: Arc<Mutex<Vec<Value>>>,
    }

    impl LeaseMockState {
        fn new(create_failure: (u16, Value), exec_exit_code: i64) -> Self {
            Self {
                create_failure,
                exec_exit_code,
                list_calls: Arc::default(),
                create_calls: Arc::default(),
                exec_bodies: Arc::default(),
            }
        }
    }

    fn lease_json(id: &str) -> Value {
        json!({
            "name": format!("{PARENT}/leases/{id}"),
            "spec": { "volumes": [{
                "volume": format!("{PARENT}/volumes/anyagents-agt-init"),
                "mount_path": DEFAULT_MOUNT_PATH,
            }] },
            "status": { "state": "ready" }
        })
    }

    async fn lease_mock_handler(
        State(st): State<LeaseMockState>,
        method: Method,
        uri: Uri,
        body: Bytes,
    ) -> (StatusCode, Json<Value>) {
        let path = uri.path().trim_start_matches("/v1/").to_string();
        let last = |p: &str| p.rsplit('/').next().unwrap_or_default().to_string();
        match (method.as_str(), path.as_str()) {
            ("GET", p) if p.contains("/volumes/") => (
                StatusCode::OK,
                Json(json!({ "name": p, "status": { "state": "available" } })),
            ),
            ("GET", p) if p.ends_with("/leases") => {
                // The pre-POST lookup sees nothing; the post-failure recovery lookup sees the
                // lease the timed-out / conflicting grant actually attached to the volume.
                let n = st.list_calls.fetch_add(1, Ordering::SeqCst);
                let leases = if n == 0 {
                    vec![]
                } else {
                    vec![lease_json("lease_recovered")]
                };
                (StatusCode::OK, Json(json!({ "leases": leases })))
            }
            ("GET", p) if p.contains("/leases/") => (StatusCode::OK, Json(lease_json(&last(p)))),
            ("POST", p) if p.ends_with("/leases") => {
                st.create_calls.fetch_add(1, Ordering::SeqCst);
                let (code, body) = st.create_failure.clone();
                (StatusCode::from_u16(code).unwrap(), Json(body))
            }
            ("POST", p) if p.ends_with(":renew") => (
                StatusCode::OK,
                Json(lease_json(&last(p.trim_end_matches(":renew")))),
            ),
            ("POST", p) if p.ends_with(":exec") => {
                let parsed = serde_json::from_slice(&body).unwrap_or_else(|_| json!({}));
                st.exec_bodies.lock().unwrap().push(parsed);
                (
                    StatusCode::OK,
                    Json(json!({ "exit_code": st.exec_exit_code, "stdout": "", "stderr": "" })),
                )
            }
            _ => (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "unmocked route" })),
            ),
        }
    }

    async fn spawn_lease_mock(st: LeaseMockState) -> (SylphxProvider, tokio::task::JoinHandle<()>) {
        let app = Router::new().fallback(lease_mock_handler).with_state(st);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        let mut cfg = SylphxConfig::new(PARENT);
        cfg.endpoint = format!("http://{addr}");
        (SylphxProvider::new(cfg).unwrap(), server)
    }

    /// The layout init must run as root, create `state/`, repair ownership, and stay
    /// idempotent / non-destructive (warm volumes skip recursive chown; nothing is deleted).
    fn assert_idempotent_layout_init(body: &Value) {
        assert_eq!(body["user"], "root", "{body}");
        let script = body["command"][2].as_str().expect("sh -lc script");
        assert!(script.contains("mkdir -p"), "{script}");
        assert!(script.contains("/home/user/agent-data/state"), "{script}");
        assert!(script.contains("chown -R user:user"), "{script}");
        assert!(script.contains("stat -c %U"), "{script}");
        assert!(!script.contains("rm -"), "{script}");
    }

    #[tokio::test]
    async fn ensure_initializes_layout_on_lease_recovered_after_create_timeout_or_409() {
        let failures = [
            (
                409,
                json!({ "code": "RESOURCE_IN_USE", "message": "volume is attached to another lease" }),
            ),
            (
                504,
                json!({ "code": "DEADLINE_EXCEEDED", "message": "lease grant timed out" }),
            ),
        ];
        for (code, body) in failures {
            let st = LeaseMockState::new((code, body), 0);
            let (provider, server) = spawn_lease_mock(st.clone()).await;
            let handle = provider
                .ensure(&EnsureSandboxRequest::for_agent("agt_init"))
                .await
                .unwrap_or_else(|e| panic!("HTTP {code} recovery failed: {e}"));
            assert_eq!(handle.sandbox_id, "lease_recovered");
            assert_eq!(handle.state_dir, "/home/user/agent-data/state");
            assert_eq!(st.create_calls.load(Ordering::SeqCst), 1);
            let execs = st.exec_bodies.lock().unwrap().clone();
            assert_eq!(execs.len(), 1, "HTTP {code}: layout init must run once");
            assert_idempotent_layout_init(&execs[0]);
            server.abort();
        }
    }

    #[tokio::test]
    async fn ensure_reattaching_active_lease_still_ensures_layout_non_destructively() {
        let st = LeaseMockState::new((500, json!({ "message": "create must not run" })), 0);
        let (provider, server) = spawn_lease_mock(st.clone()).await;
        let mut req = EnsureSandboxRequest::for_agent("agt_init");
        req.existing = Some(SandboxHandle {
            provider: ProviderKind::Sylphx,
            sandbox_id: "lease_warm".to_string(),
            volume_id: None,
            mount_path: DEFAULT_MOUNT_PATH.to_string(),
            state_dir: "/home/user/agent-data/state".to_string(),
            workdir: "/home/user/agent-data/workspace".to_string(),
            transport_url: None,
            transport_token: None,
            expires_at: None,
            metadata: BTreeMap::from([(
                "lease_resource".to_string(),
                format!("{PARENT}/leases/lease_warm"),
            )]),
        });

        // Attach twice: every re-attach re-runs the same idempotent init (no "already done" skip).
        for _ in 0..2 {
            let handle = provider.ensure(&req).await.unwrap();
            assert_eq!(handle.sandbox_id, "lease_warm");
        }
        assert_eq!(st.create_calls.load(Ordering::SeqCst), 0);
        assert_eq!(st.list_calls.load(Ordering::SeqCst), 0);
        let execs = st.exec_bodies.lock().unwrap().clone();
        assert_eq!(execs.len(), 2);
        // The per-call idempotency key differs by design; the init command must be identical.
        assert_eq!(execs[0]["command"], execs[1]["command"]);
        assert_idempotent_layout_init(&execs[0]);
        assert_idempotent_layout_init(&execs[1]);
        server.abort();
    }

    #[tokio::test]
    async fn ensure_fails_closed_when_layout_init_fails_on_recovered_lease() {
        let st = LeaseMockState::new((409, json!({ "code": "RESOURCE_IN_USE" })), 1);
        let (provider, server) = spawn_lease_mock(st.clone()).await;
        let err = provider
            .ensure(&EnsureSandboxRequest::for_agent("agt_init"))
            .await
            .expect_err("a lease without a verified layout must not be handed out");
        assert!(err.contains("persistent layout init failed"), "{err}");
        assert_eq!(st.exec_bodies.lock().unwrap().len(), 1);
        server.abort();
    }
}
