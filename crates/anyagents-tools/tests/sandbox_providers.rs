//! Comprehensive end-to-end wire-contract tests for all 8 [`SandboxProvider`] implementations,
//! [`ProviderRegistry`], `/home/user/agent-data` persistent volume preservation, and single-tarball
//! bundle sync.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};

use anyagents_tools::providers::{
    CloudflareConfig, CloudflareProvider, DEFAULT_IDLE_TTL_MS, DEFAULT_MAX_LEASE_TTL_MS,
    DEFAULT_MOUNT_PATH, DEFAULT_STATE_DIR, DEFAULT_WORKDIR, DaytonaConfig, DaytonaProvider,
    DockerConfig, DockerProvider, E2bConfig, E2bProvider, EnsureSandboxRequest, ExecRequest,
    GenericHttpOciConfig, GenericHttpOciProvider, ImageSpec, LeasePolicy, LocalHostProvider,
    ModalConfig, ModalProvider, NetworkPolicySpec, ProviderKind, ProviderRegistry, SandboxProvider,
    SandboxRegistryConfig, SylphxConfig, SylphxProvider, VolumeSpec, build_bundle_tar_gz,
    extract_bundle_tar_gz, persistent_layout_init_script,
};
use anyagents_tools::sylphx::{
    DEFAULT_LEASE_TTL, DEFAULT_MOUNT_PATH as SYLPHX_DEFAULT_MOUNT,
    DEFAULT_SANDBOX_WORKDIR as SYLPHX_DEFAULT_WORKDIR, DEFAULT_STATE_DIR as SYLPHX_DEFAULT_STATE,
    encode_connect_frame,
};
use anyagents_tools::{base64_decode, base64_encode};

#[derive(Clone, Debug)]
struct CapturedReq {
    method: String,
    path: String,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}

fn spawn_mock_http<F>(handler: F) -> String
where
    F: Fn(&CapturedReq) -> (u16, Vec<(String, String)>, Vec<u8>) + Send + Sync + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let handler = Arc::new(handler);
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let h = Arc::clone(&handler);
            std::thread::spawn(move || {
                let mut reader = BufReader::new(stream);
                let mut req_line = String::new();
                if reader.read_line(&mut req_line).is_err() || req_line.is_empty() {
                    return;
                }
                let mut parts = req_line.split_whitespace();
                let method = parts.next().unwrap_or("GET").to_string();
                let path = parts.next().unwrap_or("/").to_string();
                let mut headers = BTreeMap::new();
                let mut content_len = 0usize;
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).is_err() {
                        return;
                    }
                    if line == "\r\n" || line == "\n" || line.is_empty() {
                        break;
                    }
                    if let Some((k, v)) = line.split_once(':') {
                        let key = k.trim().to_ascii_lowercase();
                        let val = v.trim().to_string();
                        if key == "content-length" {
                            content_len = val.parse().unwrap_or(0);
                        }
                        headers.insert(key, val);
                    }
                }
                let mut body = vec![0u8; content_len];
                if content_len > 0 {
                    let _ = reader.read_exact(&mut body);
                }
                let captured = CapturedReq {
                    method,
                    path,
                    headers,
                    body,
                };
                let (status, resp_headers, resp_body) = h(&captured);
                let reason = match status {
                    200 => "OK",
                    201 => "Created",
                    204 => "No Content",
                    404 => "Not Found",
                    _ => "Error",
                };
                let mut stream = reader.into_inner();
                let mut head = format!(
                    "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: close\r\n",
                    resp_body.len()
                );
                for (k, v) in resp_headers {
                    head.push_str(&format!("{k}: {v}\r\n"));
                }
                head.push_str("\r\n");
                let _ = stream.write_all(head.as_bytes());
                let _ = stream.write_all(&resp_body);
                let _ = stream.flush();
            });
        }
    });
    format!("http://{addr}")
}

fn json_headers() -> Vec<(String, String)> {
    vec![("Content-Type".into(), "application/json".into())]
}

#[test]
fn persistent_layout_defaults_prevent_workspace_wipe_and_keep_compat_symlinks() {
    assert_eq!(DEFAULT_MOUNT_PATH, "/home/user/agent-data");
    assert_eq!(DEFAULT_STATE_DIR, "/home/user/agent-data/state");
    assert_eq!(DEFAULT_WORKDIR, "/home/user/agent-data/workspace");
    assert_eq!(SYLPHX_DEFAULT_MOUNT, "/home/user/agent-data");
    assert_eq!(SYLPHX_DEFAULT_STATE, "/home/user/agent-data/state");
    assert_eq!(SYLPHX_DEFAULT_WORKDIR, "/home/user/agent-data/workspace");
    assert_eq!(DEFAULT_LEASE_TTL, "900s");
    assert_eq!(DEFAULT_IDLE_TTL_MS, 300_000);
    assert_eq!(DEFAULT_MAX_LEASE_TTL_MS, 900_000);

    let vol = VolumeSpec::for_agent("agt_42");
    assert_eq!(vol.mount_path, "/home/user/agent-data");
    assert_eq!(vol.state_dir(), "/home/user/agent-data/state");
    assert_eq!(vol.workdir(), "/home/user/agent-data/workspace");

    let script =
        persistent_layout_init_script(DEFAULT_MOUNT_PATH, DEFAULT_STATE_DIR, DEFAULT_WORKDIR);
    assert!(script.contains("/home/user/agent-data/state"));
    assert!(script.contains("/home/user/agent-data/workspace"));
    assert!(script.contains("/home/user/agent-state"));
    assert!(script.contains("/home/user/workspace"));
}

#[test]
fn bundle_tar_roundtrip_and_path_traversal_rejection() {
    let entries = vec![
        ("SYSTEM_PROMPT.md".to_string(), b"# Prompt\n".to_vec()),
        (
            ".agents/skills/demo/SKILL.md".to_string(),
            b"# Skill\n".to_vec(),
        ),
        (".mcp-token".to_string(), b"tok-123\n".to_vec()),
    ];
    let tar_gz = build_bundle_tar_gz(&entries).unwrap();
    let dst = tempfile::tempdir().unwrap();
    let count = extract_bundle_tar_gz(&tar_gz, dst.path()).unwrap();
    assert_eq!(count, 3);
    assert_eq!(
        std::fs::read_to_string(dst.path().join("SYSTEM_PROMPT.md")).unwrap(),
        "# Prompt\n"
    );
    assert_eq!(
        std::fs::read_to_string(dst.path().join(".agents/skills/demo/SKILL.md")).unwrap(),
        "# Skill\n"
    );
    assert_eq!(
        std::fs::read_to_string(dst.path().join(".mcp-token")).unwrap(),
        "tok-123\n"
    );

    // Reject traversal & absolute paths.
    assert!(build_bundle_tar_gz(&[("../escape.txt".into(), b"bad".to_vec())]).is_err());
    assert!(build_bundle_tar_gz(&[("/etc/passwd".into(), b"bad".to_vec())]).is_err());
}

#[tokio::test]
async fn provider_1_local_host_preserves_workspace_across_release_and_syncs_tar() {
    let tmp = tempfile::tempdir().unwrap();
    let provider = LocalHostProvider::with_root(tmp.path());
    assert_eq!(provider.kind(), ProviderKind::Local);
    assert!(provider.capabilities().volume_survives_release);
    assert!(provider.health().await.unwrap().healthy);

    let req = EnsureSandboxRequest::for_agent("agt_local_1");
    let handle = provider.ensure(&req).await.unwrap();
    assert!(handle.state_dir.ends_with("/agent-data/state"));
    assert!(handle.workdir.ends_with("/agent-data/workspace"));

    // Write a workspace file via exec, sync a tar bundle into state_dir, release, and re-ensure.
    let out = provider
        .exec(
            &handle,
            &ExecRequest::shell("printf 'uncommitted edit' > draft.rs"),
        )
        .await
        .unwrap();
    assert_eq!(out.exit_code, 0);

    let tar_gz =
        build_bundle_tar_gz(&[("guest-config.json".into(), br#"{"ok":true}"#.to_vec())]).unwrap();
    provider
        .sync_bundle_tar(&handle, &handle.state_dir, &tar_gz)
        .await
        .unwrap();

    // Release must preserve BOTH state AND workspace!
    provider.release(&handle).await.unwrap();
    let mut re_req = EnsureSandboxRequest::for_agent("agt_local_1");
    re_req.existing = Some(handle.clone());
    let re_handle = provider.ensure(&re_req).await.unwrap();
    let check = provider
        .exec(&re_handle, &ExecRequest::shell("cat draft.rs"))
        .await
        .unwrap();
    assert_eq!(check.stdout, "uncommitted edit");
    assert_eq!(
        std::fs::read_to_string(
            std::path::Path::new(&re_handle.state_dir).join("guest-config.json")
        )
        .unwrap(),
        r#"{"ok":true}"#
    );

    // Destroy removes all local sandbox state.
    provider.destroy(&re_handle).await.unwrap();
    assert!(!std::path::Path::new(&re_handle.mount_path).exists());
}

#[tokio::test]
async fn provider_2_sylphx_supports_byo_oci_image_agent_data_tar_sync_and_set_network() {
    let log: Arc<Mutex<Vec<(String, serde_json::Value)>>> = Arc::new(Mutex::new(Vec::new()));
    let log_srv = Arc::clone(&log);
    let uploaded_tar: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));
    let tar_srv = Arc::clone(&uploaded_tar);

    let base_url = spawn_mock_http(move |req| {
        let json_body: serde_json::Value =
            serde_json::from_slice(&req.body).unwrap_or_else(|_| serde_json::json!({}));
        log_srv
            .lock()
            .unwrap()
            .push((format!("{} {}", req.method, req.path), json_body.clone()));

        assert_eq!(
            req.headers.get("authorization").map(String::as_str),
            Some("Bearer sylphx-tok")
        );
        match (req.method.as_str(), req.path.as_str()) {
            ("GET", "/v1/sandbox_shapes") => (
                200,
                json_headers(),
                br#"{"sandbox_shapes":[{"name":"sandbox_shapes/linux-m"}]}"#.to_vec(),
            ),
            ("GET", "/v1/orgs/o1/projects/p1/envs/e1/volumes/anyagents-agt-sylphx") => {
                (404, json_headers(), br#"{"error":"not found"}"#.to_vec())
            }
            ("POST", "/v1/orgs/o1/projects/p1/envs/e1/volumes?volume_id=anyagents-agt-sylphx") => (
                201,
                json_headers(),
                serde_json::to_vec(&serde_json::json!({
                    "name": "orgs/o1/projects/p1/envs/e1/volumes/vol_s1",
                    "spec": { "size_gib": 10 },
                    "status": { "state": "available" }
                }))
                .unwrap(),
            ),
            ("POST", "/v1/orgs/o1/projects/p1/envs/e1/leases") => (
                201,
                json_headers(),
                serde_json::to_vec(&serde_json::json!({
                    "name": "orgs/o1/projects/p1/envs/e1/leases/lease_s1",
                    "status": {
                        "state": "ready",
                        "expire_time": "2026-11-19T00:15:00Z",
                        "endpoints": {
                            "guest_uri": "https://envd.example.test/lease_s1"
                        }
                    }
                }))
                .unwrap(),
            ),
            ("POST", "/v1/orgs/o1/projects/p1/envs/e1/leases/lease_s1:exec") => (
                200,
                json_headers(),
                serde_json::to_vec(&serde_json::json!({
                    "stdout": base64_encode(b"sylphx-ok\n"),
                    "stderr": base64_encode(b""),
                    "exit_code": 0
                }))
                .unwrap(),
            ),
            ("POST", "/v1/orgs/o1/projects/p1/envs/e1/leases/lease_s1:writeFile") => {
                let b64 = json_body["content"].as_str().unwrap();
                *tar_srv.lock().unwrap() = Some(base64_decode(b64).unwrap());
                (200, json_headers(), br#"{}"#.to_vec())
            }
            ("POST", "/v1/orgs/o1/projects/p1/envs/e1/leases/lease_s1:setNetwork") => {
                (200, json_headers(), br#"{"updated":true}"#.to_vec())
            }
            ("POST", "/v1/orgs/o1/projects/p1/envs/e1/leases/lease_s1:release") => (
                200,
                json_headers(),
                serde_json::to_vec(&serde_json::json!({
                    "name": "orgs/o1/projects/p1/envs/e1/leases/lease_s1",
                    "status": { "state": "ended" }
                }))
                .unwrap(),
            ),
            ("DELETE", "/v1/orgs/o1/projects/p1/envs/e1/volumes/vol_s1") => {
                (200, json_headers(), br#"{"deleted":true}"#.to_vec())
            }
            _ => (404, json_headers(), br#"{"error":"unmocked"}"#.to_vec()),
        }
    });

    let mut cfg = SylphxConfig::new("orgs/o1/projects/p1/envs/e1");
    cfg.endpoint = base_url;
    cfg.token = Some("sylphx-tok".into());
    let provider = SylphxProvider::new(cfg).unwrap();
    assert_eq!(provider.kind(), ProviderKind::Sylphx);
    assert!(provider.capabilities().oci_image);
    assert!(provider.capabilities().live_network_mutation);
    assert!(provider.health().await.unwrap().healthy);

    let digest_image = "ghcr.io/sylphxai/anyagents-sandbox@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    let mut req = EnsureSandboxRequest::for_agent("agt_sylphx");
    req.image = ImageSpec::with_image(digest_image);
    assert!(req.image.is_digest_pinned());
    req.network =
        NetworkPolicySpec::Allowlist(vec!["api.github.com".into(), "203.0.113.0/24".into()]);
    req.lease = LeasePolicy::batch();

    let handle = provider.ensure(&req).await.unwrap();
    assert_eq!(handle.sandbox_id, "lease_s1");
    assert_eq!(handle.volume_id.as_deref(), Some("vol_s1"));
    assert_eq!(handle.mount_path, "/home/user/agent-data");
    assert_eq!(handle.state_dir, "/home/user/agent-data/state");
    assert_eq!(handle.workdir, "/home/user/agent-data/workspace");

    let out = provider
        .exec(&handle, &ExecRequest::shell("echo ok"))
        .await
        .unwrap();
    assert_eq!(out.stdout, "sylphx-ok\n");

    let tar_gz = build_bundle_tar_gz(&[("AGENTS.md".into(), b"# Rules\n".to_vec())]).unwrap();
    provider
        .sync_bundle_tar(&handle, &handle.state_dir, &tar_gz)
        .await
        .unwrap();
    assert_eq!(
        uploaded_tar.lock().unwrap().as_deref(),
        Some(tar_gz.as_slice())
    );

    provider
        .set_network(&handle, &NetworkPolicySpec::Deny)
        .await
        .unwrap();
    provider.release(&handle).await.unwrap();
    provider.destroy(&handle).await.unwrap();

    let calls = log.lock().unwrap();
    let create_vol = calls
        .iter()
        .find(|(r, _)| {
            r == "POST /v1/orgs/o1/projects/p1/envs/e1/volumes?volume_id=anyagents-agt-sylphx"
        })
        .unwrap();
    assert_eq!(create_vol.1["spec"]["size_gib"], 10);

    let create_lease = calls
        .iter()
        .find(|(r, _)| r == "POST /v1/orgs/o1/projects/p1/envs/e1/leases")
        .unwrap();
    assert_eq!(create_lease.1["spec"]["image"], digest_image);
    assert_eq!(create_lease.1["spec"]["ttl"], "900s");
    assert_eq!(
        create_lease.1["spec"]["volumes"][0]["mount_path"],
        "/home/user/agent-data"
    );
    assert_eq!(create_lease.1["spec"]["network"]["egress"], "allowlist");
    assert_eq!(
        create_lease.1["spec"]["network"]["allowed_domains"][0],
        "api.github.com"
    );
    assert_eq!(
        create_lease.1["spec"]["network"]["allowed_cidrs"][0],
        "203.0.113.0/24"
    );

    let set_net = calls
        .iter()
        .find(|(r, _)| r.ends_with(":setNetwork"))
        .unwrap();
    assert_eq!(set_net.1["network"]["egress"], "deny");
    assert!(calls.iter().any(|(r, _)| r.ends_with(":release")));
    assert!(calls.iter().any(|(r, _)| r.starts_with("DELETE ")));
}

#[tokio::test]
async fn provider_3_e2b_create_resume_pause_kill_envd_exec_and_tar_sync() {
    let calls: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let calls_srv = Arc::clone(&calls);

    let base_url = spawn_mock_http(move |req| {
        calls_srv
            .lock()
            .unwrap()
            .push(format!("{} {}", req.method, req.path));

        if req.path.ends_with("/process.Process/Start") {
            let mut frames = Vec::new();
            frames.extend(
                encode_connect_frame(
                    0x00,
                    &serde_json::to_vec(&serde_json::json!({"event": {"start": {"pid": 7}}}))
                        .unwrap(),
                )
                .unwrap(),
            );
            frames.extend(
                encode_connect_frame(
                    0x00,
                    &serde_json::to_vec(&serde_json::json!({
                        "event": {"data": {"stdout": base64_encode(b"e2b-out\n")}}
                    }))
                    .unwrap(),
                )
                .unwrap(),
            );
            frames.extend(
                encode_connect_frame(
                    0x00,
                    &serde_json::to_vec(&serde_json::json!({"event": {"end": {"exitCode": 0}}}))
                        .unwrap(),
                )
                .unwrap(),
            );
            frames.extend(encode_connect_frame(0x02, b"{}").unwrap());
            return (
                200,
                vec![("Content-Type".into(), "application/connect+json".into())],
                frames,
            );
        }
        if req.path.contains("/envd/") && req.path.contains("/files?") {
            return (200, json_headers(), br#"{"ok":true}"#.to_vec());
        }
        assert_eq!(
            req.headers.get("x-api-key").map(String::as_str),
            Some("e2b-key")
        );
        match (req.method.as_str(), req.path.as_str()) {
            ("GET", "/sandboxes") => (200, json_headers(), br#"[]"#.to_vec()),
            ("POST", "/sandboxes") => (
                201,
                json_headers(),
                serde_json::to_vec(&serde_json::json!({
                    "sandboxID": "e2b-sbx-99",
                    "clientID": "cli-1",
                    "envdAccessToken": "e2b-envd-tok"
                }))
                .unwrap(),
            ),
            ("POST", "/sandboxes/e2b-sbx-99/pause") => (200, json_headers(), br#"{}"#.to_vec()),
            ("POST", "/sandboxes/e2b-sbx-99/resume") => (
                200,
                json_headers(),
                serde_json::to_vec(&serde_json::json!({
                    "sandboxID": "e2b-sbx-99",
                    "envdAccessToken": "e2b-envd-tok-2"
                }))
                .unwrap(),
            ),
            ("DELETE", "/sandboxes/e2b-sbx-99") => (204, json_headers(), Vec::new()),
            _ => (404, json_headers(), br#"{"error":"unmocked"}"#.to_vec()),
        }
    });

    let provider = E2bProvider::new(E2bConfig {
        api_url: base_url.clone(),
        domain: "e2b.test".into(),
        api_key: Some("e2b-key".into()),
        default_template: "base".into(),
        envd_base_url: Some(format!("{base_url}/envd")),
    });
    assert_eq!(provider.kind(), ProviderKind::E2b);
    assert!(provider.capabilities().pause_resume);
    assert!(provider.health().await.unwrap().healthy);

    let req = EnsureSandboxRequest::for_agent("agt_e2b");
    let handle = provider.ensure(&req).await.unwrap();
    assert_eq!(handle.sandbox_id, "e2b-sbx-99");
    assert_eq!(handle.workdir, "/home/user/agent-data/workspace");

    let out = provider
        .exec(&handle, &ExecRequest::shell("uname"))
        .await
        .unwrap();
    assert_eq!(out.stdout, "e2b-out\n");

    let tar_gz = build_bundle_tar_gz(&[("a.txt".into(), b"hi".to_vec())]).unwrap();
    provider
        .sync_bundle_tar(&handle, &handle.state_dir, &tar_gz)
        .await
        .unwrap();

    // Pause (`release`), then resume (`ensure` with existing handle), then kill (`destroy`).
    provider.release(&handle).await.unwrap();
    let mut resume_req = EnsureSandboxRequest::for_agent("agt_e2b");
    resume_req.existing = Some(handle.clone());
    let resumed = provider.ensure(&resume_req).await.unwrap();
    assert_eq!(resumed.transport_token.as_deref(), Some("e2b-envd-tok-2"));
    provider.destroy(&resumed).await.unwrap();

    let recorded = calls.lock().unwrap();
    assert!(recorded.iter().any(|r| r == "POST /sandboxes"));
    assert!(
        recorded
            .iter()
            .any(|r| r == "POST /sandboxes/e2b-sbx-99/pause")
    );
    assert!(
        recorded
            .iter()
            .any(|r| r == "POST /sandboxes/e2b-sbx-99/resume")
    );
    assert!(recorded.iter().any(|r| r == "DELETE /sandboxes/e2b-sbx-99"));
}

#[tokio::test]
async fn provider_4_modal_oci_volume_exec_tar_terminate_and_destroy() {
    let calls: Arc<Mutex<Vec<(String, serde_json::Value)>>> = Arc::new(Mutex::new(Vec::new()));
    let calls_srv = Arc::clone(&calls);

    let base_url = spawn_mock_http(move |req| {
        assert_eq!(
            req.headers.get("modal-key").map(String::as_str),
            Some("ak-1")
        );
        assert_eq!(
            req.headers.get("modal-secret").map(String::as_str),
            Some("as-1")
        );
        let body: serde_json::Value =
            serde_json::from_slice(&req.body).unwrap_or_else(|_| serde_json::json!({}));
        calls_srv
            .lock()
            .unwrap()
            .push((format!("{} {}", req.method, req.path), body));

        if req.method == "PUT" && req.path.starts_with("/v1/sandboxes/sbx-modal-1/files?") {
            return (200, json_headers(), br#"{"ok":true}"#.to_vec());
        }
        match (req.method.as_str(), req.path.as_str()) {
            ("GET", "/v1/health") => (200, json_headers(), br#"{"ok":true}"#.to_vec()),
            ("POST", "/v1/volumes") => (
                200,
                json_headers(),
                br#"{"volume_id":"vol-modal-1"}"#.to_vec(),
            ),
            ("POST", "/v1/sandboxes") => (
                201,
                json_headers(),
                br#"{"sandbox_id":"sbx-modal-1"}"#.to_vec(),
            ),
            ("POST", "/v1/sandboxes/sbx-modal-1/exec") => (
                200,
                json_headers(),
                br#"{"stdout":"modal-ok\n","stderr":"","exit_code":0}"#.to_vec(),
            ),
            ("POST", "/v1/sandboxes/sbx-modal-1/terminate") => {
                (200, json_headers(), br#"{"terminated":true}"#.to_vec())
            }
            ("DELETE", "/v1/volumes/vol-modal-1") => {
                (200, json_headers(), br#"{"deleted":true}"#.to_vec())
            }
            _ => (404, json_headers(), br#"{"error":"unmocked"}"#.to_vec()),
        }
    });

    let provider = ModalProvider::new(ModalConfig {
        endpoint: base_url,
        token_id: Some("ak-1".into()),
        token_secret: Some("as-1".into()),
        bearer_token: None,
        app_name: "test-app".into(),
        default_image: "ghcr.io/org/sandbox:base".into(),
    });
    assert_eq!(provider.kind(), ProviderKind::Modal);
    assert!(provider.health().await.unwrap().healthy);

    let mut req = EnsureSandboxRequest::for_agent("agt_modal");
    req.image = ImageSpec::with_image("ghcr.io/org/custom:v2");
    req.network = NetworkPolicySpec::Deny;

    let handle = provider.ensure(&req).await.unwrap();
    assert_eq!(handle.sandbox_id, "sbx-modal-1");
    assert_eq!(handle.volume_id.as_deref(), Some("vol-modal-1"));
    assert_eq!(handle.mount_path, "/home/user/agent-data");

    let out = provider
        .exec(&handle, &ExecRequest::shell("echo hi"))
        .await
        .unwrap();
    assert_eq!(out.stdout, "modal-ok\n");

    let tar_gz = build_bundle_tar_gz(&[("cfg.json".into(), b"{}".to_vec())]).unwrap();
    provider
        .sync_bundle_tar(&handle, &handle.state_dir, &tar_gz)
        .await
        .unwrap();
    provider.release(&handle).await.unwrap();
    provider.destroy(&handle).await.unwrap();

    let recorded = calls.lock().unwrap();
    let create = recorded
        .iter()
        .find(|(r, _)| r == "POST /v1/sandboxes")
        .unwrap();
    assert_eq!(create.1["image"], "ghcr.io/org/custom:v2");
    assert_eq!(create.1["block_network"], true);
    assert_eq!(create.1["volumes"]["/home/user/agent-data"], "vol-modal-1");
    assert!(
        recorded
            .iter()
            .any(|(r, _)| r == "POST /v1/sandboxes/sbx-modal-1/terminate")
    );
    assert!(
        recorded
            .iter()
            .any(|(r, _)| r == "DELETE /v1/volumes/vol-modal-1")
    );
}

#[tokio::test]
async fn provider_5_daytona_volume_create_exec_tar_stop_start_and_delete() {
    let calls: Arc<Mutex<Vec<(String, serde_json::Value)>>> = Arc::new(Mutex::new(Vec::new()));
    let calls_srv = Arc::clone(&calls);

    let base_url = spawn_mock_http(move |req| {
        assert_eq!(
            req.headers.get("authorization").map(String::as_str),
            Some("Bearer day-key")
        );
        let body: serde_json::Value =
            serde_json::from_slice(&req.body).unwrap_or_else(|_| serde_json::json!({}));
        calls_srv
            .lock()
            .unwrap()
            .push((format!("{} {}", req.method, req.path), body));

        if req.method == "POST"
            && req
                .path
                .starts_with("/toolbox/day-sbx-1/toolbox/files/upload?")
        {
            return (200, json_headers(), br#"{"ok":true}"#.to_vec());
        }
        match (req.method.as_str(), req.path.as_str()) {
            ("GET", "/health") => (200, json_headers(), br#"{"status":"ok"}"#.to_vec()),
            ("GET", "/volumes/by-name/anyagents-agt-daytona") => {
                (404, json_headers(), br#"{"error":"not found"}"#.to_vec())
            }
            ("POST", "/volumes") => (201, json_headers(), br#"{"id":"day-vol-1"}"#.to_vec()),
            ("POST", "/sandbox") => (
                201,
                json_headers(),
                br#"{"id":"day-sbx-1","state":"started"}"#.to_vec(),
            ),
            ("GET", "/sandbox/day-sbx-1") => (
                200,
                json_headers(),
                br#"{"id":"day-sbx-1","state":"started"}"#.to_vec(),
            ),
            ("POST", "/toolbox/day-sbx-1/toolbox/process/execute") => (
                200,
                json_headers(),
                br#"{"result":"daytona-ok\n","exitCode":0}"#.to_vec(),
            ),
            ("POST", "/sandbox/day-sbx-1/stop") => (200, json_headers(), br#"{}"#.to_vec()),
            ("POST", "/sandbox/day-sbx-1/start") => (200, json_headers(), br#"{}"#.to_vec()),
            ("DELETE", "/sandbox/day-sbx-1") => (200, json_headers(), br#"{}"#.to_vec()),
            ("DELETE", "/volumes/day-vol-1") => (200, json_headers(), br#"{}"#.to_vec()),
            _ => (404, json_headers(), br#"{"error":"unmocked"}"#.to_vec()),
        }
    });

    let provider = DaytonaProvider::new(DaytonaConfig {
        api_url: base_url,
        api_key: Some("day-key".into()),
        target: Some("eu".into()),
        default_image: Some("ghcr.io/org/sandbox:base".into()),
        default_snapshot: None,
    });
    assert_eq!(provider.kind(), ProviderKind::Daytona);
    assert!(provider.health().await.unwrap().healthy);

    let req = EnsureSandboxRequest::for_agent("agt_daytona");
    let handle = provider.ensure(&req).await.unwrap();
    assert_eq!(handle.sandbox_id, "day-sbx-1");
    assert_eq!(handle.volume_id.as_deref(), Some("day-vol-1"));
    assert_eq!(handle.mount_path, "/home/user/agent-data");

    let out = provider
        .exec(&handle, &ExecRequest::shell("echo hi"))
        .await
        .unwrap();
    assert_eq!(out.stdout, "daytona-ok\n");

    let tar_gz = build_bundle_tar_gz(&[("b.txt".into(), b"hello".to_vec())]).unwrap();
    provider
        .sync_bundle_tar(&handle, &handle.state_dir, &tar_gz)
        .await
        .unwrap();

    // Stop (`release`), restart (`ensure` with existing), then destroy.
    provider.release(&handle).await.unwrap();
    let mut start_req = EnsureSandboxRequest::for_agent("agt_daytona");
    start_req.existing = Some(handle.clone());
    let restarted = provider.ensure(&start_req).await.unwrap();
    provider.destroy(&restarted).await.unwrap();

    let recorded = calls.lock().unwrap();
    let create = recorded.iter().find(|(r, _)| r == "POST /sandbox").unwrap();
    assert_eq!(create.1["volumes"][0]["mountPath"], "/home/user/agent-data");
    assert_eq!(create.1["volumes"][0]["volumeId"], "day-vol-1");
    assert!(
        recorded
            .iter()
            .any(|(r, _)| r == "POST /sandbox/day-sbx-1/stop")
    );
    assert!(
        recorded
            .iter()
            .any(|(r, _)| r == "POST /sandbox/day-sbx-1/start")
    );
    assert!(
        recorded
            .iter()
            .any(|(r, _)| r == "DELETE /sandbox/day-sbx-1")
    );
    assert!(
        recorded
            .iter()
            .any(|(r, _)| r == "DELETE /volumes/day-vol-1")
    );
}

#[tokio::test]
async fn provider_6_docker_cli_volume_mount_tar_stream_release_and_destroy() {
    let tmp = tempfile::tempdir().unwrap();
    let log_file = tmp.path().join("docker.log");
    let tar_capture = tmp.path().join("captured.tar.gz");
    let fake_docker = tmp.path().join("fake-docker");

    std::fs::write(
        &fake_docker,
        format!(
            r#"#!/bin/sh
echo "$@" >> "{log}"
case "$1" in
  info)
    echo "27.3.1"
    exit 0
    ;;
  volume)
    echo "$3"
    exit 0
    ;;
  run)
    echo "ctr-docker-123"
    exit 0
    ;;
  inspect)
    echo "true"
    exit 0
    ;;
  exec)
    if [ "$2" = "-i" ]; then
      cat > "{tar}"
      echo "streamed"
      exit 0
    fi
    echo "docker-exec-ok"
    exit 0
    ;;
  rm)
    exit 0
    ;;
esac
exit 0
"#,
            log = log_file.display(),
            tar = tar_capture.display(),
        ),
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&fake_docker, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    let provider = DockerProvider::new(DockerConfig {
        docker_host: None,
        docker_executable: Some(fake_docker),
        default_image: "ghcr.io/org/sandbox:base".into(),
        default_network: None,
        memory_limit: Some("2g".into()),
        cpus: Some("2".into()),
    });
    assert_eq!(provider.kind(), ProviderKind::Docker);
    let health = provider.health().await.unwrap();
    assert!(health.healthy);
    assert!(health.message.contains("27.3.1"));

    let mut req = EnsureSandboxRequest::for_agent("agt_docker");
    req.network = NetworkPolicySpec::Deny;
    let handle = provider.ensure(&req).await.unwrap();
    assert_eq!(handle.sandbox_id, "ctr-docker-123");
    assert_eq!(handle.mount_path, "/home/user/agent-data");
    assert_eq!(handle.workdir, "/home/user/agent-data/workspace");

    let out = provider
        .exec(&handle, &ExecRequest::shell("echo ok"))
        .await
        .unwrap();
    assert_eq!(out.stdout.trim(), "docker-exec-ok");

    let tar_gz = build_bundle_tar_gz(&[("hello.txt".into(), b"docker-tar".to_vec())]).unwrap();
    provider
        .sync_bundle_tar(&handle, &handle.state_dir, &tar_gz)
        .await
        .unwrap();
    assert_eq!(std::fs::read(&tar_capture).unwrap(), tar_gz);

    // Release removes container ONLY (volume survives!).
    provider.release(&handle).await.unwrap();
    let log_after_release = std::fs::read_to_string(&log_file).unwrap();
    assert!(log_after_release.contains("volume create anyagents-agt-docker"));
    assert!(log_after_release.contains("-v anyagents-agt-docker:/home/user/agent-data"));
    assert!(log_after_release.contains("--network none"));
    assert!(log_after_release.contains("rm -f ctr-docker-123"));
    assert!(!log_after_release.contains("volume rm"));

    // Destroy deletes container AND named volume.
    provider.destroy(&handle).await.unwrap();
    let log_after_destroy = std::fs::read_to_string(&log_file).unwrap();
    assert!(log_after_destroy.contains("volume rm -f anyagents-agt-docker"));
}

#[tokio::test]
async fn provider_7_cloudflare_worker_lifecycle_exec_tar_sleep_and_destroy() {
    let calls: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let calls_srv = Arc::clone(&calls);
    let captured_tar: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
    let tar_srv = Arc::clone(&captured_tar);

    let base_url = spawn_mock_http(move |req| {
        assert_eq!(
            req.headers
                .get("x-cloudflare-account-id")
                .map(String::as_str),
            Some("acct-1")
        );
        assert_eq!(
            req.headers.get("authorization").map(String::as_str),
            Some("Bearer cf-tok")
        );
        calls_srv
            .lock()
            .unwrap()
            .push(format!("{} {}", req.method, req.path));

        if req.method == "PUT" && req.path.starts_with("/v1/sandboxes/cf-sbx-1/fs/tar?") {
            *tar_srv.lock().unwrap() = req.body.clone();
            return (200, json_headers(), br#"{"ok":true}"#.to_vec());
        }
        match (req.method.as_str(), req.path.as_str()) {
            ("GET", "/health") => (200, json_headers(), br#"{"ok":true}"#.to_vec()),
            ("POST", "/v1/sandboxes") => (
                200,
                json_headers(),
                br#"{"id":"cf-sbx-1","volume_id":"cf-vol-1"}"#.to_vec(),
            ),
            ("POST", "/v1/sandboxes/cf-sbx-1/exec") => (
                200,
                json_headers(),
                br#"{"stdout":"cf-ok\n","stderr":"","exit_code":0}"#.to_vec(),
            ),
            ("POST", "/v1/sandboxes/cf-sbx-1/sleep") => {
                (200, json_headers(), br#"{"sleeping":true}"#.to_vec())
            }
            ("DELETE", "/v1/sandboxes/cf-sbx-1") => {
                (200, json_headers(), br#"{"deleted":true}"#.to_vec())
            }
            _ => (404, json_headers(), br#"{"error":"unmocked"}"#.to_vec()),
        }
    });

    let mut cfg = CloudflareConfig::new(base_url);
    cfg.api_token = Some("cf-tok".into());
    cfg.account_id = Some("acct-1".into());
    let provider = CloudflareProvider::new(cfg).unwrap();
    assert_eq!(provider.kind(), ProviderKind::Cloudflare);
    assert!(provider.health().await.unwrap().healthy);

    let handle = provider
        .ensure(&EnsureSandboxRequest::for_agent("agt_cf"))
        .await
        .unwrap();
    assert_eq!(handle.sandbox_id, "cf-sbx-1");
    assert_eq!(handle.mount_path, "/home/user/agent-data");

    let out = provider
        .exec(&handle, &ExecRequest::shell("echo ok"))
        .await
        .unwrap();
    assert_eq!(out.stdout, "cf-ok\n");

    let tar_gz = build_bundle_tar_gz(&[("c.txt".into(), b"cf-bundle".to_vec())]).unwrap();
    provider
        .sync_bundle_tar(&handle, &handle.state_dir, &tar_gz)
        .await
        .unwrap();
    assert_eq!(*captured_tar.lock().unwrap(), tar_gz);

    provider.release(&handle).await.unwrap();
    provider.destroy(&handle).await.unwrap();

    let recorded = calls.lock().unwrap();
    assert!(
        recorded
            .iter()
            .any(|r| r == "POST /v1/sandboxes/cf-sbx-1/sleep")
    );
    assert!(
        recorded
            .iter()
            .any(|r| r == "DELETE /v1/sandboxes/cf-sbx-1")
    );
}

#[tokio::test]
async fn provider_8_generic_http_oci_full_contract_and_network_mutation() {
    let calls: Arc<Mutex<Vec<(String, serde_json::Value)>>> = Arc::new(Mutex::new(Vec::new()));
    let calls_srv = Arc::clone(&calls);
    let captured_tar: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
    let tar_srv = Arc::clone(&captured_tar);

    let base_url = spawn_mock_http(move |req| {
        if req.method == "POST" && req.path.starts_with("/v1/sandboxes/http-sbx-1/bundle?") {
            *tar_srv.lock().unwrap() = req.body.clone();
            calls_srv.lock().unwrap().push((
                format!("{} {}", req.method, req.path),
                serde_json::json!({}),
            ));
            return (200, json_headers(), br#"{"ok":true}"#.to_vec());
        }
        let body: serde_json::Value =
            serde_json::from_slice(&req.body).unwrap_or_else(|_| serde_json::json!({}));
        calls_srv
            .lock()
            .unwrap()
            .push((format!("{} {}", req.method, req.path), body));

        if req.method == "DELETE" && req.path.starts_with("/v1/sandboxes/http-sbx-1") {
            return (200, json_headers(), br#"{"deleted":true}"#.to_vec());
        }
        assert_eq!(
            req.headers.get("authorization").map(String::as_str),
            Some("Bearer http-tok")
        );
        match (req.method.as_str(), req.path.as_str()) {
            ("GET", "/v1/health") => (
                200,
                json_headers(),
                br#"{"healthy":true,"message":"custom-oci-ready"}"#.to_vec(),
            ),
            ("POST", "/v1/sandboxes/ensure") => (
                200,
                json_headers(),
                serde_json::to_vec(&serde_json::json!({
                    "sandbox_id": "http-sbx-1",
                    "volume_id": "http-vol-1",
                    "mount_path": "/home/user/agent-data",
                    "state_dir": "/home/user/agent-data/state",
                    "workdir": "/home/user/agent-data/workspace"
                }))
                .unwrap(),
            ),
            ("POST", "/v1/sandboxes/http-sbx-1/exec") => (
                200,
                json_headers(),
                br#"{"stdout":"http-oci-ok\n","stderr":"","exit_code":0}"#.to_vec(),
            ),
            ("POST", "/v1/sandboxes/http-sbx-1/network") => {
                (200, json_headers(), br#"{"ok":true}"#.to_vec())
            }
            ("POST", "/v1/sandboxes/http-sbx-1/release") => {
                (200, json_headers(), br#"{"released":true}"#.to_vec())
            }
            _ => (404, json_headers(), br#"{"error":"unmocked"}"#.to_vec()),
        }
    });

    let mut cfg = GenericHttpOciConfig::new(base_url);
    cfg.token = Some("http-tok".into());
    let provider = GenericHttpOciProvider::new(cfg).unwrap();
    assert_eq!(provider.kind(), ProviderKind::Http);
    let h = provider.health().await.unwrap();
    assert!(h.healthy);
    assert_eq!(h.message, "custom-oci-ready");

    let mut req = EnsureSandboxRequest::for_agent("agt_http");
    req.image = ImageSpec::with_image(
        "ghcr.io/org/custom-sandbox@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    );
    let handle = provider.ensure(&req).await.unwrap();
    assert_eq!(handle.sandbox_id, "http-sbx-1");
    assert_eq!(handle.volume_id.as_deref(), Some("http-vol-1"));

    let out = provider
        .exec(&handle, &ExecRequest::shell("id"))
        .await
        .unwrap();
    assert_eq!(out.stdout, "http-oci-ok\n");

    let tar_gz = build_bundle_tar_gz(&[("bundle.txt".into(), b"http-tar".to_vec())]).unwrap();
    provider
        .sync_bundle_tar(&handle, &handle.state_dir, &tar_gz)
        .await
        .unwrap();
    assert_eq!(*captured_tar.lock().unwrap(), tar_gz);

    provider
        .set_network(
            &handle,
            &NetworkPolicySpec::Allowlist(vec!["registry.npmjs.org".into()]),
        )
        .await
        .unwrap();
    provider.release(&handle).await.unwrap();
    provider.destroy(&handle).await.unwrap();

    let recorded = calls.lock().unwrap();
    assert!(
        recorded
            .iter()
            .any(|(r, _)| r == "POST /v1/sandboxes/http-sbx-1/network")
    );
    assert!(
        recorded
            .iter()
            .any(|(r, _)| r == "POST /v1/sandboxes/http-sbx-1/release")
    );
    assert!(
        recorded
            .iter()
            .any(|(r, _)| r.starts_with("DELETE /v1/sandboxes/http-sbx-1"))
    );
}

#[test]
fn provider_registry_resolves_all_eight_providers_and_defaults() {
    // 1. Default -> Local
    let reg_local = ProviderRegistry::from_config(&SandboxRegistryConfig::default()).unwrap();
    assert_eq!(reg_local.active_kind(), ProviderKind::Local);
    assert_eq!(reg_local.default_lease_policy().idle_ttl_ms, 300_000);
    assert_eq!(reg_local.default_lease_policy().max_lease_ttl_ms, 900_000);
    let default_req = reg_local.default_ensure_request("agt_1");
    assert_eq!(
        default_req.workdir.as_deref(),
        Some("/home/user/agent-data/workspace")
    );
    assert_eq!(
        default_req.volume.as_ref().unwrap().mount_path,
        "/home/user/agent-data"
    );

    // 2. Sylphx
    let reg_sylphx = ProviderRegistry::from_config(&SandboxRegistryConfig {
        provider: Some("sylphx".into()),
        parent: Some("orgs/o/projects/p/envs/e".into()),
        image: Some("ghcr.io/org/sbx@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into()),
        idle_ttl_ms: Some(0),
        ..Default::default()
    })
    .unwrap();
    assert_eq!(reg_sylphx.active_kind(), ProviderKind::Sylphx);
    assert_eq!(reg_sylphx.default_lease_policy().idle_ttl_ms, 0);
    assert!(reg_sylphx.default_image_spec().is_digest_pinned());

    // 3. E2B
    let reg_e2b = ProviderRegistry::from_config(&SandboxRegistryConfig {
        provider: Some("e2b".into()),
        token: Some("k".into()),
        ..Default::default()
    })
    .unwrap();
    assert_eq!(reg_e2b.active_kind(), ProviderKind::E2b);

    // 4. Modal
    let reg_modal = ProviderRegistry::from_config(&SandboxRegistryConfig {
        provider: Some("modal".into()),
        endpoint: Some("https://modal.example.test".into()),
        ..Default::default()
    })
    .unwrap();
    assert_eq!(reg_modal.active_kind(), ProviderKind::Modal);

    // 5. Daytona
    let reg_daytona = ProviderRegistry::from_config(&SandboxRegistryConfig {
        provider: Some("daytona".into()),
        token: Some("k".into()),
        ..Default::default()
    })
    .unwrap();
    assert_eq!(reg_daytona.active_kind(), ProviderKind::Daytona);

    // 6. Docker
    let reg_docker = ProviderRegistry::from_config(&SandboxRegistryConfig {
        provider: Some("docker".into()),
        image: Some("debian:bookworm-slim".into()),
        ..Default::default()
    })
    .unwrap();
    assert_eq!(reg_docker.active_kind(), ProviderKind::Docker);

    // 7. Cloudflare
    let reg_cf = ProviderRegistry::from_config(&SandboxRegistryConfig {
        provider: Some("cloudflare".into()),
        endpoint: Some("https://sbx.example.workers.dev".into()),
        ..Default::default()
    })
    .unwrap();
    assert_eq!(reg_cf.active_kind(), ProviderKind::Cloudflare);

    // 8. Generic HTTP OCI via file config (with env overrides verified too)
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(
        tmp.path().join("sandbox-provider.json"),
        serde_json::json!({
            "provider": "http",
            "endpoint": "https://oci.example.test",
            "image": "ghcr.io/org/custom:v1",
            "idle_ttl_ms": 60000
        })
        .to_string(),
    )
    .unwrap();

    let env_keys = [
        "ANYAGENTS_SANDBOX_PROVIDER",
        "ANYAGENTS_SANDBOX_DRIVER",
        "ANYAGENTS_SANDBOX_IMAGE",
        "ANYAGENTS_SANDBOX_TEMPLATE",
        "ANYAGENTS_SANDBOX_IDLE_TTL_MS",
        "ANYAGENTS_SANDBOX_MAX_LEASE_TTL_MS",
        "ANYAGENTS_SANDBOX_HTTP_URL",
        "ANYAGENTS_SANDBOX_PROVIDER_URL",
        "ANYAGENTS_SANDBOX_ENDPOINT",
        "ANYAGENTS_SANDBOX_TOKEN",
        "ANYAGENTS_SYLPHX_PARENT",
        "E2B_API_KEY",
        "DAYTONA_API_KEY",
        "ANYAGENTS_MODAL_ENDPOINT",
        "MODAL_SANDBOX_URL",
        "ANYAGENTS_CLOUDFLARE_SANDBOX_URL",
        "CLOUDFLARE_SANDBOX_URL",
    ];
    let saved: Vec<(&str, Option<String>)> = env_keys
        .iter()
        .map(|k| (*k, std::env::var(k).ok()))
        .collect();
    for k in env_keys {
        unsafe { std::env::remove_var(k) };
    }

    let reg_http = ProviderRegistry::from_env_or_config(Some(tmp.path()), None).unwrap();
    assert_eq!(reg_http.active_kind(), ProviderKind::Http);
    assert_eq!(reg_http.default_lease_policy().idle_ttl_ms, 60_000);
    assert_eq!(
        reg_http.default_image_spec().image.as_deref(),
        Some("ghcr.io/org/custom:v1")
    );

    // Verify `ANYAGENTS_SANDBOX_*` environment variables override file config cleanly.
    unsafe {
        std::env::set_var("ANYAGENTS_SANDBOX_PROVIDER", "docker");
        std::env::set_var(
            "ANYAGENTS_SANDBOX_IMAGE",
            "ghcr.io/org/env-image@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        );
        std::env::set_var("ANYAGENTS_SANDBOX_TEMPLATE", "custom-tpl");
        std::env::set_var("ANYAGENTS_SANDBOX_IDLE_TTL_MS", "120000");
        std::env::set_var("ANYAGENTS_SANDBOX_MAX_LEASE_TTL_MS", "600000");
    }
    let reg_env = ProviderRegistry::from_env_or_config(Some(tmp.path()), None).unwrap();
    assert_eq!(reg_env.active_kind(), ProviderKind::Docker);
    assert!(reg_env.default_image_spec().is_digest_pinned());
    assert_eq!(
        reg_env.default_image_spec().template.as_deref(),
        Some("custom-tpl")
    );
    assert_eq!(reg_env.default_lease_policy().idle_ttl_ms, 120_000);
    assert_eq!(reg_env.default_lease_policy().max_lease_ttl_ms, 600_000);

    for (k, val) in saved {
        match val {
            Some(v) => unsafe { std::env::set_var(k, v) },
            None => unsafe { std::env::remove_var(k) },
        }
    }
}
