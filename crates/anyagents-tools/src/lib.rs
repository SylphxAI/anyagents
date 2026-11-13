//! Shared capability core and vendor-neutral [`WorkspaceBackend`] trait (`Sandbox` axis).
//!
//! By default, [`LocalWorkspace`] executes file and command operations against a local directory
//! or git worktree on the orchestrator host. Pluggable isolated backends—[`RemoteSandboxWorkspace`]
//! (Cloud Sandbox / E2B HTTP APIs), [`DockerWorkspace`] (local/remote Docker containers via
//! `docker://<host>` or direct construction), or custom [`WorkspaceBackend`] trait implementations—

#[allow(unused_imports)]
use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
use sha2::{Digest, Sha256};
#[allow(unused_imports)]
use std::io::Read as _;
#[allow(unused_imports)]
use std::path::{Component, Path, PathBuf};
#[allow(unused_imports)]
use std::time::Duration;

pub mod web;

mod encoding;
mod path;
mod sandbox;
mod workspace;

pub use encoding::{base64_decode, base64_encode, sha256_hex};
pub use path::{normalize_lexical_path, resolve_workspace_path};
pub use sandbox::{SandboxConfig, SandboxProvider, SandboxProviderConfig};
pub use workspace::{
    DockerWorkspace, ExecEnv, ExecOutput, LocalWorkspace, RemoteSandboxWorkspace, WorkspaceBackend,
    resolve_workspace_backend,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sandbox_config_loads_from_json_and_local_host_backend_executes() {
        let tmp = std::env::temp_dir().join(format!("anyagents-tools-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        let cfg_dir = tmp.join("cfg");
        let work_dir = tmp.join("work");
        std::fs::create_dir_all(&cfg_dir).unwrap();
        std::fs::create_dir_all(&work_dir).unwrap();

        assert!(SandboxConfig::from_config_dir(&cfg_dir).is_none());

        std::fs::write(
            cfg_dir.join("sandbox.json"),
            r#"{"endpoint":"https://sandbox.example.test/","sandbox_id":"sbx-1","token":"tok-1","workdir":"/workspace/"}"#,
        )
        .unwrap();

        let loaded = SandboxConfig::from_config_dir(&cfg_dir).expect("should parse sandbox.json");
        assert_eq!(loaded.endpoint, "https://sandbox.example.test");
        assert_eq!(loaded.sandbox_id, "sbx-1");
        assert_eq!(loaded.token.as_deref(), Some("tok-1"));
        assert_eq!(loaded.workdir, "/workspace");

        let local = LocalWorkspace::new(work_dir.clone());
        let (_, written, _) = local
            .write_file_bytes(Some("sub/note.txt"), b"hello local", false, true)
            .unwrap();
        assert_eq!(written, 11);
        let (_, read_back) = local.read_file_bytes(Some("sub/note.txt")).unwrap();
        assert_eq!(read_back, b"hello local");
        let exec_res = local
            .exec("printf 'ok-%s' local", Some("sub"), Some(5_000))
            .unwrap();
        assert_eq!(exec_res.exit_code, 0);
        assert_eq!(exec_res.stdout, "ok-local");

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn sandbox_provider_provisions_pauses_resumes_destroys_and_memory_store_works_remotely() {
        use std::collections::HashMap;
        use std::io::{Read, Write};
        use std::net::TcpListener;
        use std::sync::{Arc, Mutex};

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let events: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let remote_files: Arc<Mutex<HashMap<String, Vec<u8>>>> =
            Arc::new(Mutex::new(HashMap::new()));

        let events_srv = Arc::clone(&events);
        let files_srv = Arc::clone(&remote_files);
        let handle = std::thread::spawn(move || {
            for _ in 0..6 {
                let Ok((mut stream, _)) = listener.accept() else {
                    break;
                };
                let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
                let mut data = Vec::new();
                let mut chunk = [0u8; 2048];
                loop {
                    match stream.read(&mut chunk) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            data.extend_from_slice(&chunk[..n]);
                            if let Some(hdr_end) = data.windows(4).position(|w| w == b"\r\n\r\n") {
                                let body_start = hdr_end + 4;
                                let hdr_str = String::from_utf8_lossy(&data[..hdr_end]);
                                let content_len = hdr_str
                                    .lines()
                                    .find_map(|line| {
                                        let (k, v) = line.split_once(':')?;
                                        k.trim()
                                            .eq_ignore_ascii_case("content-length")
                                            .then(|| v.trim().parse::<usize>().ok())
                                            .flatten()
                                    })
                                    .unwrap_or(0);
                                if data.len() >= body_start + content_len {
                                    break;
                                }
                            }
                        }
                    }
                }
                let hdr_end = data
                    .windows(4)
                    .position(|w| w == b"\r\n\r\n")
                    .unwrap_or(data.len());
                let req_head = String::from_utf8_lossy(&data[..hdr_end]);
                let first_line = req_head.lines().next().unwrap_or("");
                let body_bytes = if hdr_end + 4 <= data.len() {
                    &data[hdr_end + 4..]
                } else {
                    &[][..]
                };

                let (status_line, resp_body) = if first_line.starts_with("POST /v1/sandboxes ") {
                    events_srv.lock().unwrap().push("provision".into());
                    (
                        "HTTP/1.1 201 Created",
                        serde_json::json!({
                            "sandbox_id": "sbx-prov-42",
                            "workdir": "/workspace",
                            "remote_spawn": true
                        })
                        .to_string(),
                    )
                } else if first_line.starts_with("POST /v1/sandboxes/sbx-prov-42/pause ") {
                    events_srv.lock().unwrap().push("pause".into());
                    ("HTTP/1.1 200 OK", r#"{"ok":true}"#.to_string())
                } else if first_line.starts_with("POST /v1/sandboxes/sbx-prov-42/resume ") {
                    events_srv.lock().unwrap().push("resume".into());
                    ("HTTP/1.1 200 OK", r#"{"ok":true}"#.to_string())
                } else if first_line.starts_with("DELETE /v1/sandboxes/sbx-prov-42 ") {
                    events_srv.lock().unwrap().push("destroy".into());
                    ("HTTP/1.1 200 OK", r#"{"ok":true}"#.to_string())
                } else if first_line.starts_with("PUT /v1/sandboxes/sbx-prov-42/files?path=") {
                    let path_part = first_line
                        .split_whitespace()
                        .nth(1)
                        .unwrap_or("")
                        .strip_prefix("/v1/sandboxes/sbx-prov-42/files?path=")
                        .unwrap_or("");
                    files_srv
                        .lock()
                        .unwrap()
                        .insert(path_part.to_string(), body_bytes.to_vec());
                    ("HTTP/1.1 200 OK", r#"{"ok":true}"#.to_string())
                } else if first_line.starts_with("GET /v1/sandboxes/sbx-prov-42/files?path=")
                    && first_line.contains("&list=true")
                {
                    let body = serde_json::json!({
                        "entries": [
                            { "name": "roadmap.md", "path": "memory/topics/roadmap.md", "kind": "file", "bytes": 12 }
                        ]
                    })
                    .to_string();
                    ("HTTP/1.1 200 OK", body)
                } else if first_line.starts_with("GET /v1/sandboxes/sbx-prov-42/files?path=") {
                    let path_part = first_line
                        .split_whitespace()
                        .nth(1)
                        .unwrap_or("")
                        .strip_prefix("/v1/sandboxes/sbx-prov-42/files?path=")
                        .unwrap_or("");
                    let content = files_srv
                        .lock()
                        .unwrap()
                        .get(path_part)
                        .cloned()
                        .unwrap_or_default();
                    let body = serde_json::json!({
                        "content": String::from_utf8_lossy(&content),
                        "encoding": "utf-8"
                    })
                    .to_string();
                    ("HTTP/1.1 200 OK", body)
                } else {
                    (
                        "HTTP/1.1 404 Not Found",
                        r#"{"error":"not found"}"#.to_string(),
                    )
                };

                let response = format!(
                    "{status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    resp_body.len(),
                    resp_body
                );
                let _ = stream.write_all(response.as_bytes());
                let _ = stream.flush();
            }
        });

        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().join("data");
        let cfg_dir = dir.path().join("cfg");
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::write(
            data_dir.join("sandbox-provider.json"),
            serde_json::json!({
                "endpoint": format!("http://{addr}"),
                "token": "prov-tok-1",
                "default_workdir": "/workspace",
                "remote_spawn": true
            })
            .to_string(),
        )
        .unwrap();

        let provider = SandboxProviderConfig::resolve(Some(&data_dir)).unwrap();
        let sbx = SandboxProvider::provision(&provider, "agt_1", "builder", Some("org/repo"))
            .expect("provision should succeed");
        assert_eq!(sbx.sandbox_id, "sbx-prov-42");
        assert!(sbx.remote_spawn);
        sbx.write_to_config_dir(&cfg_dir).unwrap();
        let reloaded = SandboxConfig::from_config_dir(&cfg_dir).unwrap();
        assert_eq!(reloaded, sbx);

        // Verify WorkspaceBackend::RemoteSandbox read/write
        let backend = RemoteSandboxWorkspace::new(sbx.clone());
        backend
            .write_file_bytes(Some("notes.md"), b"# Remote Sandbox\n", false, true)
            .unwrap();
        assert_eq!(
            backend.read_file_bytes(Some("notes.md")).unwrap().1,
            b"# Remote Sandbox\n"
        );

        SandboxProvider::pause(&sbx).unwrap();
        SandboxProvider::resume(&sbx).unwrap();
        SandboxProvider::destroy(&sbx).unwrap();

        let _ = handle.join();
        assert_eq!(
            *events.lock().unwrap(),
            vec!["provision", "pause", "resume", "destroy"]
        );
    }
    #[test]
    fn workspace_backend_trait_supports_local_cloud_e2b_docker_and_custom_drivers() {
        struct InMemoryBackend {
            root: PathBuf,
            files: std::sync::Mutex<std::collections::HashMap<String, Vec<u8>>>,
        }
        impl WorkspaceBackend for InMemoryBackend {
            fn kind(&self) -> &'static str {
                "in-memory"
            }
            fn base_path(&self) -> &Path {
                &self.root
            }
            fn exec(
                &self,
                command: &str,
                _cwd: Option<&str>,
                _timeout_ms: Option<u64>,
            ) -> Result<ExecOutput, String> {
                Ok(ExecOutput {
                    stdout: format!("mem:{command}"),
                    stderr: String::new(),
                    exit_code: 0,
                })
            }
            fn read_file_bytes(&self, path: Option<&str>) -> Result<(String, Vec<u8>), String> {
                let resolved = self.resolve_path(path, true)?;
                let bytes = self
                    .files
                    .lock()
                    .unwrap()
                    .get(&resolved)
                    .cloned()
                    .ok_or_else(|| format!("missing {resolved}"))?;
                Ok((resolved, bytes))
            }
            fn write_file_bytes(
                &self,
                path: Option<&str>,
                content: &[u8],
                _append: bool,
                _overwrite: bool,
            ) -> Result<(String, usize, String), String> {
                let resolved = self.resolve_path(path, true)?;
                self.files
                    .lock()
                    .unwrap()
                    .insert(resolved.clone(), content.to_vec());
                Ok((resolved, content.len(), sha256_hex(content)))
            }
            fn list_dir(
                &self,
                path: Option<&str>,
                _include_hidden: bool,
                _max_results: usize,
            ) -> Result<serde_json::Value, String> {
                let resolved = self.resolve_path(path, false)?;
                Ok(
                    serde_json::json!({ "action": "list", "path": resolved, "entries": [], "count": 0, "truncated": false }),
                )
            }
        }

        let tmp = std::env::temp_dir().join(format!("anyagents-ws-trait-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        let cfg_dir = tmp.join("cfg");
        let work_dir = tmp.join("work");
        std::fs::create_dir_all(&cfg_dir).unwrap();
        std::fs::create_dir_all(&work_dir).unwrap();

        let resolved_local = <dyn WorkspaceBackend>::resolve(Some(&cfg_dir), work_dir.clone());
        assert_eq!(resolved_local.kind(), "local");
        assert!(!resolved_local.is_remote());

        std::fs::write(
            cfg_dir.join("sandbox.json"),
            r#"{"endpoint":"e2b://api.e2b.dev","sandbox_id":"sbx-e2b","workdir":"/home/user"}"#,
        )
        .unwrap();
        let resolved_e2b = <dyn WorkspaceBackend>::resolve(Some(&cfg_dir), work_dir.clone());
        assert_eq!(resolved_e2b.kind(), "e2b");
        assert!(resolved_e2b.is_remote());
        assert_eq!(resolved_e2b.root_display(), "/home/user");

        std::fs::write(
            cfg_dir.join("sandbox.json"),
            r#"{"endpoint":"docker://local","sandbox_id":"ctr-123","workdir":"/workspace"}"#,
        )
        .unwrap();
        let resolved_docker = <dyn WorkspaceBackend>::resolve(Some(&cfg_dir), work_dir.clone());
        assert_eq!(resolved_docker.kind(), "docker");
        assert!(resolved_docker.is_remote());
        assert_eq!(resolved_docker.root_display(), "/workspace");

        let custom: Box<dyn WorkspaceBackend> = Box::new(InMemoryBackend {
            root: PathBuf::from("/vfs"),
            files: std::sync::Mutex::new(std::collections::HashMap::new()),
        });
        assert_eq!(custom.kind(), "in-memory");
        assert!(custom.is_remote());
        custom
            .write_file_bytes(Some("a.txt"), b"ok", false, true)
            .unwrap();
        assert_eq!(custom.read_file_bytes(Some("a.txt")).unwrap().1, b"ok");
        assert_eq!(
            custom.exec("uname", None, None).unwrap().stdout,
            "mem:uname"
        );

        let _ = std::fs::remove_dir_all(&tmp);
    }
}
