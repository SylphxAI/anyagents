use std::sync::Mutex;

use super::*;
use crate::sandbox_providers::providers::{ExecOutput, ProviderHealth, SandboxCapabilities};

struct SharedSandbox {
    local: LocalHostProvider,
    delay: Duration,
    requests: Mutex<Vec<EnsureSandboxRequest>>,
    polls: Mutex<Vec<(String, String)>>,
}

impl SharedSandbox {
    fn new(root: &std::path::Path, delay: Duration) -> Self {
        Self {
            local: LocalHostProvider::with_root(root),
            delay,
            requests: Mutex::default(),
            polls: Mutex::default(),
        }
    }
}

#[async_trait::async_trait]
impl SandboxProvider for SharedSandbox {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Local
    }

    fn capabilities(&self) -> SandboxCapabilities {
        self.local.capabilities()
    }

    async fn ensure(&self, req: &EnsureSandboxRequest) -> Result<SandboxHandle, String> {
        self.requests.lock().unwrap().push(req.clone());
        tokio::time::sleep(self.delay).await;
        self.local.ensure(req).await
    }

    async fn exec(&self, handle: &SandboxHandle, req: &ExecRequest) -> Result<ExecOutput, String> {
        if let Some(command) = req.command.last()
            && command.contains("echo starting")
        {
            let directory = command.split_whitespace().nth(1).unwrap();
            self.polls
                .lock()
                .unwrap()
                .push((handle.sandbox_id.clone(), directory.to_owned()));
        }
        self.local.exec(handle, req).await
    }

    async fn sync_bundle_tar(&self, _: &SandboxHandle, _: &str, _: &[u8]) -> Result<(), String> {
        unreachable!("native sign-in must not stage bundles")
    }

    async fn release(&self, _: &SandboxHandle) -> Result<(), String> {
        panic!("native sign-in must not release the shared lease")
    }

    async fn destroy(&self, _: &SandboxHandle) -> Result<(), String> {
        panic!("native sign-in must not destroy the shared volume")
    }

    async fn health(&self) -> Result<ProviderHealth, String> {
        self.local.health().await
    }
}

#[tokio::test(start_paused = true)]
async fn cold_ensure_streams_keepalives_and_has_a_90_second_deadline() {
    let root = tempfile::tempdir().unwrap();
    let (out, mut lines) = mpsc::channel(16);
    let run = |delay| {
        let out = out.clone();
        let provider: Arc<dyn SandboxProvider> = Arc::new(SharedSandbox::new(root.path(), delay));
        tokio::spawn(async move {
            let started = Instant::now();
            let result = progress(
                &out,
                "Waiting for the sign-in sandbox…",
                ensure(&provider, EnsureSandboxRequest::for_agent(SLOT)),
            )
            .await
            .unwrap();
            (started.elapsed(), result)
        })
    };
    let slow = run(Duration::from_secs(61));
    let line: serde_json::Value = serde_json::from_str(&lines.recv().await.unwrap()).unwrap();
    assert_eq!(line["kind"], "progress");
    assert!(!slow.is_finished());
    let (_, handle) = slow.await.unwrap();
    assert_eq!(handle.unwrap().sandbox_id, "local-anyagents-native-signin");
    let (elapsed, error) = run(Duration::from_secs(95)).await.unwrap();
    assert_eq!(elapsed, Duration::from_secs(90));
    assert!(error.unwrap_err().contains("90-second deadline"));
}

#[test]
fn masks_provider_diagnostics_before_logging() {
    let line = diagnostic(
        "sandbox returned HTTP 503: endpoint https://example.invalid/?token=hidden credential api_key=masked\x1b",
    );
    assert!(line.contains("HTTP 503"));
    assert!(!line.contains("example.invalid"));
    assert!(!line.contains("hidden"));
    assert!(!line.contains("masked"));
    assert!(!line.contains('\x1b'));
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn concurrent_signins_share_a_slot_but_only_clean_up_their_own_pty_and_home() {
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;
    use tokio::sync::oneshot;

    let root = tempfile::tempdir().unwrap();
    let binary = root.path().join("login-fixture");
    std::fs::write(
        &binary,
        r#"#!/usr/bin/python3
import os, sys
from pathlib import Path
assert sys.argv[1:] == ['auth', 'login']
home = Path(os.environ['HOME'])
assert Path.cwd() == home and home.stat().st_mode & 0o777 == 0o700
assert Path(os.environ['XDG_CONFIG_HOME']).parent == home
assert Path(os.environ['TMPDIR']).parent == home
config = Path(os.environ['CLAUDE_CONFIG_DIR'])
assert config.parent == home and not config.exists()
(home / 'pid').write_text(str(os.getpid()))
print('https://claude.com/oauth/authorize?client_id=test&redirect_uri=https%3A%2F%2Fexample.invalid%2Fcallback&response_type=code&state=test', flush=True)
print('Paste code here:', flush=True)
value = input()
config.mkdir()
(config / '.credentials.json').write_text(value)
(home / '.claude.json').write_text('account-' + value)
"#,
    )
    .unwrap();
    std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700)).unwrap();
    let provider = Arc::new(SharedSandbox::new(root.path(), Duration::ZERO));
    let start = || {
        let (output, lines) = mpsc::channel(16);
        let (input, requests) = mpsc::channel(1);
        let provider: Arc<dyn SandboxProvider> = provider.clone();
        let binary = binary.clone();
        let task = tokio::spawn(async move {
            collect(
                &provider,
                EnsureSandboxRequest::for_agent(SLOT),
                &Spec::for_harness("claude-code").unwrap(),
                SANDBOX_PYTHON,
                binary.to_str(),
                &output,
                requests,
            )
            .await
        });
        (lines, input, task)
    };
    async fn ready(lines: &mut mpsc::Receiver<String>) {
        timeout(Duration::from_secs(10), async {
            while let Some(line) = lines.recv().await {
                let event: serde_json::Value = serde_json::from_str(&line).unwrap();
                if event["kind"] == "input" {
                    return;
                }
            }
            panic!("PTY stream ended without input prompt");
        })
        .await
        .expect("PTY did not become ready");
    }
    let (mut first, _first_input, first_task) = start();
    ready(&mut first).await;
    let (mut second, second_input, second_task) = start();
    ready(&mut second).await;
    {
        let requests = provider.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0], requests[1]);
        assert_eq!(requests[0].agent_id, "anyagents-native-signin");
        assert_eq!(
            requests[0].volume.as_ref().unwrap().id_or_name,
            "anyagents-native-signin"
        );
        assert_eq!(requests[0].lease.ttl_duration_string(), "600s");
    }
    let homes: Vec<_> = {
        let polls = provider.polls.lock().unwrap();
        assert!(
            polls
                .iter()
                .all(|(id, _)| id == "local-anyagents-native-signin")
        );
        let mut dirs: Vec<_> = Vec::new();
        for (_, dir) in polls.iter() {
            if !dirs.contains(dir) {
                dirs.push(dir.clone());
            }
        }
        assert_eq!(dirs.len(), 2);
        dirs.into_iter()
            .map(|dir| {
                assert!(dir.starts_with("/tmp/anyagents-signin-"));
                let path = Path::new(&dir);
                assert_eq!(path.metadata().unwrap().permissions().mode() & 0o777, 0o700);
                let pid = std::fs::read_to_string(path.join("home/pid")).unwrap();
                (path.to_owned(), format!("/proc/{pid}"))
            })
            .collect()
    };
    drop(first);
    assert_eq!(first_task.await.unwrap().unwrap_err(), CANCELLED);
    assert!(!homes[0].0.exists());
    assert!(!Path::new(&homes[0].1).exists());
    assert!(homes[1].0.exists());
    assert!(Path::new(&homes[1].1).exists());

    let (result, reply) = oneshot::channel();
    second_input
        .send(Input {
            value: "fixture-code".into(),
            result,
        })
        .await
        .unwrap();
    assert_eq!(reply.await.unwrap(), Ok(()));
    let files = second_task.await.unwrap().unwrap();
    assert_eq!(files[".claude/.credentials.json"], b"fixture-code");
    assert_eq!(files[".claude.json"], b"account-fixture-code");
    assert!(!homes[1].0.exists());
    assert!(!Path::new(&homes[1].1).exists());
    assert!(root.path().join(SLOT).is_dir());
}
