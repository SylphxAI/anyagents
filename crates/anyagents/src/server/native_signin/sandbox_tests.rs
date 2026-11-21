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
    let handle = handle.unwrap();
    assert_eq!(handle.sandbox_id, "local-anyagents-native-signin");
    assert!(handle.volume_id.is_none());
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
        assert!(requests[0].volume.is_none());
        assert!(requests[0].existing.is_none());
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
}

// Captured PTY prompt shapes with inert OAuth values in place of session data.
const CLAUDE_REQUEST: &str =
    "https://claude.ai/oauth/authorize?ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi012345-_";
const AGY_REQUEST: &str = "https://accounts.google.com/o/oauth2/auth?access_type=offline&client_id=fixture&redirect_uri=https%3A%2F%2Fantigravity.google%2Foauth-callback&response_type=code&state=fixture";
const AGY_MENU: &str = " Select login method:\r\n > 1. Google OAuth\n\x1b[4G2. Use a Google Cloud project\n\n\x1b[4G\x1b[1m↑/↓\x1b[m Navigate · \x1b[1menter\x1b[m Select";

fn agy_browser_step() -> String {
    // Like the real TUI, split the visible URL across rows and leave the full
    // href in each OSC-8. The href (not a fragment of the label) must be emitted.
    format!(
        "\x1b[8;2HYour browser should open automatically. If not:\x1b[K\n\n\x1b[2G\x1b[4m\x1b]8;id=fixture;{AGY_REQUEST}\x07https://accounts.google.com/o/oauth2/auth?access_type=offline\x1b[m\x1b]8;;\x07\r\n \x1b[4m\x1b]8;id=fixture;{AGY_REQUEST}\x07&client_id=fixture\x1b[m\x1b]8;;\x07\r\n"
    )
}

fn drain_steps(lines: &mut mpsc::Receiver<String>) -> Vec<(String, String)> {
    std::iter::from_fn(|| lines.try_recv().ok())
        .map(|line| {
            let step: serde_json::Value = serde_json::from_str(&line).unwrap();
            (
                step["kind"].as_str().unwrap().to_owned(),
                step["text"].as_str().unwrap().to_owned(),
            )
        })
        .collect()
}

#[tokio::test]
async fn current_native_pty_transcripts_emit_actionable_steps() {
    let (output, mut lines) = mpsc::channel(16);
    let claude = Spec::for_harness("claude-code").unwrap();
    for suffix in ["", "67"] {
        let link = format!("{CLAUDE_REQUEST}{suffix}");
        let mut transcript = Transcript::default();
        let prefix = format!(
            "Opening browser to sign in…\r\nIf the browser didn't open, visit:\r\n\x1b]8;;{link}"
        );
        transcript.update(&claude, &prefix, &output).await;
        assert!(drain_steps(&mut lines).is_empty(), "partial OSC-8");
        let raw = format!("{prefix}\x07{link}\x1b]8;;\x07\r\nPaste code here if prompted > ");
        transcript.update(&claude, &raw, &output).await;
        assert_eq!(
            drain_steps(&mut lines),
            [
                ("link".into(), link.clone()),
                (
                    "prompt".into(),
                    "Finish signing in in your browser, then paste the authorization code below."
                        .into()
                ),
                ("input".into(), String::new()),
            ]
        );
        assert!(matches!(
            transcript.relay("fixture-code"),
            Ok(Relay::Terminal { .. })
        ));
        transcript.update(&claude, &raw, &output).await;
        assert!(drain_steps(&mut lines).is_empty());
    }

    let agy = Spec::for_harness("agy").unwrap();
    let mut transcript = Transcript::default();
    transcript.update(&agy, AGY_MENU, &output).await;
    assert!(drain_steps(&mut lines).is_empty());
    transcript
        .update(&agy, &format!("{AGY_MENU}{}", agy_browser_step()), &output)
        .await;
    assert_eq!(
        drain_steps(&mut lines),
        [
            ("link".into(), AGY_REQUEST.into()),
            ("input".into(), String::new())
        ]
    );

    let codex = Spec::for_harness("codex").unwrap();
    for code in ["U81Q-J76WZ", "U81Q-J76WZ9"] {
        let mut transcript = Transcript::default();
        transcript.update(&codex, &format!("  1. Open this link in your browser and sign in to your account\r\n     https://auth.openai.com/codex/device\r\n\r\n  2. Enter this one-time code (expires in 15 minutes)\r\n     {code}\r\n"), &output).await;
        assert_eq!(
            drain_steps(&mut lines),
            [
                ("link".into(), "https://auth.openai.com/codex/device".into()),
                ("code".into(), code.into())
            ]
        );
    }
}

#[tokio::test]
async fn short_oauth_query_is_only_accepted_at_claude_authorize_routes() {
    let (output, mut lines) = mpsc::channel(16);
    let claude = Spec::for_harness("claude-code").unwrap();
    let token = CLAUDE_REQUEST.split_once('?').unwrap().1;
    for url in [
        format!("https://claude.ai/not-authorize?{token}"),
        format!("https://claude.ai.example.test/oauth/authorize?{token}"),
        format!("https://platform.claude.com/oauth/authorize?{token}"),
        format!("https://claude.ai/oauth/authorize?{token}#fragment"),
        format!("https://claude.ai/oauth/authorize?{token}&state=fixture"),
        format!("https://claude.ai/oauth/authorize?code={}", &token[5..]),
        format!("https://claude.ai/oauth/authorize?{}%2F", &token[3..]),
        format!("https://claude.ai/oauth/authorize?{}", &token[..23]),
        format!(
            "https://claude.ai/oauth/authorize?{token}{}",
            "x".repeat(214)
        ),
        format!("https://claude.ai/oauth/authorize?{token}="),
        "https://claude.ai/oauth/authorize?".to_owned(),
        format!("https://claude.ai/oauth/authorize?{token}%20suffix"),
        "https://claude.ai/oauth/authorize?state".to_owned(),
    ] {
        let mut transcript = Transcript::default();
        transcript
            .update(
                &claude,
                &format!("{url}\r\nPaste code here if prompted > "),
                &output,
            )
            .await;
        assert!(drain_steps(&mut lines).is_empty(), "accepted {url}");
        assert!(transcript.relay("fixture-code").is_err());
    }
    let mut transcript = Transcript::default();
    let url = format!("https://claude.com/cai/oauth/authorize?{token}");
    transcript
        .update(
            &claude,
            &format!("\x1b]8;;{url}\x1b\\Open browser\x1b]8;;\x1b\\"),
            &output,
        )
        .await;
    let steps = drain_steps(&mut lines);
    assert_eq!(steps[0], ("link".into(), url));
    assert_eq!(steps.last().unwrap().0, "input");
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn agy_pty_supervisor_selects_google_once_with_ansi_and_wrapping() {
    use std::os::unix::fs::PermissionsExt;

    let root = tempfile::tempdir().unwrap();
    let binary = root.path().join("agy-fixture");
    // Use a raw PTY so the fixture can check the exact key and detect repeats.
    let script = format!(
        r#"#!/usr/bin/python3
import os, select, time, tty
from pathlib import Path
tty.setraw(0)
os.write(1, {first})
time.sleep(0.05)
os.write(1, {second})
assert select.select([0], [], [], 2)[0], 'no Enter received'
assert os.read(0, 32) == b'\r'
os.write(1, {browser})
assert not select.select([0], [], [], 0.2)[0], 'Enter was sent twice'
artifact = Path('.gemini/antigravity-cli/antigravity-oauth-token')
artifact.parent.mkdir(parents=True)
artifact.write_text('{{"access_token":"fixture"}}')
"#,
        first = python_bytes(b" Select\x1b[1m login\x1b[0m\r\n\x1b[4Gmethod:\r\n > 1. Go"),
        second = python_bytes(
            b"\x1b[32mogle\x1b[0m\r\n\x1b[4GOAuth\r\n\x1b[4G2. Use a Google Cloud project\n"
        ),
        browser = python_bytes(agy_browser_step().as_bytes()),
    );
    std::fs::write(&binary, script).unwrap();
    std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700)).unwrap();
    let provider: Arc<dyn SandboxProvider> = Arc::new(LocalHostProvider::with_root(root.path()));
    let (output, mut lines) = mpsc::channel(16);
    let (_input, requests) = mpsc::channel(1);
    timeout(
        Duration::from_secs(10),
        collect(
            &provider,
            EnsureSandboxRequest::for_agent(SLOT),
            &Spec::for_harness("agy").unwrap(),
            SANDBOX_PYTHON,
            binary.to_str(),
            &output,
            requests,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(drain_steps(&mut lines).contains(&("link".into(), AGY_REQUEST.into())));
}

#[cfg(target_os = "linux")]
fn python_bytes(bytes: &[u8]) -> String {
    format!(
        "bytes.fromhex('{}')",
        bytes.iter().map(|b| format!("{b:02x}")).collect::<String>()
    )
}
