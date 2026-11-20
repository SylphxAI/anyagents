use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::time::{Instant, timeout};
use url::Url;

use super::{AppState, Input, Output, identity, is_loopback, send};
use crate::sandbox_exec::resolve_remote_provider_registry;
use crate::sandbox_providers::providers::local::LocalHostProvider;
use crate::sandbox_providers::providers::{
    EnsureSandboxRequest, ExecRequest, LeasePolicy, ProviderKind, SandboxHandle, SandboxProvider,
};

const PTY: &str = include_str!("login_pty.py");
const MAX_FILE: usize = 64 * 1024;
const SESSION_TIME: Duration = Duration::from_secs(245);

pub(super) struct Spec {
    pub harness: &'static str,
    pub provider: &'static str,
    pub credential: &'static str,
    pub account_file: Option<&'static str>,
    binary: &'static str,
    args: &'static str,
    auth_hosts: &'static [&'static str],
}

impl Spec {
    pub(super) fn for_harness(harness: &str) -> Option<Self> {
        Some(match harness {
            "agy" => Self {
                harness: "agy",
                provider: "google-antigravity",
                binary: "agy",
                args: "",
                credential: ".gemini/antigravity-cli/antigravity-oauth-token",
                account_file: None,
                auth_hosts: &["accounts.google.com"],
            },
            "claude-code" => Self {
                harness: "claude-code",
                provider: "anthropic",
                binary: "claude",
                args: "auth login",
                credential: ".claude/.credentials.json",
                account_file: Some(".claude.json"),
                auth_hosts: &[
                    "claude.com",
                    "claude.ai",
                    "platform.claude.com",
                    "console.anthropic.com",
                ],
            },
            "codex" => Self {
                harness: "codex",
                provider: "openai",
                binary: "codex",
                args: "login --device-auth",
                credential: ".codex/auth.json",
                account_file: None,
                auth_hosts: &["auth.openai.com"],
            },
            _ => return None,
        })
    }
}

pub(super) async fn run(
    id: &str,
    state: AppState,
    spec: Spec,
    serverless: bool,
    output: &Output,
    input: mpsc::Receiver<Input>,
) -> Result<String, String> {
    // Only a random suffix, not the input session's bearer, enters provider logs.
    let name = format!("signin-{}-{}", spec.harness, &id[4..24]);
    let directory = format!("/tmp/anyagents-{name}");
    let local = if serverless {
        None
    } else {
        Some(tempfile::tempdir().map_err(|_| "Could not allocate an isolated login directory.")?)
    };
    let (provider, mut request): (Arc<dyn SandboxProvider>, _) = if let Some(dir) = &local {
        (
            Arc::new(LocalHostProvider::with_root(dir.path())),
            EnsureSandboxRequest::for_agent(&name),
        )
    } else {
        let data_dir = state.store.data_dir().to_owned();
        let registry = tokio::task::spawn_blocking(move || resolve_remote_provider_registry(Some(&data_dir)))
            .await.ok().and_then(Result::ok).flatten()
            .ok_or("Configure a working sandbox provider (`ANYAGENTS_SANDBOX_PROVIDER`) before connecting an account in serverless mode.")?;
        (registry.active(), registry.default_ensure_request(&name))
    };
    // These adapters emulate stdin by putting its contents in the provider's
    // command string. Do not send authorization codes over that transport.
    if spec.harness != "codex"
        && matches!(provider.kind(), ProviderKind::Daytona | ProviderKind::E2b)
    {
        return Err("This sandbox provider does not support private callback input. Use a provider with native stdin transport for this sign-in.".into());
    }
    request.lease = LeasePolicy {
        idle_ttl_ms: 0,
        max_lease_ttl_ms: 300_000,
    };
    send(
        output,
        "prompt",
        format!("Preparing an isolated {} sign-in…", spec.harness),
    )
    .await;
    let handle = timeout(Duration::from_secs(35), provider.ensure(&request)).await
        .map_err(|_| "Sign-in sandbox setup timed out. Check provider availability and try again.")?
        .map_err(|_| "Could not provision the sign-in sandbox. Check the configured provider and sandbox image.")?;
    let runner = Runner {
        provider: provider.as_ref(),
        handle: &handle,
        directory: &directory,
    };
    let result = runner.login(&spec, output, input).await;
    // Cleanup is bounded and attempted even on cancellation; also stop the
    // process for providers that do not terminate it when deleting the mount.
    let _ = runner
        .command(
            format!("if [ -d {directory} ]; then touch {directory}/stop; fi"),
            1500,
        )
        .await;
    if !matches!(
        timeout(Duration::from_secs(3), provider.destroy(&handle)).await,
        Ok(Ok(()))
    ) {
        let _ = timeout(Duration::from_secs(3), provider.release(&handle)).await;
    }
    if local.is_some() {
        let _ = std::fs::remove_dir_all(directory);
    }
    let files = result?;
    if output.is_closed() {
        return Err("Sign-in cancelled.".into());
    }
    let store = Arc::clone(&state.store);
    let provider = spec.provider;
    let label = tokio::task::spawn_blocking(move || identity::finish(&store, &spec, files))
        .await
        .map_err(|_| "Could not save the authenticated profile. Try again.")??;
    state.profiles.invalidate(&state.store);
    super::super::quota_rotation::profiles_changed(Some(provider));
    Ok(label)
}

struct Runner<'a> {
    provider: &'a dyn SandboxProvider,
    handle: &'a SandboxHandle,
    // Always generated by this module. User input is only sent over stdin.
    directory: &'a str,
}

impl Runner<'_> {
    async fn exec(
        &self,
        request: ExecRequest,
        ms: u64,
    ) -> Result<crate::sandbox_providers::providers::ExecOutput, String> {
        timeout(
            Duration::from_millis(ms + 1000),
            self.provider
                .exec(self.handle, &request.with_timeout_ms(ms)),
        )
        .await
        .ok()
        .and_then(Result::ok)
        .ok_or_else(|| {
            "Could not reach the sign-in sandbox. Check the provider and retry.".to_owned()
        })
    }

    async fn command(
        &self,
        command: String,
        ms: u64,
    ) -> Result<crate::sandbox_providers::providers::ExecOutput, String> {
        self.exec(ExecRequest::shell(command), ms).await
    }

    async fn launch(&self, spec: &Spec) -> Result<(), String> {
        let dir = self.directory;
        let result = self.command(format!(
            "umask 077; command -v {binary} >/dev/null || exit 127; command -v python3 >/dev/null || exit 126; \
             mkdir {dir} && cd {dir} || exit 125\n\
             cat > login_pty.py <<'ANYAGENTS_PTY'\n{PTY}\nANYAGENTS_PTY\n\
             nohup python3 login_pty.py {harness} {credential} {binary} {args} </dev/null >runner.log 2>&1 &",
            binary = spec.binary, harness = spec.harness, credential = spec.credential, args = spec.args,
        ), 12_000).await?;
        match result.exit_code {
            0 => Ok(()),
            127 => Err(format!("{} is not installed in the sign-in runtime. Install it locally or choose a sandbox image that includes it.", spec.binary)),
            126 => Err("The sign-in runtime is missing Python 3. Use a sandbox image with Python 3 and PTY support.".into()),
            _ => Err("Could not initialize the private sign-in workspace. Check sandbox permissions.".into()),
        }
    }

    async fn login(
        &self,
        spec: &Spec,
        output: &Output,
        mut input: mpsc::Receiver<Input>,
    ) -> Result<BTreeMap<String, Vec<u8>>, String> {
        self.launch(spec).await?;
        let deadline = Instant::now() + SESSION_TIME;
        let mut transcript = Transcript::default();
        let mut log = Vec::new();
        let mut next = Instant::now();
        loop {
            tokio::select! {
                _ = output.closed() => return Err("Sign-in cancelled.".into()),
                _ = tokio::time::sleep_until(deadline) => return Err("Sign-in timed out. Open a fresh sign-in and complete it within four minutes.".into()),
                Some(request) = input.recv() => {
                    let result = match transcript.relay(&request.value) {
                        Ok(payload) => self.relay(payload).await,
                        Err(message) => Err(message),
                    };
                    let _ = request.result.send(result);
                }
                _ = tokio::time::sleep_until(next) => {
                    let start = log.len() + 1;
                    let result = self.command(format!(
                        "cd {} || exit 1; if [ -f exit ]; then cat exit; else echo pending; fi; \
                         if [ -f output ]; then tail -c +{start} output | base64; fi", self.directory,
                    ), 6500).await?;
                    let (status, chunk) = result.stdout.split_once('\n').ok_or("Sign-in sandbox returned an invalid status.")?;
                    let encoded: String = chunk.chars().filter(|c| !c.is_ascii_whitespace()).collect();
                    let chunk = STANDARD.decode(encoded).map_err(|_| "Sign-in sandbox returned invalid output.")?;
                    log.extend_from_slice(&chunk);
                    if log.len() > 262_144 { return Err("Sign-in produced too much output. Check the sandbox image.".into()); }
                    transcript.update(spec, &String::from_utf8_lossy(&log), output).await;
                    match status.trim() {
                        "pending" => (),
                        "0" => break,
                        "124" => return Err("Sign-in expired. Start again to get a new link or device code.".into()),
                        "125" | "126" => return Err("Could not run native sign-in in the runtime. Check that the sandbox image supports PTYs and Python 3.".into()),
                        "127" => return Err(format!("Could not find {} in the sign-in runtime.", spec.binary)),
                        _ => return Err(format!("{} could not complete sign-in. Check provider availability, device sign-in settings, and sandbox egress, then retry.", spec.harness)),
                    }
                    next = Instant::now() + Duration::from_millis(850);
                }
            }
        }
        let mut files = BTreeMap::new();
        for rel in std::iter::once(spec.credential).chain(spec.account_file) {
            let got = self
                .command(
                    format!(
                        "cd {} && if [ -f home/{rel} ]; then head -c {} home/{rel} | base64; fi",
                        self.directory,
                        MAX_FILE + 1
                    ),
                    5000,
                )
                .await?;
            let encoded: String = got
                .stdout
                .chars()
                .filter(|c| !c.is_ascii_whitespace())
                .collect();
            let bytes = STANDARD
                .decode(encoded)
                .map_err(|_| "Sign-in sandbox returned invalid account data.")?;
            if got.exit_code == 0 && !bytes.is_empty() && bytes.len() <= MAX_FILE {
                files.insert(rel.to_owned(), bytes);
            }
        }
        Ok(files)
    }

    async fn relay(&self, payload: Relay<'_>) -> Result<(), &'static str> {
        let mut data = serde_json::to_vec(&payload).map_err(|_| "Invalid callback.")?;
        data.push(b'\n');
        let request = ExecRequest::argv(["python3", "-c", "import socket, sys\nwith socket.socket(socket.AF_UNIX) as s:\n s.settimeout(6)\n s.connect('relay.sock')\n s.sendall(sys.stdin.buffer.read(16384))\n sys.exit(0 if s.recv(8) == b'ok\\n' else 1)"])
            .with_cwd(self.directory).with_stdin(data);
        match self.exec(request, 7200).await {
            Ok(out) if out.exit_code == 0 => Ok(()),
            _ => Err(
                "The sandbox could not deliver this callback. Check the pasted value or start a new sign-in.",
            ),
        }
    }
}

#[derive(Serialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
enum Relay<'a> {
    Terminal {
        value: std::borrow::Cow<'a, str>,
    },
    Loopback {
        host: &'a str,
        port: u16,
        target: String,
    },
}

#[derive(Default)]
struct Transcript {
    links: Vec<Url>,
    code: bool,
    manual: bool,
    callback: Option<(Url, String)>,
}

impl Transcript {
    // Only structured provider links/codes and fixed prompts leave the PTY.
    // In particular, never reflect arbitrary CLI diagnostics or pasted values.
    async fn update(&mut self, spec: &Spec, raw: &str, output: &Output) {
        let (clean, hyperlinks) = strip_escapes(raw);
        let candidates = extract_urls(&clean, hyperlinks);
        // Prefer the full OSC-8 href; the printed label may wrap in the PTY.
        let before_input = self.manual || self.callback.is_some();
        for candidate in candidates {
            let Ok(link) = Url::parse(candidate) else {
                continue;
            };
            let Some(host) = link.host_str() else {
                continue;
            };
            if link.scheme() != "https"
                || !spec.auth_hosts.contains(&host)
                || link.port().is_some()
                || !link.username().is_empty()
                || link.password().is_some()
                || link.fragment().is_some()
                || candidate.len() > 8192
                || self.links.len() >= 4
                || self.links.contains(&link)
            {
                continue;
            }
            let params: BTreeMap<_, _> = link.query_pairs().collect();
            if params.iter().any(|(key, value)| {
                matches!(
                    key.as_ref(),
                    "access_token" | "id_token" | "refresh_token" | "client_secret"
                ) || (key == "code" && !(spec.harness == "claude-code" && value == "true"))
            }) {
                continue;
            }
            if spec.harness == "codex" {
                if link.path() != "/codex/device" || link.query().is_some() {
                    continue;
                }
            } else if !["client_id", "redirect_uri", "state", "response_type"]
                .into_iter()
                .all(|key| params.get(key).is_some_and(|v| !v.is_empty()))
            {
                continue;
            }
            if let Some(uri) = params
                .get("redirect_uri")
                .and_then(|raw| Url::parse(raw).ok())
                && ((is_loopback(&uri) && uri.scheme() == "http" && uri.port().is_some())
                    || (spec.harness == "agy"
                        && uri.as_str() == crate::signin::AGY_OAUTH_REDIRECT_URI))
                && let Some(state) = params.get("state").filter(|v| !v.is_empty())
                && uri.query().is_none()
                && uri.fragment().is_none()
                && uri.username().is_empty()
                && uri.password().is_none()
            {
                self.callback = Some((uri, state.to_string()));
            }
            send(output, "link", link.as_str()).await;
            self.links.push(link);
        }
        if spec.harness == "codex"
            && !self.code
            && !self.links.is_empty()
            && let Some(code) = clean
                .split_whitespace()
                .map(|word| word.trim_matches(['[', ']', '(', ')', '.', ':']))
                .find(|word| device_code(word))
        {
            self.code = true;
            send(output, "code", code).await;
        }
        if !self.links.is_empty() && !self.manual && spec.harness != "codex" {
            let lower = clean.to_ascii_lowercase();
            self.manual = lower.contains("paste code here") || lower.contains("authorization code");
            if self.manual && self.callback.is_none() {
                send(
                    output,
                    "prompt",
                    "Finish signing in in your browser, then paste the authorization code below.",
                )
                .await;
            }
        }
        if !before_input && (self.manual || self.callback.is_some()) {
            send(output, "input", "").await;
        }
    }

    fn relay<'a>(&self, value: &'a str) -> Result<Relay<'a>, &'static str> {
        if value.starts_with("http://") || value.starts_with("https://") {
            let (expected, state) = self
                .callback
                .as_ref()
                .ok_or("This sign-in expects an authorization code, not a URL.")?;
            let uri = Url::parse(value)
                .map_err(|_| "Paste the complete callback URL from the browser.")?;
            if uri.scheme() != expected.scheme()
                || uri.host_str() != expected.host_str()
                || uri.port() != expected.port()
                || uri.path() != expected.path()
                || uri.fragment().is_some_and(|s| !s.is_empty())
                || !uri.username().is_empty()
                || uri.password().is_some()
                || !uri
                    .query_pairs()
                    .any(|(k, v)| (k == "code" || k == "error") && !v.is_empty())
                || uri
                    .query_pairs()
                    .filter(|(k, _)| k == "state")
                    .map(|(_, v)| v)
                    .ne([state.as_str()])
            {
                return Err(
                    "This callback does not match the current sign-in. Paste the matching callback URL.",
                );
            }
            if expected.scheme() == "https" {
                let mut codes = uri
                    .query_pairs()
                    .filter_map(|(k, v)| (k == "code").then_some(v));
                let code = codes
                    .next()
                    .filter(|code| valid_terminal_code(code))
                    .ok_or(
                        "The provider did not return an authorization code. Try signing in again.",
                    )?;
                if codes.next().is_some() {
                    return Err("The callback contains more than one authorization code.");
                }
                return Ok(Relay::Terminal {
                    value: std::borrow::Cow::Owned(code.into_owned()),
                });
            }
            // Use the parsed path and query (not the user's raw URL), so HTTP
            // framing, destinations and headers cannot be supplied by the form.
            let target = uri[url::Position::BeforePath..url::Position::AfterQuery].to_owned();
            return Ok(Relay::Loopback {
                host: match expected.host_str() {
                    Some("[::1]") => "::1",
                    Some("localhost") => "localhost",
                    _ => "127.0.0.1",
                },
                port: expected.port().unwrap_or_default(),
                target,
            });
        }
        if !self.manual {
            return Err(
                "Wait for the provider's code prompt or paste the complete localhost callback URL.",
            );
        }
        if !valid_terminal_code(value) {
            return Err("Paste only the authorization code shown by your provider.");
        }
        Ok(Relay::Terminal {
            value: value.into(),
        })
    }
}

fn valid_terminal_code(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.~#%+/:=".contains(&b))
}

// Materialize before the async send; do not keep nested borrowed closure
// iterators in the spawned future.
fn extract_urls<'a>(clean: &'a str, mut links: Vec<&'a str>) -> Vec<&'a str> {
    for line in clean.lines().take(4000) {
        for word in line.split_whitespace() {
            let word = word.trim_matches(['[', ']', '(', ')', '<', '>', '"', '\'', '`', ',', '.']);
            if word.starts_with("https://") {
                links.push(word);
            }
        }
    }
    links
}

fn device_code(word: &str) -> bool {
    word.split_once('-').is_some_and(|(a, b)| {
        [a, b].iter().all(|part| {
            (3..=6).contains(&part.len())
                && part
                    .bytes()
                    .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
        })
    }) && word.bytes().any(|byte| byte.is_ascii_uppercase())
}

fn strip_escapes(raw: &str) -> (String, Vec<&str>) {
    let mut clean = String::with_capacity(raw.len());
    let mut links = Vec::new();
    let mut rest = raw;
    while let Some((line, tail)) = rest.split_once('\x1b') {
        clean.push_str(line);
        if let Some(csi) = tail.strip_prefix('[') {
            let Some(end) = csi.find(|c: char| ('@'..='~').contains(&c)) else {
                return (clean, links);
            };
            rest = &csi[end + 1..];
        } else if let Some(osc) = tail.strip_prefix(']') {
            let Some(end) = osc.find(['\x07', '\x1b']) else {
                return (clean, links);
            };
            if let Some((_, href)) = osc[..end]
                .strip_prefix("8;")
                .and_then(|s| s.split_once(';'))
                && !href.is_empty()
            {
                links.push(href);
            }
            rest = osc[end..].strip_prefix("\x1b\\").unwrap_or(&osc[end + 1..]);
        } else {
            rest = tail;
        }
    }
    clean.push_str(rest);
    (clean, links)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn streams_cli_prompt_shapes_without_diagnostics_or_tokens() {
        let (output, mut lines) = mpsc::channel(16);
        let cases = [
            (
                "claude-code",
                "\x1b[31mhttps://claude.com/cai/oauth/authorize?client_id=test&redirect_uri=https%3A%2F%2Fplatform.claude.com%2Foauth%2Fcode%2Fcallback&response_type=code&state=test&code=true\x1b[0m\nPaste code here:",
                "input",
            ),
            (
                "agy",
                "\x1b]8;;https://accounts.google.com/o/oauth2/auth?client_id=test&redirect_uri=https%3A%2F%2Fantigravity.google%2Foauth-callback&response_type=code&state=test\x07Open Browser\x1b]8;;\x07\nEnter the authorization code",
                "input",
            ),
            (
                "codex",
                "Visit https://auth.openai.com/codex/device\nEnter this one-time code: CODE-TEST",
                "code",
            ),
        ];
        for (harness, raw, expected) in cases {
            let mut transcript = Transcript::default();
            let spec = Spec::for_harness(harness).unwrap();
            transcript.update(&spec, "private diagnostic https://auth.openai.com/codex/device?access_token=not-public", &output).await;
            assert!(lines.try_recv().is_err());
            transcript.update(&spec, raw, &output).await;
            let mut kinds = Vec::new();
            while let Ok(line) = lines.try_recv() {
                assert!(!line.contains("private diagnostic"));
                let event: serde_json::Value = serde_json::from_str(&line).unwrap();
                kinds.push(event["kind"].as_str().unwrap().to_owned());
            }
            assert_eq!(kinds.first().unwrap(), "link", "{harness}");
            assert_eq!(kinds.last().unwrap(), expected, "{harness}");
            transcript.update(&spec, raw, &output).await;
            assert!(lines.try_recv().is_err(), "duplicate {harness} events");
        }
    }

    #[tokio::test]
    async fn callback_requires_exact_destination_and_state_and_never_accepts_shell_syntax() {
        let (output, _lines) = mpsc::channel(16);
        let mut t = Transcript::default();
        t.update(&Spec::for_harness("agy").unwrap(), "https://accounts.google.com/o/oauth2/auth?client_id=test&redirect_uri=https%3A%2F%2Fantigravity.google%2Foauth-callback&response_type=code&state=test\nPaste authorization code", &output).await;
        assert!(
            matches!(t.relay("https://antigravity.google/oauth-callback?state=test&code=abc%2F123#"), Ok(Relay::Terminal { value }) if value == "abc/123")
        );
        for value in [
            "https://antigravity.google/oauth-callback?code=abc",
            "https://antigravity.google/oauth-callback?code=abc&state=wrong",
            "https://example.com/oauth-callback?code=abc&state=test",
            "a$(command)",
        ] {
            assert!(t.relay(value).is_err());
        }
        let mut t = Transcript::default();
        t.update(&Spec::for_harness("claude-code").unwrap(), "https://claude.com/cai/oauth/authorize?client_id=test&redirect_uri=http%3A%2F%2Flocalhost%3A9123%2Fcallback&response_type=code&state=test", &output).await;
        assert!(matches!(
            t.relay("http://localhost:9123/callback?code=abc&state=test"),
            Ok(Relay::Loopback {
                host: "localhost",
                port: 9123,
                ..
            })
        ));
        for value in [
            "http://localhost:9123/other?code=abc&state=test",
            "http://localhost:9124/callback?code=abc&state=test",
            "http://localhost:9123/callback?code=abc&state=test&state=test",
            "http://127.0.0.1:9123/callback?code=abc&state=test",
        ] {
            assert!(t.relay(value).is_err());
        }
    }
}
