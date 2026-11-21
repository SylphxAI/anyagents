use std::collections::BTreeMap;
use std::future::Future;
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
    shell_single_quote,
};
use anyagents_core::stateless::generate_callback_token;
use anyagents_harness::masking::mask_known_secrets;

const PTY: &str = include_str!("login_pty.py");
// Installed by Dockerfile.sandbox. A coordinator path is not a guest path.
const SANDBOX_PYTHON: &str = "/usr/bin/python3";
const MAX_FILE: usize = 64 * 1024;
const SLOT: &str = "anyagents-native-signin";
const ENSURE_TIMEOUT: Duration = Duration::from_secs(90);
const KEEPALIVE: Duration = Duration::from_secs(6);
const CANCELLED: &str = "Sign-in cancelled.";
const SESSION_TIME: Duration = Duration::from_secs(555);
const CODE_REJECTED: &str = "The provider rejected the authorization code. It may be incomplete, expired, or already used. Start again to get a fresh link.";

pub(super) struct Spec {
    pub harness: &'static str,
    pub provider: &'static str,
    pub credential: &'static str,
    pub account_files: &'static [&'static str],
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
                account_files: &[],
                auth_hosts: &["accounts.google.com"],
            },
            "claude-code" => Self {
                harness: "claude-code",
                provider: "anthropic",
                binary: "claude",
                args: "auth login",
                credential: ".claude/.credentials.json",
                account_files: &[".claude/.claude.json", ".claude.json"],
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
                account_files: &[],
                auth_hosts: &["auth.openai.com"],
            },
            _ => return None,
        })
    }
}

pub(super) async fn run(
    state: AppState,
    spec: Spec,
    serverless: bool,
    output: &Output,
    input: mpsc::Receiver<Input>,
) -> Result<String, String> {
    send(
        output,
        "prompt",
        format!("Preparing an isolated {} sign-in…", spec.harness),
    )
    .await;
    let local = if serverless {
        None
    } else {
        Some(
            tempfile::tempdir()
                .map_err(|err| report("allocate local workspace", &err.to_string()))?,
        )
    };
    let (provider, request): (Arc<dyn SandboxProvider>, _) = if let Some(dir) = &local {
        (
            Arc::new(LocalHostProvider::with_root(dir.path())),
            EnsureSandboxRequest::for_agent(SLOT),
        )
    } else {
        let data_dir = state.store.data_dir().to_owned();
        let resolve =
            tokio::task::spawn_blocking(move || resolve_remote_provider_registry(Some(&data_dir)));
        let registry = progress(output, Some("Reading sandbox configuration…"), resolve).await?
            .map_err(|err| report("read sandbox configuration", &err.to_string()))?
            .map_err(|err| report("read sandbox configuration", &err))?
            .ok_or("Configure a working sandbox provider (`ANYAGENTS_SANDBOX_PROVIDER`) before connecting an account in serverless mode.")?;
        (registry.active(), registry.default_ensure_request(SLOT))
    };
    // These adapters emulate stdin by putting its contents in the provider's
    // command string. Do not send authorization codes over that transport.
    if spec.harness != "codex"
        && matches!(provider.kind(), ProviderKind::Daytona | ProviderKind::E2b)
    {
        return Err("This sandbox provider does not support private callback input. Use a provider with native stdin transport for this sign-in.".into());
    }
    // Other adapters currently synthesize a volume even if none was requested.
    // Refuse rather than falling back to a persistent sign-in workspace.
    if !matches!(provider.kind(), ProviderKind::Local | ProviderKind::Sylphx) {
        return Err("This sandbox provider does not support volume-free native sign-in.".into());
    }
    let (python, local_binary) = if provider.kind() == ProviderKind::Local {
        (local_tool("python3")?, Some(local_tool(spec.binary)?))
    } else {
        (SANDBOX_PYTHON.to_owned(), None)
    };
    let files = collect(
        &provider,
        request,
        &spec,
        &python,
        local_binary.as_deref(),
        output,
        input,
    )
    .await?;
    if output.is_closed() {
        return Err(CANCELLED.into());
    }
    let store = Arc::clone(&state.store);
    let provider = spec.provider;
    let label = tokio::task::spawn_blocking(move || identity::finish(&store, &spec, files))
        .await
        .map_err(|err| report("save authenticated profile", &err.to_string()))??;
    state.profiles.invalidate(&state.store);
    super::super::quota_rotation::profiles_changed(Some(provider));
    Ok(label)
}

fn session_request(mut request: EnsureSandboxRequest) -> EnsureSandboxRequest {
    request.volume = None;
    request.existing = None;
    request.workdir = None;
    // The lease expires even if the coordinator dies during sign-in.
    request.lease = LeasePolicy {
        idle_ttl_ms: 660_000,
        max_lease_ttl_ms: 660_000,
    };
    request
}

async fn collect(
    provider: &Arc<dyn SandboxProvider>,
    request: EnsureSandboxRequest,
    spec: &Spec,
    python: &str,
    local_binary: Option<&str>,
    output: &Output,
    input: mpsc::Receiver<Input>,
) -> Result<BTreeMap<String, Vec<u8>>, String> {
    let handle = keepalive(
        output,
        Some("Waiting for the sign-in sandbox…"),
        ensure(provider, request),
    )
    .await?;
    if output.is_closed() {
        release_scratch(provider, &handle).await;
        return Err(CANCELLED.into());
    }
    let runner = Runner {
        provider: provider.as_ref(),
        handle: &handle,
        // Independent of the stream's bearer; only shell-safe hex enters argv.
        directory: &format!(
            "/tmp/anyagents-signin-{}",
            &generate_callback_token()[4..36]
        ),
        python,
        local_binary,
    };
    // A dropped exec future does not stop a blocking adapter. Let launch settle
    // (bounded by exec's timeout) before asking the guest to stop this session.
    let launch = runner.launch(spec);
    let result = keepalive(output, Some("Starting the native sign-in…"), launch).await;
    let result = match result {
        Ok(()) if !output.is_closed() => progress(output, None, runner.login(spec, output, input))
            .await
            .and_then(|result| result),
        Ok(()) => Err(CANCELLED.into()),
        Err(err) => Err(err),
    };
    let cleanup = runner.cleanup().await;
    release_scratch(provider, &handle).await;
    let files = result?;
    cleanup?;
    Ok(files)
}

async fn release_scratch(provider: &Arc<dyn SandboxProvider>, handle: &SandboxHandle) {
    // Local work is already scoped by a TempDir. On the remote provider, the
    // safety TTL backs up release without delaying diagnostics indefinitely.
    if provider.kind() == ProviderKind::Local {
        return;
    }
    match timeout(Duration::from_secs(5), provider.release(handle)).await {
        Ok(Ok(())) => {}
        Ok(Err(err)) => eprintln!(
            "native sign-in: release scratch lease: {}",
            diagnostic(&err)
        ),
        Err(_) => {
            eprintln!("native sign-in: scratch lease release timed out; provider TTL applies")
        }
    }
}

pub(super) async fn ensure(
    provider: &Arc<dyn SandboxProvider>,
    request: EnsureSandboxRequest,
) -> Result<SandboxHandle, String> {
    let request = session_request(request);
    timeout(ENSURE_TIMEOUT, provider.ensure(&request))
        .await
        .map_err(|err| report("prepare sandbox (90-second deadline)", &err.to_string()))?
        .map_err(|err| {
            let detail = diagnostic(&err);
            eprintln!("native sign-in: prepare sandbox: {detail}");
            let Some((_, status)) = detail.split_once(" returned HTTP ") else {
                return "Could not prepare sandbox. Check the sign-in runtime and coordinator logs, then retry.".into();
            };
            format!("Could not prepare sandbox: HTTP {status}")
        })
}

async fn progress<T>(
    output: &Output,
    text: Option<&'static str>,
    work: impl Future<Output = T>,
) -> Result<T, String> {
    tokio::select! {
        biased;
        _ = output.closed() => Err(CANCELLED.into()),
        result = keepalive(output, text, work) => Ok(result),
    }
}

async fn keepalive<T>(
    output: &Output,
    text: Option<&'static str>,
    work: impl Future<Output = T>,
) -> T {
    let mut tick = tokio::time::interval_at(Instant::now() + KEEPALIVE, KEEPALIVE);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tokio::pin!(work);
    loop {
        tokio::select! {
            result = &mut work => return result,
            _ = tick.tick() => {
                // Never block the actual operation on an unread stream.
                let (kind, text) = text.map_or(("keepalive", ""), |message| ("progress", message));
                if let Ok(line) = serde_json::to_string(&super::SignInLine { kind, text: text.into() }) {
                    let _ = output.try_send(line);
                }
            }
        }
    }
}

fn local_tool(name: &str) -> Result<String, String> {
    let tool = anyagents_toolchain::tool(name, "native-signin")
        .map_err(|err| report("resolve sign-in executable", &err.to_string()))?;
    tool.path()
        .to_str()
        .filter(|_| tool.path().is_absolute())
        .map(str::to_owned)
        .ok_or_else(|| "The sign-in executable must have an absolute UTF-8 path.".into())
}

// Strip endpoint URLs in addition to the shared credential masker. Only the
// provisioning HTTP status and its sanitized message are shown in the stream.
fn diagnostic(error: &str) -> String {
    let sanitized = mask_known_secrets(error);
    let detail = sanitized
        .split_whitespace()
        .map(|word| {
            if word.contains("://") {
                "[endpoint]"
            } else {
                word
            }
        })
        .collect::<Vec<_>>()
        .join(" ");
    detail
        .chars()
        .filter(|c| !c.is_control())
        .take(1024)
        .collect()
}

fn report(operation: &str, error: &str) -> String {
    eprintln!("native sign-in: {operation}: {}", diagnostic(error));
    format!("Could not {operation}. Check the sign-in runtime and coordinator logs, then retry.")
}

// Exec adapters may reflect the command or response, including callback stdin,
// credential file bytes, and the PTY transcript. Keep only the status from such
// failures, never the provider's unstructured payload.
fn private_exec_error(operation: &str, error: &str) -> String {
    let status = error
        .split_once("HTTP ")
        .and_then(|(_, tail)| tail.get(..3))
        .filter(|s| s.bytes().all(|b| b.is_ascii_digit()));
    report(
        operation,
        &status.map_or_else(
            || "exec transport failed (private payload omitted)".into(),
            |s| format!("HTTP {s} (private payload omitted)"),
        ),
    )
}

struct Runner<'a> {
    provider: &'a dyn SandboxProvider,
    handle: &'a SandboxHandle,
    // Always generated by this module. User input is only sent over stdin.
    directory: &'a str,
    python: &'a str,
    local_binary: Option<&'a str>,
}

impl Runner<'_> {
    async fn exec(
        &self,
        operation: &str,
        mut request: ExecRequest,
        ms: u64,
        private: bool,
    ) -> Result<crate::sandbox_providers::providers::ExecOutput, String> {
        request.cwd.get_or_insert_with(|| "/tmp".into());
        let result = timeout(
            Duration::from_millis(ms + 1000),
            self.provider
                .exec(self.handle, &request.with_timeout_ms(ms)),
        )
        .await
        .map_err(|err| report(operation, &err.to_string()))?
        .map_err(|err| {
            if private {
                private_exec_error(operation, &err)
            } else {
                report(operation, &err)
            }
        })?;
        if result.exit_code != 0 {
            let detail = if private {
                String::new()
            } else {
                result.stderr.clone()
            };
            return Err(report(
                operation,
                &format!("exec exited {}: {detail}", result.exit_code),
            ));
        }
        Ok(result)
    }

    async fn cleanup(&self) -> Result<(), String> {
        // The supervisor owns the PTY and removes the home. Never kill by a PID
        // read from disk or remove the directory while the CLI can still write.
        self.exec(
            "clean up sign-in workspace",
            ExecRequest::shell(format!(
                "cd {dir} 2>/dev/null || exit 0; /usr/bin/touch stop || exit 1; \
             for i in 1 2 3 4 5 6 7 8 9 10 11 12; do \
                 [ -d {dir} ] || exit 0; sleep 0.5; \
             done; [ ! -d {dir} ]",
                dir = self.directory,
            )),
            7000,
            false,
        )
        .await?;
        Ok(())
    }

    async fn launch(&self, spec: &Spec) -> Result<(), String> {
        let dir = self.directory;
        // Use the same fixed guest install directories as Dockerfile.sandbox.
        // Do not search the shell's PATH or resolve the harness again in the PTY.
        let binary = self.local_binary.map_or_else(
            || {
                format!(
                    "binary=/usr/local/bin/{name}; [ -x \"$binary\" ] || binary=/usr/bin/{name}",
                    name = spec.binary
                )
            },
            |path| format!("binary={}", shell_single_quote(path)),
        );
        let python = shell_single_quote(self.python);
        let script = shell_single_quote(PTY);
        self.exec("start native sign-in", ExecRequest::shell(format!(
            "{binary}; [ -f \"$binary\" ] && [ -x \"$binary\" ] || {{ echo 'native login executable unavailable' >&2; exit 127; }}; \
             [ -f {python} ] && [ -x {python} ] || {{ echo 'Python 3 unavailable' >&2; exit 126; }}; \
             /usr/bin/nohup {python} -I -c {script} {dir} {harness} {credential} \"$binary\" {args} </dev/null >/dev/null 2>&1 &",
            harness = spec.harness, credential = spec.credential, args = spec.args,
        )), 12_000, false).await?;
        Ok(())
    }

    async fn login(
        &self,
        spec: &Spec,
        output: &Output,
        mut input: mpsc::Receiver<Input>,
    ) -> Result<BTreeMap<String, Vec<u8>>, String> {
        let started = Instant::now();
        let deadline = started + SESSION_TIME;
        let mut transcript = Transcript::default();
        let mut submitted = false;
        let mut log = Vec::new();
        let mut next = Instant::now();
        loop {
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => return Err("Sign-in timed out. Open a fresh sign-in and complete it within nine minutes.".into()),
                Some(request) = input.recv() => {
                    let result = match transcript.relay(&request.value) {
                        Ok(payload) => self.relay(payload).await,
                        Err(message) => Err(message),
                    };
                    submitted |= result.is_ok();
                    let _ = request.result.send(result);
                }
                _ = tokio::time::sleep_until(next) => {
                    let start = log.len() + 1;
                    let result = self.exec("poll native sign-in", ExecRequest::shell(format!(
                        "cd {dir} 2>/dev/null || {{ echo starting; exit 0; }}; \
                         /usr/bin/touch alive || exit 1; \
                         if [ -f exit ]; then cat exit; else echo pending; fi; \
                         if [ -f output ]; then tail -c +{start} output | base64; fi", dir = self.directory,
                    )), 6500, true).await?;
                    if result.stdout.trim() == "starting" {
                        if started.elapsed() >= Duration::from_secs(10) {
                            return Err(report("start private PTY supervisor", "the workspace did not appear after launch"));
                        }
                        next = Instant::now() + Duration::from_millis(200);
                        continue;
                    }
                    let (status, chunk) = result.stdout.split_once('\n').ok_or_else(|| report("poll native sign-in", "exec output omitted the status line"))?;
                    let encoded: String = chunk.chars().filter(|c| !c.is_ascii_whitespace()).collect();
                    let chunk = STANDARD.decode(encoded).map_err(|err| report("decode sign-in output", &err.to_string()))?;
                    log.extend_from_slice(&chunk);
                    if log.len() > 262_144 { return Err("Sign-in produced too much output. Check the sandbox image.".into()); }
                    transcript.update(spec, &String::from_utf8_lossy(&log), output).await?;
                    match status.trim() {
                        "pending" => (),
                        "0" => break,
                        "124" => return Err("Sign-in expired. Start again to get a new link or device code.".into()),
                        "125" | "126" => return Err(report("run native sign-in", "PTY supervisor or CLI setup failed; check Python and PTY support")),
                        _ if spec.harness == "claude-code" && submitted => return Err(CODE_REJECTED.into()),
                        _ => return Err(report("complete native sign-in", &format!("CLI exit status: {:?}", status.trim().parse::<i32>().ok()))),
                    }
                    next = Instant::now() + Duration::from_millis(850);
                }
            }
        }
        let mut files = BTreeMap::new();
        for rel in std::iter::once(spec.credential).chain(spec.account_files.iter().copied()) {
            let got = self
                .exec(
                    "read native credentials",
                    ExecRequest::shell(format!(
                        "cd {} && /usr/bin/touch alive && if [ -f home/{rel} ]; then head -c {} home/{rel} | base64; fi",
                        self.directory, MAX_FILE + 1
                    )),
                    5000,
                    true,
                )
                .await?;
            let encoded: String = got
                .stdout
                .chars()
                .filter(|c| !c.is_ascii_whitespace())
                .collect();
            let bytes = STANDARD
                .decode(encoded)
                .map_err(|err| report("decode native credentials", &err.to_string()))?;
            if !bytes.is_empty() && bytes.len() <= MAX_FILE {
                files.insert(rel.to_owned(), bytes);
            }
        }
        Ok(files)
    }

    async fn relay(&self, payload: Relay<'_>) -> Result<(), &'static str> {
        let mut data = serde_json::to_vec(&payload).map_err(|err| {
            report("encode sign-in callback", &err.to_string());
            "Invalid callback."
        })?;
        data.push(b'\n');
        let request = ExecRequest::argv([self.python, "-c", "import socket, sys\nwith socket.socket(socket.AF_UNIX) as s:\n s.settimeout(6)\n s.connect('relay.sock')\n s.sendall(sys.stdin.buffer.read(16384))\n sys.exit(0 if s.recv(8) == b'ok\\n' else 1)"])
            .with_cwd(self.directory).with_stdin(data);
        match self
            .exec("deliver native sign-in callback", request, 7200, true)
            .await
        {
            Ok(_) => Ok(()),
            Err(_) => Err(
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
    claude_feedback_offset: usize,
}

impl Transcript {
    // Only structured provider links/codes and fixed prompts leave the PTY.
    // In particular, never reflect arbitrary CLI diagnostics or pasted values.
    async fn update(&mut self, spec: &Spec, raw: &str, output: &Output) -> Result<(), String> {
        let (clean, hyperlinks) = strip_escapes(raw);
        let candidates = if spec.harness == "claude-code" {
            if self.links.is_empty() {
                // OSC-8 targets are only returned once terminated. For plain
                // text, wait for the newline so a partial handle can't win.
                let complete = clean.rsplit_once('\n').map_or("", |(lines, _)| lines);
                extract_urls(complete, hyperlinks)
            } else {
                // Later output can include echoed input and diagnostics.
                Vec::new()
            }
        } else {
            extract_urls(&clean, hyperlinks)
        };
        // Prefer the full OSC-8 href; the printed label may wrap in the PTY.
        let before_input = self.manual || self.callback.is_some();
        let mut claude_short_link = false;
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
            // Current Claude Code prints a base64url request handle instead of
            // OAuth parameters. Allow length variation, but only on Claude's authorize routes.
            let short_link = spec.harness == "claude-code"
                && matches!(host, "claude.ai" | "claude.com")
                && matches!(link.path(), "/oauth/authorize" | "/cai/oauth/authorize")
                && candidate.split_once('?').is_some_and(|(_, query)| {
                    (24..=256).contains(&query.len())
                        && query
                            .bytes()
                            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
                });
            if spec.harness == "codex" {
                if link.path() != "/codex/device" || link.query().is_some() {
                    continue;
                }
            } else if !short_link
                && !["client_id", "redirect_uri", "state", "response_type"]
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
            claude_short_link |= short_link;
            if spec.harness == "claude-code" {
                break;
            }
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
            self.manual = claude_short_link
                || lower.contains("paste code here")
                || lower.contains("authorization code");
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
            let mode = if spec.harness == "claude-code" && self.callback.is_none() {
                "authorization_code"
            } else {
                ""
            };
            send(output, "input", mode).await;
        }
        if spec.harness == "claude-code" {
            self.claude_feedback(&clean, output).await?;
        }
        Ok(())
    }

    async fn claude_feedback(&mut self, clean: &str, output: &Output) -> Result<(), String> {
        // The caller supplies the cumulative transcript. Advance only through
        // complete lines so chunk boundaries don't lose a match or replay an old
        // rejection; a repeated mistake on a new line must be reported again.
        let Some(tail) = clean.get(self.claude_feedback_offset..) else {
            return Ok(());
        };
        let Some(end) = tail.rfind('\n') else {
            return Ok(());
        };
        self.claude_feedback_offset += end + 1;
        for line in tail[..=end].lines().map(str::trim) {
            let message = match line {
                "Signing in…" | "Signing in..." => {
                    send(output, "progress", "Exchanging authorization code…").await;
                    continue;
                }
                _ if line.starts_with("Couldn't log in:") || line == "Couldn't log in" => {
                    return Err(CODE_REJECTED.into());
                }
                _ if !self.manual => continue,
                _ if line.starts_with("That doesn't look like the full code.") => {
                    "Paste the full authorization code, including the # in the middle."
                }
                _ if line.starts_with("That's a web address,")
                    || line.starts_with("That address only works in a browser on this machine") =>
                {
                    "Paste the authorization code shown on the sign-in page, not a web address."
                }
                _ if line.starts_with("That looks like a verification code") => {
                    "Paste the authorization code shown after approval, not the email verification code."
                }
                _ if line.starts_with("That code is not from this sign-in") => {
                    "Paste the authorization code from this sign-in's browser page."
                }
                _ => continue,
            };
            send(output, "input_error", message).await;
        }
        Ok(())
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
#[path = "sandbox_tests.rs"]
mod sandbox_tests;

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
            transcript.update(&spec, "private diagnostic https://auth.openai.com/codex/device?access_token=not-public", &output).await.unwrap();
            assert!(lines.try_recv().is_err());
            transcript.update(&spec, raw, &output).await.unwrap();
            let mut kinds = Vec::new();
            while let Ok(line) = lines.try_recv() {
                assert!(!line.contains("private diagnostic"));
                let event: serde_json::Value = serde_json::from_str(&line).unwrap();
                kinds.push(event["kind"].as_str().unwrap().to_owned());
            }
            assert_eq!(kinds.first().unwrap(), "link", "{harness}");
            assert_eq!(kinds.last().unwrap(), expected, "{harness}");
            transcript.update(&spec, raw, &output).await.unwrap();
            assert!(lines.try_recv().is_err(), "duplicate {harness} events");
        }
    }

    #[tokio::test]
    async fn callback_requires_exact_destination_and_state_and_never_accepts_shell_syntax() {
        let (output, _lines) = mpsc::channel(16);
        let mut t = Transcript::default();
        t.update(&Spec::for_harness("agy").unwrap(), "https://accounts.google.com/o/oauth2/auth?client_id=test&redirect_uri=https%3A%2F%2Fantigravity.google%2Foauth-callback&response_type=code&state=test\nPaste authorization code", &output).await.unwrap();
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
        t.update(&Spec::for_harness("claude-code").unwrap(), "https://claude.com/cai/oauth/authorize?client_id=test&redirect_uri=http%3A%2F%2Flocalhost%3A9123%2Fcallback&response_type=code&state=test\n", &output).await.unwrap();
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
