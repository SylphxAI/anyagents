//! Remote Sandbox Process Spawn (`anyagents sandbox-exec`).
//!
//! Bridges `ClaudeHarness` (`stream-json` on stdin/stdout) to a remote sandbox
//! container (`POST /v1/sandboxes/{sandbox_id}/exec`), materializing `.mcp.json`
//! inside the remote workspace with the agent's Streamable HTTP `/mcp` URL and
//! bearer capability token.

use std::collections::{BTreeMap, HashMap};
use std::io::{BufRead, BufReader, Read as _, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr as _;
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::Duration;

use crate::sandbox_providers::providers::{
    DEFAULT_MAX_LEASE_TTL_MS, DEFAULT_MOUNT_PATH, DEFAULT_WORKDIR, EnsureSandboxRequest,
    ExecRequest, ImageSpec, LeasePolicy, ProviderKind, ProviderRegistry, SandboxHandle,
    SandboxProvider as AsyncSandboxProvider, SandboxRegistryConfig, VolumeSpec,
    build_bundle_tar_gz,
};
use crate::sandbox_providers::sylphx::{
    EnvdProcessEvent, EnvdSignal, SandboxDriver, SandboxMode, SylphxLeaseConfig,
    SylphxProviderConfig, ensure_leased_for_config_dir, renew_lease_for_config_dir,
};
use agent_tools::{RemoteSandboxWorkspace, SandboxConfig, WorkspaceBackend, sha256_hex};
use anyagents_core::stateless::SandboxBinding;
use anyagents_core::{Agent, AgentId, Error, RuntimeHeartbeat, RuntimeState, Store};
use anyagents_harness::normalize_harness_name;
use clap::Args;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Args, Clone, Debug, Default)]
pub struct SandboxExecArgs {
    /// Agent configuration directory containing `sandbox.json` and `mcp-capability`.
    #[arg(long)]
    pub config: Option<PathBuf>,
    /// Canonical harness identifier (`claude-code`, `codex`, `spiron`, `opencode`, `agy`, `muse`, `omp`, `dsh`, `grok`).
    #[arg(long)]
    pub harness: Option<String>,
    /// Target binary/program inside the remote sandbox.
    #[arg(long)]
    pub program: Option<String>,
    /// Existing session ID to resume.
    #[arg(long)]
    pub resume: Option<String>,
    /// Execute a single command/turn and exit instead of reading `stream-json` from stdin.
    #[arg(long)]
    pub oneshot: bool,
    /// Trailing arguments passed to the remote program.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub args: Vec<String>,
}

/// Extract `SandboxExecArgs` when `anyagents` is invoked via `ClaudeHarness`
/// with leading flags (`-p --input-format stream-json ... sandbox-exec --config ...`).
pub fn try_parse_from_raw_argv(argv: &[String]) -> Option<SandboxExecArgs> {
    let pos = argv.iter().position(|arg| arg == "sandbox-exec")?;
    let mut config: Option<PathBuf> = None;
    let mut harness: Option<String> = None;
    let mut program: Option<String> = None;
    let mut resume: Option<String> = None;
    let mut oneshot = false;
    let mut trailing_args = Vec::new();

    // Capture any `--resume <id>` placed before `sandbox-exec` as well.
    let mut pre = 0;
    while pre < pos {
        if argv[pre] == "--resume" && pre + 1 < pos {
            resume = Some(argv[pre + 1].clone());
            pre += 2;
        } else {
            pre += 1;
        }
    }

    let mut i = pos + 1;
    while i < argv.len() {
        match argv[i].as_str() {
            "--config" if i + 1 < argv.len() => {
                config = Some(PathBuf::from(&argv[i + 1]));
                i += 2;
            }
            "--harness" if i + 1 < argv.len() => {
                harness = Some(argv[i + 1].clone());
                i += 2;
            }
            "--program" if i + 1 < argv.len() => {
                program = Some(argv[i + 1].clone());
                i += 2;
            }
            "--resume" if i + 1 < argv.len() => {
                resume = Some(argv[i + 1].clone());
                i += 2;
            }
            "--oneshot" => {
                oneshot = true;
                i += 1;
            }
            "--" => {
                trailing_args.extend_from_slice(&argv[i + 1..]);
                break;
            }
            other => {
                trailing_args.push(other.to_string());
                i += 1;
            }
        }
    }

    Some(SandboxExecArgs {
        config,
        harness,
        program,
        resume,
        oneshot,
        args: trailing_args,
    })
}

pub fn run(args: SandboxExecArgs) -> Result<(), Error> {
    run_with_io(args, BufReader::new(std::io::stdin()), std::io::stdout())
}

/// Drive the `sandbox-exec` stream-json protocol against arbitrary reader/writer
/// streams so `HarnessHost::InProcess` under `remote_required` runs remote
/// sandbox harnesses in-process without spawning local OS shim processes.
pub fn run_with_io<R: BufRead + Send + 'static, W: Write + Send + 'static>(
    args: SandboxExecArgs,
    reader: R,
    writer: W,
) -> Result<(), Error> {
    run_with_io_in_store(args, reader, writer, None)
}

/// Same as [`run_with_io`], reusing an already-open coordinator [`Store`] for
/// PostgreSQL `agent_sandbox_bindings` persistence and guest heartbeat updates.
pub fn run_with_io_in_store<R: BufRead + Send + 'static, W: Write + Send + 'static>(
    args: SandboxExecArgs,
    reader: R,
    writer: W,
    store: Option<Arc<Store>>,
) -> Result<(), Error> {
    let config = args
        .config
        .or_else(|| std::env::var_os("ANYAGENTS_AGENT_CONFIG").map(PathBuf::from));
    let raw_harness = args
        .harness
        .or_else(|| std::env::var("ANYAGENTS_AGENT_HARNESS").ok())
        .unwrap_or_else(|| "claude-code".to_string());
    let harness = match raw_harness.trim() {
        "spiron-local" => "spiron".to_string(),
        other => normalize_harness_name(other).to_string(),
    };

    let data_dir = config
        .as_deref()
        .and_then(|c| c.parent())
        .and_then(|agent_dir| agent_dir.parent())
        .and_then(|agents_dir| agents_dir.parent())
        .map(Path::to_path_buf)
        .or_else(|| std::env::var_os("ANYAGENTS_DATA").map(PathBuf::from));

    let agent_id_str = config
        .as_deref()
        .and_then(|c| c.parent())
        .and_then(|p| p.file_name())
        .and_then(|s| s.to_str())
        .map(str::to_string);
    let parsed_agent_id = agent_id_str.as_deref().and_then(|s| AgentId::parse(s).ok());
    // When running in-process under `InProcessRemoteHarness`, the caller owns
    // heartbeat ingestion on the output stream so we avoid duplicate PG writes.
    // When invoked standalone (`store.is_none()`), open the store lazily if
    // available so guest heartbeats still flow into PG.
    let heartbeat_store = if store.is_some() {
        None
    } else {
        data_dir
            .as_deref()
            .and_then(|d| Store::open(d.join("state.db")).ok())
            .map(Arc::new)
    };
    let effective_store = store.clone().or_else(|| heartbeat_store.clone());

    let out_writer: Arc<Mutex<dyn Write + Send>> = Arc::new(Mutex::new(writer));

    if let Some(cfg_dir) = config.as_deref() {
        let aid = aid_or_default(agent_id_str.as_deref());
        // 1. Unified Track 3B `ProviderRegistry` path (`ANYAGENTS_SANDBOX_PROVIDER`,
        //    `ANYAGENTS_SANDBOX_IMAGE`, `ANYAGENTS_SANDBOX_TEMPLATE`, `LeasePolicy`),
        //    or an existing non-legacy PG `agent_sandbox_bindings` row.
        let existing_binding = effective_store
            .as_deref()
            .zip(parsed_agent_id.as_ref())
            .and_then(|(s, id)| s.sandbox_binding(id).ok().flatten());
        let registry_opt =
            match resolve_remote_provider_registry(data_dir.as_deref()).map_err(Error::Message)? {
                Some(reg) => Some(reg),
                None => existing_binding
                    .as_ref()
                    .filter(|b| b.provider != "legacy_http" && b.provider != "local")
                    .and_then(|b| resolve_registry_for_binding(data_dir.as_deref(), b).ok()),
            };

        if let Some(registry) = registry_opt {
            let remote_workdir = existing_binding
                .as_ref()
                .map(|b| b.remote_workdir.as_str())
                .filter(|s| !s.trim().is_empty());
            let (handle, binding_opt) = if let (Some(st), Some(agent_id)) =
                (effective_store.as_deref(), parsed_agent_id.as_ref())
            {
                let agent_name = st.agent(agent_id).ok().map(|a| a.name);
                let ensured = ensure_sandbox_with_registry(
                    st,
                    &registry,
                    agent_id,
                    agent_name.as_deref(),
                    cfg_dir,
                    remote_workdir,
                    &BTreeMap::new(),
                )
                .map_err(Error::Message)?;
                (ensured.handle, Some(ensured.binding))
            } else {
                let mut req = registry.default_ensure_request(aid);
                if let Some(wd) = remote_workdir {
                    req.workdir = Some(wd.to_string());
                }
                let handle =
                    block_on_provider(registry.active().ensure(&req)).map_err(Error::Message)?;
                (handle, None)
            };

            if handle.provider == ProviderKind::Sylphx {
                let lease = sylphx_lease_from_handle(
                    &handle,
                    &registry,
                    binding_opt.as_ref(),
                    data_dir.as_deref(),
                    cfg_dir,
                )
                .map_err(Error::Message)?;
                return run_sylphx_sandbox(
                    lease,
                    data_dir.as_deref(),
                    cfg_dir,
                    aid,
                    &harness,
                    args.program.as_deref(),
                    args.resume.as_deref(),
                    args.oneshot,
                    &args.args,
                    reader,
                    out_writer,
                    heartbeat_store.zip(parsed_agent_id),
                );
            }

            return run_provider_sandbox(
                registry.active(),
                handle,
                effective_store.as_deref(),
                data_dir.as_deref(),
                cfg_dir,
                aid,
                &harness,
                args.program.as_deref(),
                args.resume.as_deref(),
                args.oneshot,
                &args.args,
                reader,
                out_writer,
                heartbeat_store.zip(parsed_agent_id),
            );
        }

        // 2. Backwards-compatible file-only `sandbox-sylphx.json` / `SylphxProviderConfig` path.
        let mut lease_cfg = SylphxLeaseConfig::from_config_dir(cfg_dir);
        let sylphx_provider =
            SylphxProviderConfig::resolve(data_dir.as_deref()).map_err(Error::Message)?;
        if lease_cfg.is_some()
            || sylphx_provider
                .as_ref()
                .is_some_and(|p| p.driver == SandboxDriver::Sylphx)
        {
            let lease_env = BTreeMap::new();
            lease_cfg =
                ensure_leased_for_config_dir(data_dir.as_deref(), cfg_dir, aid, None, &lease_env)
                    .map_err(Error::Message)?;
        }
        if let Some(lease) = lease_cfg {
            return run_sylphx_sandbox(
                lease,
                data_dir.as_deref(),
                cfg_dir,
                aid,
                &harness,
                args.program.as_deref(),
                args.resume.as_deref(),
                args.oneshot,
                &args.args,
                reader,
                out_writer,
                heartbeat_store.zip(parsed_agent_id),
            );
        }
    }

    let mode = SandboxMode::from_env();
    let sbx = crate::sandbox_providers::resolve_sandbox_config(config.as_deref()).ok_or_else(|| {
        let suffix = if mode.is_remote_required() {
            " (ANYAGENTS_SANDBOX_MODE=remote_required forbids host fallback)"
        } else {
            ""
        };
        Error::Message(format!(
            "sandbox-exec requires a configured remote sandbox (`sandbox-sylphx.json`, `sandbox.json`, or `ANYAGENTS_SANDBOX_API_URL`){suffix}"
        ))
    })?;

    let program = args
        .program
        .filter(|p| !p.trim().is_empty())
        .unwrap_or_else(|| default_remote_program(&harness).to_string());

    let mut mcp_token: Option<String> = None;
    refresh_remote_mcp_config(&sbx, config.as_deref(), &mut mcp_token);

    if args.oneshot {
        let cmd_str = if args.args.is_empty() {
            program.clone()
        } else {
            format!("{} {}", program, args.args.join(" "))
        };
        let out = RemoteSandboxWorkspace::new(sbx)
            .exec(&cmd_str, None, Some(300_000))
            .map_err(Error::Message)?;
        if !out.stdout.is_empty() {
            let mut guard = out_writer.lock().unwrap_or_else(|e| e.into_inner());
            write!(guard, "{}", out.stdout)?;
            guard.flush()?;
        }
        if !out.stderr.is_empty() {
            eprint!("{}", out.stderr);
        }
        if out.exit_code != 0 {
            return Err(Error::Message(format!(
                "remote command exited with code {}",
                out.exit_code
            )));
        }
        return Ok(());
    }

    let mut session_id = args.resume.filter(|s| !s.trim().is_empty()).or_else(|| {
        config
            .as_deref()
            .and_then(|c| read_persisted_session_id(c, &harness))
    });

    for line in reader.lines() {
        let Ok(line) = line else { break };
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(event) = serde_json::from_str::<Value>(trimmed) else {
            continue;
        };
        let event_type = event.get("type").and_then(Value::as_str).unwrap_or("");
        if event_type != "user" {
            continue;
        }

        // Echo the user message back immediately so `ClaudeHarness` / `InProcessRemoteHarness`
        // records the message marker as echoed.
        {
            let mut stdout = out_writer.lock().unwrap_or_else(|e| e.into_inner());
            writeln!(stdout, "{trimmed}")?;
            stdout.flush()?;
        }

        let prompt_text = extract_user_prompt(&event);
        refresh_remote_mcp_config(&sbx, config.as_deref(), &mut mcp_token);
        match dispatch_remote_sandbox_turn(
            &sbx,
            &harness,
            &program,
            session_id.as_deref(),
            &prompt_text,
            &args.args,
        ) {
            Ok(outcome) => {
                session_id = Some(outcome.session_id.clone());
                if let Some(ref cfg_dir) = config {
                    persist_session_id(cfg_dir, &harness, &outcome.session_id);
                }
                let mut stdout = out_writer.lock().unwrap_or_else(|e| e.into_inner());
                for raw_line in &outcome.forwarded_stream_lines {
                    writeln!(stdout, "{raw_line}")?;
                }
                if outcome.forwarded_stream_lines.is_empty() {
                    let assistant_event = json!({
                        "type": "assistant",
                        "session_id": outcome.session_id,
                        "message": {
                            "role": "assistant",
                            "content": [
                                {
                                    "type": "text",
                                    "text": outcome.reply_text,
                                }
                            ]
                        }
                    });
                    writeln!(stdout, "{assistant_event}")?;
                }
                if !outcome.forwarded_has_result {
                    let result_event = json!({
                        "type": "result",
                        "subtype": if outcome.is_error { "error" } else { "success" },
                        "is_error": outcome.is_error,
                        "session_id": outcome.session_id,
                        "result": outcome.reply_text,
                        "usage": {
                            "input_tokens": outcome.input_tokens,
                            "output_tokens": outcome.output_tokens,
                        }
                    });
                    writeln!(stdout, "{result_event}")?;
                }
                stdout.flush()?;
            }
            Err(err_msg) => {
                let fallback_sid = session_id
                    .clone()
                    .unwrap_or_else(|| format!("{harness}-remote-session"));
                let result_event = json!({
                    "type": "result",
                    "subtype": "error",
                    "is_error": true,
                    "session_id": fallback_sid,
                    "result": err_msg,
                });
                let mut stdout = out_writer.lock().unwrap_or_else(|e| e.into_inner());
                writeln!(stdout, "{result_event}")?;
                stdout.flush()?;
            }
        }
    }

    Ok(())
}

#[derive(Clone, Debug)]
pub struct RemoteTurnOutcome {
    pub session_id: String,
    pub reply_text: String,
    pub is_error: bool,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub forwarded_stream_lines: Vec<String>,
    pub forwarded_has_result: bool,
}

fn default_remote_program(harness: &str) -> &'static str {
    match normalize_harness_name(harness) {
        "claude-code" => "claude",
        "codex" => "codex",
        "spiron" => "spiron",
        "opencode" => "opencode",
        "agy" => "agy",
        "muse" => "muse",
        "omp" => "omp",
        _ => "claude",
    }
}

/// Write the sandbox's `.mcp.json`: the server's public MCP URL and the
/// agent's current remote session token. Returns the token written, so a
/// caller rewrites the file only after a rotation. The agent's local mailbox
/// capability never enters the sandbox.
pub fn materialize_remote_mcp_config(
    sbx: &SandboxConfig,
    config_dir: &Path,
) -> Result<String, String> {
    let capability = crate::remote::session_token(config_dir)?;
    let mcp_url = crate::cloud_harness::resolve_public_mcp_url(config_dir)?;
    let mcp_doc = json!({
        "mcpServers": {
            "anyagents": {
                "type": "http",
                "url": mcp_url,
                "headers": {
                    "Authorization": format!("Bearer {capability}"),
                }
            }
        }
    });
    let mut bytes =
        serde_json::to_vec_pretty(&mcp_doc).map_err(|e| format!("serialize .mcp.json: {e}"))?;
    bytes.push(b'\n');
    RemoteSandboxWorkspace::new(sbx.clone())
        .write_file_bytes(Some(".mcp.json"), &bytes, false, true)
        .map(|_| capability)
}

/// Rewrite the sandbox's `.mcp.json` when the server rotated the session
/// token since `written`. A failure is reported once and retried next turn.
fn refresh_remote_mcp_config(
    sbx: &SandboxConfig,
    config: Option<&Path>,
    written: &mut Option<String>,
) {
    let Some(cfg_dir) = config else {
        return;
    };
    let current = crate::remote::session_token(cfg_dir).ok();
    if current.is_some() && current == *written {
        return;
    }
    match materialize_remote_mcp_config(sbx, cfg_dir) {
        Ok(token) => *written = Some(token),
        Err(why) => eprintln!("sandbox-exec: remote MCP config not written: {why}"),
    }
}

pub fn dispatch_remote_sandbox_turn(
    sbx: &SandboxConfig,
    harness: &str,
    program: &str,
    session_id: Option<&str>,
    prompt_text: &str,
    extra_args: &[String],
) -> Result<RemoteTurnOutcome, String> {
    let canonical = normalize_harness_name(harness);
    let cmd_str =
        build_remote_command_string(canonical, program, session_id, prompt_text, extra_args);
    let url = format!(
        "{}/v1/sandboxes/{}/exec",
        sbx.endpoint.trim_end_matches('/'),
        sbx.sandbox_id
    );
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(305)))
        .http_status_as_error(false)
        .build()
        .into();
    let mut req = agent.post(&url).header("content-type", "application/json");
    if let Some(ref token) = sbx.token {
        req = req.header("authorization", &format!("Bearer {token}"));
    }
    let payload = json!({
        "command": cmd_str,
        "program": program,
        "harness": canonical,
        "prompt": prompt_text,
        "session_id": session_id,
        "cwd": sbx.workdir,
        "timeout_ms": 300_000,
    });
    let mut resp = req
        .send_json(&payload)
        .map_err(|e| format!("remote sandbox exec failed ({url}): {e}"))?;
    let status = resp.status().as_u16();
    let mut body = Vec::new();
    let _ = resp.body_mut().as_reader().read_to_end(&mut body);
    if !(200..300).contains(&status) {
        return Err(format!(
            "remote sandbox exec returned HTTP {status}: {}",
            String::from_utf8_lossy(&body).trim()
        ));
    }
    let parsed: Value = serde_json::from_slice(&body)
        .map_err(|e| format!("invalid JSON from remote sandbox exec: {e}"))?;
    let stdout_str = parsed
        .get("stdout")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let stderr_str = parsed
        .get("stderr")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let exit_code = parsed
        .get("exit_code")
        .or_else(|| parsed.get("exitCode"))
        .and_then(Value::as_i64)
        .unwrap_or(0) as i32;

    let mut resolved_sid = parsed
        .get("session_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| session_id.map(str::to_string))
        .unwrap_or_else(|| format!("{canonical}-remote-1"));

    let mut forwarded_stream_lines = Vec::new();
    let mut forwarded_has_result = false;
    let mut extracted_reply = String::new();
    let mut input_tokens = 0u64;
    let mut output_tokens = 0u64;

    for raw_line in stdout_str.lines() {
        let l = raw_line.trim();
        if l.is_empty() {
            continue;
        }
        if let Ok(val) = serde_json::from_str::<Value>(l)
            && let Some(t) = val.get("type").and_then(Value::as_str)
            && matches!(t, "assistant" | "result" | "system")
        {
            if let Some(sid) = val
                .get("session_id")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                resolved_sid = sid.to_string();
            }
            if t == "result" {
                forwarded_has_result = true;
                if let Some(r) = val.get("result").and_then(Value::as_str) {
                    extracted_reply = r.to_string();
                }
                input_tokens = val
                    .pointer("/usage/input_tokens")
                    .and_then(Value::as_u64)
                    .unwrap_or(input_tokens);
                output_tokens = val
                    .pointer("/usage/output_tokens")
                    .and_then(Value::as_u64)
                    .unwrap_or(output_tokens);
            } else if t == "assistant"
                && extracted_reply.is_empty()
                && let Some(arr) = val.pointer("/message/content").and_then(Value::as_array)
            {
                for item in arr {
                    if let Some(txt) = item.get("text").and_then(Value::as_str) {
                        extracted_reply.push_str(txt);
                    }
                }
            }
            forwarded_stream_lines.push(l.to_string());
        }
    }

    if !forwarded_stream_lines.is_empty() {
        if extracted_reply.is_empty() {
            extracted_reply = "completed".to_string();
        }
        return Ok(RemoteTurnOutcome {
            session_id: resolved_sid,
            reply_text: extracted_reply,
            is_error: exit_code != 0,
            input_tokens,
            output_tokens,
            forwarded_stream_lines,
            forwarded_has_result,
        });
    }

    let is_error = exit_code != 0 && stdout_str.trim().is_empty();
    let reply_text = if !stdout_str.trim().is_empty() {
        stdout_str.trim().to_string()
    } else if !stderr_str.trim().is_empty() {
        stderr_str.trim().to_string()
    } else {
        "completed".to_string()
    };

    Ok(RemoteTurnOutcome {
        session_id: resolved_sid,
        reply_text,
        is_error,
        input_tokens: 0,
        output_tokens: 0,
        forwarded_stream_lines: Vec::new(),
        forwarded_has_result: false,
    })
}

fn aid_or_default(s: Option<&str>) -> &str {
    s.filter(|v| !v.trim().is_empty()).unwrap_or("agent")
}

/// Guest binary inside the sandbox VM (`anyagents-guest`).
fn guest_binary_path() -> String {
    std::env::var("ANYAGENTS_SANDBOX_GUEST_PATH")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "/usr/local/bin/anyagents-guest".to_string())
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct BundleManifest {
    #[serde(default)]
    volume_id: String,
    #[serde(default)]
    files: HashMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SyncedRepoSpec {
    repo: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    branch: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    base: Option<String>,
}

/// Result of ensuring a remote sandbox through [`ProviderRegistry`].
#[derive(Clone, Debug)]
pub(crate) struct EnsuredSandbox {
    pub handle: SandboxHandle,
    pub binding: SandboxBinding,
}

/// Parsed `anyagents-guest` NDJSON `{"type":"system","subtype":"heartbeat",...}` frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct GuestHeartbeatFrame {
    pub timestamp_ms: i64,
    pub turn_state: String,
    pub active_turns: u64,
    pub queued_turns: u64,
    pub inflight_children: i32,
    pub busy: bool,
}

impl GuestHeartbeatFrame {
    /// Fail-closed busy check: active turns, queued turns, non-idle turn state,
    /// explicit `busy`, or running remote child subprocesses all block idle release.
    pub(crate) fn is_busy(&self) -> bool {
        self.busy
            || self.active_turns > 0
            || self.queued_turns > 0
            || self.inflight_children > 0
            || (!self.turn_state.is_empty() && self.turn_state != "idle")
    }
}

/// Outcome of one singleton 2-tier idle-TTL sandbox sweep pass.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct IdleSweepReport {
    pub inspected: usize,
    pub released: Vec<AgentId>,
    pub skipped_busy: Vec<AgentId>,
    pub failed: Vec<(AgentId, String)>,
}

fn provider_runtime() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .thread_name("anyagents-sbx-provider")
            .build()
            .expect("create sandbox provider tokio runtime")
    })
}

pub(crate) fn block_on_provider<F, T>(fut: F) -> T
where
    F: std::future::Future<Output = T> + Send,
    T: Send,
{
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        return match handle.runtime_flavor() {
            tokio::runtime::RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(|| provider_runtime().block_on(fut))
            }
            _ => std::thread::scope(|s| {
                s.spawn(|| provider_runtime().block_on(fut))
                    .join()
                    .expect("sandbox provider thread panicked")
            }),
        };
    }
    provider_runtime().block_on(fut)
}

fn env_non_empty(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn raw_provider_file_selects_registry(data_dir: Option<&Path>) -> bool {
    let mut candidates = Vec::new();
    if let Some(p) = env_non_empty("ANYAGENTS_SANDBOX_PROVIDER_CONFIG") {
        candidates.push(PathBuf::from(p));
    }
    if let Some(dir) = data_dir {
        candidates.push(dir.join("sandbox-provider.json"));
    }
    for path in candidates {
        let Ok(raw) = std::fs::read_to_string(&path) else {
            continue;
        };
        let Ok(val) = serde_json::from_str::<Value>(&raw) else {
            continue;
        };
        let has_key = |k: &str| {
            val.get(k)
                .and_then(Value::as_str)
                .is_some_and(|s| !s.trim().is_empty())
        };
        // Explicit provider/driver or provider-specific fields select Track 3B
        // `ProviderRegistry`. Legacy `{ endpoint, remote_spawn: true }` files
        // without provider keys stay on `SandboxProviderConfig` for compatibility.
        if has_key("provider")
            || has_key("driver")
            || has_key("parent")
            || has_key("cloudflare_account_id")
            || has_key("modal_token_id")
            || has_key("modal_token_secret")
            || has_key("e2b_domain")
            || has_key("e2b_envd_base_url")
            || has_key("daytona_target")
        {
            return true;
        }
        return false;
    }
    false
}

fn env_selects_registry() -> bool {
    [
        "ANYAGENTS_SANDBOX_PROVIDER",
        "ANYAGENTS_SANDBOX_DRIVER",
        "ANYAGENTS_SYLPHX_PARENT",
        "ANYAGENTS_CLOUDFLARE_SANDBOX_URL",
        "CLOUDFLARE_SANDBOX_URL",
        "ANYAGENTS_SANDBOX_HTTP_URL",
        "ANYAGENTS_SANDBOX_PROVIDER_URL",
        "E2B_API_KEY",
        "DAYTONA_API_KEY",
        "ANYAGENTS_MODAL_ENDPOINT",
        "MODAL_SANDBOX_URL",
    ]
    .iter()
    .any(|k| env_non_empty(k).is_some())
}

/// Resolve the active remote [`ProviderRegistry`] when a non-local provider is
/// configured via `ANYAGENTS_SANDBOX_PROVIDER` (or provider-specific env/config).
pub(crate) fn resolve_remote_provider_registry(
    data_dir: Option<&Path>,
) -> Result<Option<ProviderRegistry>, String> {
    if !env_selects_registry() && !raw_provider_file_selects_registry(data_dir) {
        return Ok(None);
    }
    let reg = ProviderRegistry::from_env_or_config(data_dir, None)?;
    if reg.active_kind() == ProviderKind::Local {
        return Ok(None);
    }
    Ok(Some(reg))
}

/// Whether a remote [`ProviderRegistry`] provider (non-local) is configured.
pub(crate) fn has_remote_sandbox_provider(data_dir: Option<&Path>) -> bool {
    resolve_remote_provider_registry(data_dir)
        .ok()
        .flatten()
        .is_some()
}

fn binding_spec_value(binding: &SandboxBinding) -> Value {
    serde_json::from_str::<Value>(&binding.spec_json).unwrap_or_else(|_| json!({}))
}

fn overrides_from_binding(binding: &SandboxBinding) -> SandboxRegistryConfig {
    let spec = binding_spec_value(binding);
    let spec_str = |key: &str| {
        spec.get(key)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
    };
    SandboxRegistryConfig {
        provider: (!binding.provider.trim().is_empty()).then(|| binding.provider.clone()),
        endpoint: spec_str("endpoint"),
        image: binding.image.clone(),
        template: binding.template.clone(),
        shape: spec_str("shape"),
        parent: spec_str("parent"),
        volume_size_gib: binding.volume_size_gib.and_then(|v| u32::try_from(v).ok()),
        idle_ttl_ms: (binding.idle_ttl_ms >= 0).then_some(binding.idle_ttl_ms as u64),
        max_lease_ttl_ms: spec.get("max_lease_ttl_ms").and_then(Value::as_u64),
        mount_path: spec_str("mount_path"),
        workdir: (!binding.remote_workdir.trim().is_empty())
            .then(|| binding.remote_workdir.clone()),
        ..SandboxRegistryConfig::default()
    }
}

/// Resolve a [`ProviderRegistry`] targeting an existing persisted [`SandboxBinding`]
/// (combining environment credentials with the binding's provider/endpoint/parent).
pub(crate) fn resolve_registry_for_binding(
    data_dir: Option<&Path>,
    binding: &SandboxBinding,
) -> Result<ProviderRegistry, String> {
    let overrides = overrides_from_binding(binding);
    ProviderRegistry::from_env_or_config(data_dir, Some(&overrides))
}

/// Convert a persisted [`SandboxBinding`] into a [`SandboxHandle`] for provider reuse or release.
pub(crate) fn sandbox_handle_from_binding(binding: &SandboxBinding) -> Option<SandboxHandle> {
    let provider = ProviderKind::from_str(&binding.provider).ok()?;
    let spec = binding_spec_value(binding);
    let mount_path = spec
        .get("mount_path")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(DEFAULT_MOUNT_PATH)
        .to_string();
    let state_dir = spec
        .get("state_dir")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| {
            if mount_path == "/home/user/agent-state" {
                mount_path.clone()
            } else {
                format!("{}/state", mount_path.trim_end_matches('/'))
            }
        });
    let workdir = if binding.remote_workdir.trim().is_empty() {
        DEFAULT_WORKDIR.to_string()
    } else {
        binding.remote_workdir.clone()
    };
    let mut metadata: BTreeMap<String, String> = spec
        .get("metadata")
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .unwrap_or_default();
    if !binding.volume_resource.trim().is_empty() {
        metadata
            .entry("volume_resource".to_string())
            .or_insert_with(|| binding.volume_resource.clone());
    }
    if let Some(ref lr) = binding.lease_resource
        && !lr.trim().is_empty()
    {
        metadata
            .entry("lease_resource".to_string())
            .or_insert_with(|| lr.clone());
    }
    if let Some(ref dp) = binding.data_plane_resource
        && !dp.trim().is_empty()
    {
        metadata
            .entry("data_plane_resource".to_string())
            .or_insert_with(|| dp.clone());
    }
    if let Some(parent) = spec
        .get("parent")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        metadata
            .entry("parent".to_string())
            .or_insert_with(|| parent.to_string());
    }
    let expires_at = binding
        .lease_expires_at_ms
        .and_then(chrono::DateTime::from_timestamp_millis)
        .map(|dt| dt.to_rfc3339());
    Some(SandboxHandle {
        provider,
        sandbox_id: binding.lease_id.clone().unwrap_or_default(),
        volume_id: (!binding.volume_id.trim().is_empty()).then(|| binding.volume_id.clone()),
        mount_path,
        state_dir,
        workdir,
        transport_url: binding
            .transport_url
            .clone()
            .or_else(|| binding.guest_endpoint.clone()),
        transport_token: binding.transport_token.clone(),
        expires_at,
        metadata,
    })
}

fn binding_from_handle(
    agent_id: &AgentId,
    existing: Option<&SandboxBinding>,
    handle: &SandboxHandle,
    req: &EnsureSandboxRequest,
    provider_endpoint: Option<&str>,
    provider_parent: Option<&str>,
    allow_transport_token: bool,
) -> SandboxBinding {
    let mut binding = existing
        .cloned()
        .unwrap_or_else(|| SandboxBinding::new(agent_id.clone(), handle.provider.as_str()));
    binding.provider = handle.provider.as_str().to_string();
    binding.image = req
        .image
        .image
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| existing.and_then(|b| b.image.clone()));
    binding.template = req
        .image
        .template
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| existing.and_then(|b| b.template.clone()));
    if !handle.workdir.trim().is_empty() && !handle.workdir.ends_with("/launcher") {
        binding.remote_workdir = handle.workdir.clone();
    }
    let vol_id = handle
        .volume_id
        .clone()
        .filter(|s| !s.trim().is_empty())
        .or_else(|| {
            existing
                .map(|b| b.volume_id.clone())
                .filter(|s| !s.trim().is_empty())
        })
        .unwrap_or_default();
    let vol_res = handle
        .metadata
        .get("volume_resource")
        .cloned()
        .filter(|s| !s.trim().is_empty())
        .or_else(|| {
            existing
                .map(|b| b.volume_resource.clone())
                .filter(|s| !s.trim().is_empty())
        })
        .unwrap_or_else(|| vol_id.clone());
    binding.volume_id = vol_id;
    binding.volume_resource = vol_res;
    binding.volume_size_gib = req
        .volume
        .as_ref()
        .and_then(|v| v.size_gib)
        .map(i64::from)
        .or_else(|| existing.and_then(|b| b.volume_size_gib));
    binding.lease_id = (!handle.sandbox_id.trim().is_empty()).then(|| handle.sandbox_id.clone());
    binding.lease_resource = handle
        .metadata
        .get("lease_resource")
        .cloned()
        .filter(|s| !s.trim().is_empty());
    binding.data_plane_resource = handle
        .metadata
        .get("data_plane_resource")
        .cloned()
        .filter(|s| !s.trim().is_empty());
    binding.transport_url = handle.transport_url.clone();
    binding.guest_endpoint = handle.transport_url.clone();
    binding.transport_token = if allow_transport_token {
        handle.transport_token.clone()
    } else {
        None
    };
    binding.lease_state = if binding.lease_id.is_some() {
        "ready".to_string()
    } else {
        "detached".to_string()
    };
    let now = anyagents_core::now_ms();
    binding.lease_expires_at_ms = handle
        .expires_at
        .as_deref()
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.timestamp_millis())
        .or_else(|| Some(now.saturating_add(req.lease.max_lease_ttl_ms as i64)));
    binding.idle_since_ms = None;
    binding.idle_ttl_ms = req.lease.idle_ttl_ms as i64;
    binding.last_heartbeat_ms = Some(now);

    let prev_spec = existing
        .map(binding_spec_value)
        .unwrap_or_else(|| json!({}));
    let endpoint = provider_endpoint
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| {
            prev_spec
                .get("endpoint")
                .and_then(Value::as_str)
                .filter(|s| !s.trim().is_empty())
                .map(str::to_string)
        })
        .or_else(|| env_non_empty("ANYAGENTS_SANDBOX_ENDPOINT"))
        .or_else(|| env_non_empty("ANYAGENTS_SANDBOX_HTTP_URL"))
        .or_else(|| env_non_empty("ANYAGENTS_SANDBOX_PROVIDER_URL"));
    let parent = provider_parent
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| handle.metadata.get("parent").cloned())
        .or_else(|| {
            prev_spec
                .get("parent")
                .and_then(Value::as_str)
                .filter(|s| !s.trim().is_empty())
                .map(str::to_string)
        })
        .or_else(|| env_non_empty("ANYAGENTS_SYLPHX_PARENT"));
    let mut spec = serde_json::Map::new();
    if let Some(ep) = endpoint {
        spec.insert("endpoint".into(), json!(ep));
    }
    if let Some(p) = parent {
        spec.insert("parent".into(), json!(p));
    }
    if let Some(ref shape) = req.shape {
        spec.insert("shape".into(), json!(shape));
    }
    spec.insert("mount_path".into(), json!(handle.mount_path));
    spec.insert("state_dir".into(), json!(handle.state_dir));
    spec.insert("max_lease_ttl_ms".into(), json!(req.lease.max_lease_ttl_ms));
    spec.insert("metadata".into(), json!(handle.metadata));
    binding.spec_json = Value::Object(spec).to_string();
    binding
}

fn sylphx_lease_from_handle(
    handle: &SandboxHandle,
    registry: &ProviderRegistry,
    binding: Option<&SandboxBinding>,
    data_dir: Option<&Path>,
    config_dir: &Path,
) -> Result<SylphxLeaseConfig, String> {
    let spec = binding.map(binding_spec_value).unwrap_or_else(|| json!({}));
    let existing = SylphxLeaseConfig::from_config_dir(config_dir);
    let sylphx_cfg = SylphxProviderConfig::resolve(data_dir).ok().flatten();
    let endpoint = spec
        .get("endpoint")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .map(str::to_string)
        .or_else(|| sylphx_cfg.as_ref().map(|c| c.endpoint.clone()))
        .or_else(|| existing.as_ref().map(|c| c.endpoint.clone()))
        .or_else(|| env_non_empty("ANYAGENTS_SANDBOX_ENDPOINT"))
        .or_else(|| env_non_empty("SYLPHX_SANDBOX_URL"))
        .or_else(|| env_non_empty("ANYAGENTS_SYLPHX_ENDPOINT"))
        .unwrap_or_else(|| "https://api.sylphx.com".to_string());
    let token = sylphx_cfg
        .as_ref()
        .and_then(|c| c.token.clone())
        .or_else(|| existing.as_ref().and_then(|c| c.token.clone()))
        .or_else(|| env_non_empty("ANYAGENTS_SANDBOX_TOKEN"))
        .or_else(|| env_non_empty("SYLPHX_API_KEY"))
        .or_else(|| env_non_empty("ANYAGENTS_SYLPHX_TOKEN"));
    let parent = handle
        .metadata
        .get("parent")
        .cloned()
        .or_else(|| {
            spec.get("parent")
                .and_then(Value::as_str)
                .filter(|s| !s.trim().is_empty())
                .map(str::to_string)
        })
        .or_else(|| sylphx_cfg.as_ref().map(|c| c.parent.clone()))
        .or_else(|| existing.as_ref().map(|c| c.parent.clone()))
        .or_else(|| env_non_empty("ANYAGENTS_SYLPHX_PARENT"))
        .ok_or_else(|| "Sylphx sandbox handle missing `parent` resource".to_string())?;
    let template = binding
        .and_then(|b| b.template.clone())
        .or_else(|| registry.default_image_spec().template.clone())
        .or_else(|| sylphx_cfg.as_ref().map(|c| c.template.clone()))
        .or_else(|| existing.as_ref().map(|c| c.template.clone()))
        .unwrap_or_else(|| crate::sandbox_providers::sylphx::DEFAULT_TEMPLATE.to_string());
    let shape = spec
        .get("shape")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .map(str::to_string)
        .or_else(|| sylphx_cfg.as_ref().map(|c| c.shape.clone()))
        .or_else(|| existing.as_ref().map(|c| c.shape.clone()))
        .unwrap_or_else(|| crate::sandbox_providers::sylphx::DEFAULT_SHAPE.to_string());
    let volume_id = handle
        .volume_id
        .clone()
        .or_else(|| binding.map(|b| b.volume_id.clone()))
        .unwrap_or_default();
    let volume_resource = handle
        .metadata
        .get("volume_resource")
        .cloned()
        .or_else(|| binding.map(|b| b.volume_resource.clone()))
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| format!("{parent}/volumes/{volume_id}"));
    let lease_resource = handle
        .metadata
        .get("lease_resource")
        .cloned()
        .or_else(|| binding.and_then(|b| b.lease_resource.clone()))
        .or_else(|| Some(format!("{parent}/leases/{}", handle.sandbox_id)));
    let data_plane_resource = handle
        .metadata
        .get("data_plane_resource")
        .cloned()
        .or_else(|| binding.and_then(|b| b.data_plane_resource.clone()))
        .or_else(|| lease_resource.clone());
    Ok(SylphxLeaseConfig {
        endpoint,
        token,
        parent,
        template,
        shape,
        volume_size_gib: binding
            .and_then(|b| b.volume_size_gib)
            .and_then(|v| u64::try_from(v).ok()),
        workdir: handle.workdir.clone(),
        mount_path: handle.mount_path.clone(),
        volume_id,
        volume_resource,
        lease_id: (!handle.sandbox_id.is_empty()).then(|| handle.sandbox_id.clone()),
        lease_resource,
        data_plane_resource,
        envd_url: handle.transport_url.clone(),
        envd_token: handle.transport_token.clone(),
        expires_at: handle.expires_at.clone(),
    })
}

/// Ensure a remote sandbox via an explicit [`AsyncSandboxProvider`], persisting
/// provider/image/template/volume/lease metadata into `agent_sandbox_bindings`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn ensure_sandbox_with_provider(
    store: &Store,
    provider: Arc<dyn AsyncSandboxProvider>,
    image: &ImageSpec,
    lease_policy: &LeasePolicy,
    shape: Option<String>,
    volume_size_gib: Option<u32>,
    agent_id: &AgentId,
    agent_name: Option<&str>,
    config_dir: &Path,
    remote_workdir: Option<&str>,
    extra_env: &BTreeMap<String, String>,
) -> Result<EnsuredSandbox, String> {
    let existing = store.sandbox_binding(agent_id).map_err(|e| e.to_string())?;
    let mut vol = VolumeSpec::for_agent(agent_id.as_str());
    vol.size_gib = volume_size_gib.or_else(|| {
        existing
            .as_ref()
            .and_then(|b| b.volume_size_gib)
            .and_then(|v| u32::try_from(v).ok())
    });
    if let Some(ref ex) = existing {
        let spec = binding_spec_value(ex);
        if let Some(mp) = spec
            .get("mount_path")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            vol.mount_path = mp.to_string();
        }
        if !ex.volume_id.trim().is_empty() {
            vol.id_or_name = ex.volume_id.clone();
        }
    }

    let mut req_image = image.clone();
    if req_image.image.is_none() {
        req_image.image = existing.as_ref().and_then(|b| b.image.clone());
    }
    if req_image.template.is_none() {
        req_image.template = existing.as_ref().and_then(|b| b.template.clone());
    }
    req_image.env.extend(extra_env.clone());

    let existing_handle = existing
        .as_ref()
        .filter(|b| ProviderKind::from_str(&b.provider).ok() == Some(provider.kind()))
        .and_then(sandbox_handle_from_binding);

    let req = EnsureSandboxRequest {
        agent_id: agent_id.as_str().to_string(),
        agent_name: agent_name.map(str::to_string),
        image: req_image,
        volume: Some(vol),
        shape,
        network: crate::sandbox_providers::providers::NetworkPolicySpec::Allow,
        lease: lease_policy.clone(),
        existing: existing_handle,
        workdir: remote_workdir
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .or_else(|| {
                existing
                    .as_ref()
                    .map(|b| b.remote_workdir.clone())
                    .filter(|s| !s.trim().is_empty())
            }),
    };

    let handle = block_on_provider(provider.ensure(&req))?;
    let allow_transport_token = !store.is_postgres()
        || anyagents_core::MasterKey::from_env()
            .ok()
            .flatten()
            .is_some();
    let binding = binding_from_handle(
        agent_id,
        existing.as_ref(),
        &handle,
        &req,
        None,
        None,
        allow_transport_token,
    );
    let stored = store
        .sandbox_binding_store()
        .upsert(&binding)
        .map_err(|e| format!("failed to persist sandbox binding: {e}"))?;

    // Preserve backwards-compatible local `sandbox-sylphx.json` mirror when Sylphx
    // is active and the config dir is writable.
    if handle.provider == ProviderKind::Sylphx
        && let Ok(reg) = resolve_registry_for_binding(Some(store.data_dir()), &stored)
        && let Ok(lease_cfg) = sylphx_lease_from_handle(
            &handle,
            &reg,
            Some(&stored),
            Some(store.data_dir()),
            config_dir,
        )
    {
        let _ = lease_cfg.save_to_config_dir(config_dir);
    }

    Ok(EnsuredSandbox {
        handle,
        binding: stored,
    })
}

/// Ensure a remote sandbox using a resolved [`ProviderRegistry`].
pub(crate) fn ensure_sandbox_with_registry(
    store: &Store,
    registry: &ProviderRegistry,
    agent_id: &AgentId,
    agent_name: Option<&str>,
    config_dir: &Path,
    remote_workdir: Option<&str>,
    extra_env: &BTreeMap<String, String>,
) -> Result<EnsuredSandbox, String> {
    let template_req = registry.default_ensure_request(agent_id.as_str());
    ensure_sandbox_with_provider(
        store,
        registry.active(),
        registry.default_image_spec(),
        registry.default_lease_policy(),
        template_req.shape,
        template_req.volume.and_then(|v| v.size_gib),
        agent_id,
        agent_name,
        config_dir,
        remote_workdir,
        extra_env,
    )
}

/// Ensure a remote sandbox for `agent` when a remote provider is configured
/// (via `ANYAGENTS_SANDBOX_PROVIDER`, `ANYAGENTS_SANDBOX_IMAGE`,
/// `ANYAGENTS_SANDBOX_TEMPLATE`, `sandbox-provider.json`, or an existing PG
/// `agent_sandbox_bindings` row). Returns `Ok(None)` when local mode applies.
pub(crate) fn ensure_sandbox_for_agent(
    store: &Store,
    agent: &Agent,
    config_dir: &Path,
    remote_workdir: Option<&str>,
    extra_env: &BTreeMap<String, String>,
) -> Result<Option<SandboxBinding>, String> {
    if let Some(registry) = resolve_remote_provider_registry(Some(store.data_dir()))? {
        let ensured = ensure_sandbox_with_registry(
            store,
            &registry,
            &agent.id,
            Some(&agent.name),
            config_dir,
            remote_workdir,
            extra_env,
        )?;
        return Ok(Some(ensured.binding));
    }

    if let Some(existing) = store.sandbox_binding(&agent.id).ok().flatten()
        && existing.provider != "legacy_http"
        && existing.provider != "local"
    {
        let registry = resolve_registry_for_binding(Some(store.data_dir()), &existing)?;
        if registry.active_kind() != ProviderKind::Local {
            let ensured = ensure_sandbox_with_registry(
                store,
                &registry,
                &agent.id,
                Some(&agent.name),
                config_dir,
                remote_workdir,
                extra_env,
            )?;
            return Ok(Some(ensured.binding));
        }
    }

    // Legacy file-only `sandbox-sylphx.json` without env vars.
    if let Some(leased) = ensure_leased_for_config_dir(
        Some(store.data_dir()),
        config_dir,
        agent.id.as_str(),
        remote_workdir,
        extra_env,
    )? {
        let mut binding = store
            .sandbox_binding(&agent.id)
            .ok()
            .flatten()
            .unwrap_or_else(|| SandboxBinding::new(agent.id.clone(), "sylphx"));
        binding.provider = "sylphx".to_string();
        binding.template = Some(leased.template.clone());
        if !leased.workdir.trim().is_empty() && !leased.workdir.ends_with("/launcher") {
            binding.remote_workdir = leased.workdir.clone();
        }
        binding.volume_id = leased.volume_id.clone();
        binding.volume_resource = leased.volume_resource.clone();
        binding.volume_size_gib = leased.volume_size_gib.map(|v| v as i64);
        binding.lease_id = leased.lease_id.clone();
        binding.lease_resource = leased.lease_resource.clone();
        binding.data_plane_resource = leased.data_plane_resource.clone();
        binding.transport_url = leased.envd_url.clone();
        binding.guest_endpoint = leased.envd_url.clone();
        if !store.is_postgres()
            || anyagents_core::MasterKey::from_env()
                .ok()
                .flatten()
                .is_some()
        {
            binding.transport_token = leased.envd_token.clone();
        }
        binding.lease_state = if leased.lease_id.is_some() {
            "ready".to_string()
        } else {
            "detached".to_string()
        };
        binding.idle_since_ms = None;
        binding.last_heartbeat_ms = Some(anyagents_core::now_ms());
        binding.spec_json = json!({
            "endpoint": leased.endpoint,
            "parent": leased.parent,
            "shape": leased.shape,
            "mount_path": leased.mount_path,
            "state_dir": leased.state_dir(),
        })
        .to_string();
        let stored = store
            .sandbox_binding_store()
            .upsert(&binding)
            .map_err(|e| format!("failed to persist sandbox binding: {e}"))?;
        return Ok(Some(stored));
    }

    Ok(None)
}

fn clear_local_sylphx_lease_only(config_dir: &Path) {
    if let Some(mut cfg) = SylphxLeaseConfig::from_config_dir(config_dir) {
        cfg.lease_id = None;
        cfg.lease_resource = None;
        cfg.data_plane_resource = None;
        cfg.envd_url = None;
        cfg.envd_token = None;
        cfg.expires_at = None;
        let _ = cfg.save_to_config_dir(config_dir);
    }
}

/// Release compute for a persisted [`SandboxBinding`] using a concrete provider,
/// preserving `/home/user/agent-data` persistent volumes and clearing only the
/// ephemeral lease fields in `agent_sandbox_bindings`.
pub(crate) fn release_sandbox_binding_with_provider(
    store: &Store,
    binding: &SandboxBinding,
    provider: &dyn AsyncSandboxProvider,
    config_dir: Option<&Path>,
) -> Result<bool, String> {
    let Some(handle) = sandbox_handle_from_binding(binding) else {
        return Ok(false);
    };
    if handle.sandbox_id.trim().is_empty() && binding.lease_resource.is_none() {
        let _ = store.sandbox_binding_store().clear_lease(&binding.agent_id);
        return Ok(false);
    }
    let _ = store.sandbox_binding_store().update_lease_state(
        &binding.agent_id,
        "releasing",
        binding.idle_since_ms,
    );
    if let Err(err) = block_on_provider(provider.release(&handle)) {
        // Restore prior state so the next sweep retries cleanly.
        let _ = store.sandbox_binding_store().update_lease_state(
            &binding.agent_id,
            &binding.lease_state,
            binding.idle_since_ms,
        );
        return Err(err);
    }
    store
        .sandbox_binding_store()
        .clear_lease(&binding.agent_id)
        .map_err(|e| e.to_string())?;
    if let Some(cfg_dir) = config_dir {
        clear_local_sylphx_lease_only(cfg_dir);
    }
    Ok(true)
}

/// Release compute for `agent_id` (on explicit pause, archive, or batch idle),
/// preserving persistent volumes.
pub(crate) fn release_sandbox_for_agent(
    store: &Store,
    agent_id: &AgentId,
    config_dir: &Path,
) -> Result<bool, String> {
    if let Some(binding) = store.sandbox_binding(agent_id).ok().flatten() {
        if binding.lease_id.is_none() && binding.lease_resource.is_none() {
            let _ = store.sandbox_binding_store().clear_lease(agent_id);
            return Ok(false);
        }
        if binding.provider != "legacy_http"
            && binding.provider != "local"
            && let Ok(registry) = resolve_registry_for_binding(Some(store.data_dir()), &binding)
            && registry.active_kind() != ProviderKind::Local
        {
            return release_sandbox_binding_with_provider(
                store,
                &binding,
                registry.active().as_ref(),
                Some(config_dir),
            );
        }
    }
    crate::sandbox_providers::release_sandbox_for_config_dir(config_dir)?;
    let _ = store.sandbox_binding_store().clear_lease(agent_id);
    Ok(true)
}

/// Handle supervisor idle notification with 2-tier warm-hold policy:
/// - Never releases while `inflight_children > 0`.
/// - When `idle_ttl_ms > 0`, marks `idle_since_ms = Some(now)` on both
///   `agent_runtime` and `agent_sandbox_bindings` and leaves the compute lease
///   warm until the singleton PG sweeper releases it after `idle_ttl_ms`.
/// - When `idle_ttl_ms == 0` (batch policy), releases compute immediately while
///   preserving the persistent volume.
pub(crate) fn handle_agent_idle_release(store: &Store, agent_id: &AgentId, config_dir: &Path) {
    let now = anyagents_core::now_ms();
    if let Ok(Some(hb)) = store.runtime_heartbeat(agent_id)
        && hb.inflight_children > 0
    {
        let _ = store
            .sandbox_binding_store()
            .update_lease_state(agent_id, "ready", None);
        return;
    }

    if let Ok(Some(binding)) = store.sandbox_binding(agent_id) {
        if binding.lease_state != "ready" {
            return;
        }
        if binding.idle_ttl_ms > 0 {
            let idle_since = binding.idle_since_ms.unwrap_or(now);
            let _ = store.sandbox_binding_store().update_lease_state(
                agent_id,
                "ready",
                Some(idle_since),
            );
            let prev = store.runtime_heartbeat(agent_id).ok().flatten();
            let _ = store.update_runtime_heartbeat(
                agent_id,
                &RuntimeHeartbeat {
                    owner_replica_id: Some(store.replica_id().to_string()),
                    turn_state: "idle".to_string(),
                    retry_at_ms: prev.as_ref().and_then(|p| p.retry_at_ms),
                    heartbeat_at_ms: Some(now),
                    idle_since_ms: Some(idle_since),
                    inflight_children: 0,
                },
            );
            return;
        }
        if let Err(err) = release_sandbox_for_agent(store, agent_id, config_dir) {
            eprintln!("anyagents-sandbox: immediate batch idle release failed: {err}");
        }
        return;
    }

    if let Err(err) = crate::sandbox_providers::release_sandbox_for_config_dir(config_dir) {
        eprintln!("anyagents-sandbox: idle release failed: {err}");
    }
}

/// Parse an incoming `anyagents-guest` NDJSON `system/heartbeat` event.
pub(crate) fn parse_guest_heartbeat(val: &Value) -> Option<GuestHeartbeatFrame> {
    if val.get("type").and_then(Value::as_str) != Some("system")
        || val.get("subtype").and_then(Value::as_str) != Some("heartbeat")
    {
        return None;
    }
    let timestamp_ms = val
        .get("timestamp_ms")
        .and_then(|v| {
            v.as_i64()
                .or_else(|| v.as_u64().and_then(|u| i64::try_from(u).ok()))
        })
        .filter(|&ts| ts > 0)
        .unwrap_or_else(anyagents_core::now_ms);
    let active_turns = val.get("active_turns").and_then(Value::as_u64).unwrap_or(0);
    let queued_turns = val.get("queued_turns").and_then(Value::as_u64).unwrap_or(0);
    let inflight_children = val
        .get("inflight_children")
        .and_then(|v| {
            v.as_i64()
                .or_else(|| v.as_u64().map(|u| u.min(i32::MAX as u64) as i64))
        })
        .unwrap_or(0)
        .clamp(0, i32::MAX as i64) as i32;
    let turn_state = val
        .get("turn_state")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| {
            if active_turns > 0 || queued_turns > 0 {
                "in_turn".to_string()
            } else {
                "idle".to_string()
            }
        });
    let busy = val.get("busy").and_then(Value::as_bool).unwrap_or(false)
        || active_turns > 0
        || queued_turns > 0
        || inflight_children > 0
        || turn_state != "idle";
    Some(GuestHeartbeatFrame {
        timestamp_ms,
        turn_state,
        active_turns,
        queued_turns,
        inflight_children,
        busy,
    })
}

/// Persist a parsed guest heartbeat frame into PostgreSQL `agent_runtime`
/// (`heartbeat_at_ms`, `inflight_children`, `idle_since_ms`) and synchronize
/// `agent_sandbox_bindings` (`last_heartbeat_ms`, `idle_since_ms`).
pub(crate) fn apply_guest_heartbeat(
    store: &Store,
    agent_id: &AgentId,
    frame: &GuestHeartbeatFrame,
    now_ms: i64,
) -> Result<(), Error> {
    let prev = store.runtime_heartbeat(agent_id)?;
    let heartbeat_at = if frame.timestamp_ms > 0 {
        frame.timestamp_ms
    } else {
        now_ms
    };
    let busy = frame.is_busy();
    let idle_since_ms = if busy {
        None
    } else {
        prev.as_ref()
            .and_then(|p| p.idle_since_ms)
            .or(Some(heartbeat_at))
    };
    let turn_state = if frame.turn_state.trim().is_empty() {
        if busy {
            "in_turn".to_string()
        } else {
            "idle".to_string()
        }
    } else {
        frame.turn_state.clone()
    };
    let hb = RuntimeHeartbeat {
        owner_replica_id: Some(store.replica_id().to_string()),
        turn_state,
        retry_at_ms: prev.and_then(|p| p.retry_at_ms),
        heartbeat_at_ms: Some(heartbeat_at),
        idle_since_ms,
        inflight_children: frame.inflight_children.max(0),
    };
    let _ = store.update_runtime_heartbeat(agent_id, &hb)?;

    let bindings = store.sandbox_binding_store();
    if let Some(binding) = bindings.get(agent_id)? {
        let _ = bindings.touch_heartbeat(agent_id, heartbeat_at);
        if binding.lease_state == "ready" {
            let next_binding_idle = if busy {
                None
            } else {
                binding.idle_since_ms.or(idle_since_ms)
            };
            if binding.idle_since_ms != next_binding_idle {
                let _ = bindings.update_lease_state(agent_id, "ready", next_binding_idle);
            }
        }
    }
    Ok(())
}

/// Parse and apply an `anyagents-guest` NDJSON frame if it is a `system/heartbeat`.
/// Returns `true` when `val` was a heartbeat frame.
pub(crate) fn ingest_guest_heartbeat_value(store: &Store, agent_id: &AgentId, val: &Value) -> bool {
    let Some(frame) = parse_guest_heartbeat(val) else {
        return false;
    };
    if let Err(err) = apply_guest_heartbeat(store, agent_id, &frame, anyagents_core::now_ms()) {
        eprintln!("anyagents-sandbox: failed to record guest heartbeat for {agent_id}: {err}");
    }
    true
}

/// Singleton 2-tier idle-TTL sandbox sweeper (invoked under `maint:singleton`).
///
/// Releases idle compute leases whose `idle_since_ms + idle_ttl_ms <= now_ms`
/// ONLY when:
/// - `agent_runtime.inflight_children == 0`
/// - `agent_runtime.turn_state == "idle"` (and no fresher busy heartbeat)
/// - no unfinished turn and no undelivered mailbox message exist
/// - runtime is not actively `starting`
///
/// Always calls `provider.release(&handle)` (NEVER `destroy()`) so the persistent
/// `/home/user/agent-data` volume survives compute release intact.
pub(crate) fn sweep_idle_sandboxes_with_resolver<F>(
    store: &Store,
    data_dir: &Path,
    now_ms: i64,
    mut resolve_provider: F,
) -> Result<IdleSweepReport, Error>
where
    F: FnMut(&SandboxBinding) -> Result<Option<Arc<dyn AsyncSandboxProvider>>, String>,
{
    let mut report = IdleSweepReport::default();
    let due = store.sandbox_binding_store().list_idle_due(now_ms)?;
    for binding in due {
        report.inspected += 1;
        let Some(idle_since) = binding.idle_since_ms else {
            continue;
        };
        if binding.lease_state != "ready" || idle_since.saturating_add(binding.idle_ttl_ms) > now_ms
        {
            continue;
        }

        // Fail-closed causal guards: never release compute while remote children,
        // active turns, pending wakes, or startup transitions exist.
        let busy_from_hb = match store.runtime_heartbeat(&binding.agent_id) {
            Ok(Some(hb)) => {
                hb.inflight_children > 0
                    || (!hb.turn_state.trim().is_empty() && hb.turn_state != "idle")
                    || (hb.idle_since_ms.is_none()
                        && hb.heartbeat_at_ms.is_some_and(|t| t >= idle_since))
            }
            Ok(None) => false,
            Err(_) => true,
        };
        let has_unfinished_turn = match store.unfinished_turn(&binding.agent_id) {
            Ok(turn) => turn.is_some(),
            Err(_) => true,
        };
        let has_undelivered = match store.next_undelivered(&binding.agent_id) {
            Ok(msg) => msg.is_some(),
            Err(_) => true,
        };
        let is_starting = match store.agent_runtime(&binding.agent_id) {
            Ok(Some(rt)) => rt.state == RuntimeState::Starting,
            Ok(None) => false,
            Err(_) => true,
        };
        let has_active_remote_turn = match store
            .remote_turn_store()
            .list_active_for_agent(&binding.agent_id)
        {
            Ok(turns) => !turns.is_empty(),
            Err(_) => true,
        };

        if busy_from_hb
            || has_unfinished_turn
            || has_undelivered
            || is_starting
            || has_active_remote_turn
        {
            let _ =
                store
                    .sandbox_binding_store()
                    .update_lease_state(&binding.agent_id, "ready", None);
            report.skipped_busy.push(binding.agent_id);
            continue;
        }

        let cfg_dir = anyagents_core::agent_config_dir(data_dir, &binding.agent_id);
        match resolve_provider(&binding) {
            Ok(Some(provider)) => {
                match release_sandbox_binding_with_provider(
                    store,
                    &binding,
                    provider.as_ref(),
                    Some(&cfg_dir),
                ) {
                    Ok(true) => report.released.push(binding.agent_id),
                    Ok(false) => {}
                    Err(err) => report.failed.push((binding.agent_id, err)),
                }
            }
            Ok(None) => {
                // Legacy file-backed provider (e.g. `legacy_http`).
                match crate::sandbox_providers::release_sandbox_for_config_dir(&cfg_dir) {
                    Ok(()) => {
                        let _ = store.sandbox_binding_store().clear_lease(&binding.agent_id);
                        report.released.push(binding.agent_id);
                    }
                    Err(err) => report.failed.push((binding.agent_id, err)),
                }
            }
            Err(err) => report.failed.push((binding.agent_id, err)),
        }
    }
    Ok(report)
}

/// Run the singleton 2-tier idle-TTL sweeper using [`ProviderRegistry`].
pub(crate) fn sweep_idle_sandboxes(
    store: &Store,
    data_dir: &Path,
    now_ms: i64,
) -> Result<IdleSweepReport, Error> {
    sweep_idle_sandboxes_with_resolver(store, data_dir, now_ms, |binding| {
        if binding.provider == "legacy_http" || binding.provider == "local" {
            return Ok(None);
        }
        let reg = resolve_registry_for_binding(Some(data_dir), binding)?;
        if reg.active_kind() == ProviderKind::Local {
            return Ok(None);
        }
        Ok(Some(reg.active()))
    })
}

/// Compute lease renewal cadence from `ANYAGENTS_SANDBOX_MAX_LEASE_TTL_MS`
/// (default 15 min TTL -> renew every 5 min, clamped to `[60s, 6h]`).
fn lease_renew_interval() -> Duration {
    let max_ttl_ms = env_non_empty("ANYAGENTS_SANDBOX_MAX_LEASE_TTL_MS")
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(DEFAULT_MAX_LEASE_TTL_MS);
    let secs = (max_ttl_ms / 1_000 / 3).clamp(60, 6 * 3600);
    Duration::from_secs(secs)
}

struct CollectedBundle {
    files: Vec<(String, Vec<u8>)>,
    dirs: std::collections::BTreeSet<String>,
}

#[path = "sandbox_exec_env.rs"]
mod sandbox_exec_env;
pub(crate) use sandbox_exec_env::filter_sandbox_launch_env;

/// Collect relative `(rel_path, bytes)` bundle files + remote directories for
/// syncing into `<state_root>` inside any remote sandbox provider.
#[allow(clippy::too_many_arguments)]
fn collect_agent_bundle_entries(
    state_root: &str,
    workdir: &str,
    store: Option<&Store>,
    data_dir: Option<&Path>,
    config_dir: &Path,
    agent_id_str: &str,
    harness: &str,
    extra_args: &[String],
) -> Result<CollectedBundle, String> {
    let state_root = state_root.trim_end_matches('/');
    let mut uploads: Vec<(String, Vec<u8>)> = Vec::new();
    let mut dirs_to_create: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    dirs_to_create.insert(state_root.to_string());
    dirs_to_create.insert(workdir.to_string());

    // 1. Agent instructions (`SYSTEM_PROMPT.md`, `AGENTS.md`, `CLAUDE.md`).
    for fname in ["SYSTEM_PROMPT.md", "AGENTS.md", "CLAUDE.md"] {
        let src = config_dir.join(fname);
        if let Ok(bytes) = std::fs::read(&src) {
            uploads.push((fname.to_string(), bytes));
        }
    }

    // 2. Dereferenced skills from `<config_dir>/.agents/skills`, `<config_dir>/skills`,
    //    and canonical `<data_dir>/skills`.
    let mut skill_roots: Vec<PathBuf> = Vec::new();
    if let Some(d) = data_dir {
        skill_roots.push(d.join("skills"));
    }
    skill_roots.push(config_dir.join("skills"));
    skill_roots.push(config_dir.join(".agents/skills"));
    for root in skill_roots {
        collect_dereferenced_tree(&root, ".agents/skills", &mut uploads, &mut dirs_to_create);
    }

    // 3. Remote MCP token + harness configs targeting `<state_root>` & `mcp-proxy`.
    let mcp_url = crate::cloud_harness::resolve_public_mcp_url(config_dir)?;
    let mcp_token = crate::remote::session_token(config_dir)?;
    let token_path_in_vm = format!("{state_root}/.mcp-token");
    uploads.push((
        ".mcp-token".to_string(),
        format!("{mcp_token}\n").into_bytes(),
    ));

    let guest_bin = guest_binary_path();
    let proxy_args = vec![
        "mcp-proxy".to_string(),
        "--url".to_string(),
        mcp_url.clone(),
        "--token-file".to_string(),
        token_path_in_vm.clone(),
    ];
    let mut stdio_mcp_bytes = serde_json::to_vec_pretty(&json!({
        "mcpServers": {
            "anyagents": {
                "command": guest_bin,
                "args": proxy_args,
            }
        }
    }))
    .map_err(|e| e.to_string())?;
    stdio_mcp_bytes.push(b'\n');
    for target in [".mcp.json", ".claude.json", "mcp.json", "spiron-mcp.json"] {
        uploads.push((target.to_string(), stdio_mcp_bytes.clone()));
    }
    dirs_to_create.insert(format!("{state_root}/.gemini/config"));
    uploads.push((
        ".gemini/config/mcp_config.json".to_string(),
        stdio_mcp_bytes.clone(),
    ));
    dirs_to_create.insert(format!("{state_root}/.omp/agent"));
    uploads.push((".omp/agent/mcp.json".to_string(), stdio_mcp_bytes));

    let toml_cfg = build_sandbox_toml_config(&guest_bin, &mcp_url, &token_path_in_vm);
    uploads.push(("config.toml".to_string(), toml_cfg.as_bytes().to_vec()));

    let opencode_mcp_entry = json!({
        "type": "local",
        "command": [guest_bin, "mcp-proxy", "--url", mcp_url, "--token-file", token_path_in_vm],
        "enabled": true,
    });
    let mut opencode_obj = [
        ".config/opencode/opencode.json",
        "opencode/opencode.json",
        "opencode.json",
    ]
    .iter()
    .find_map(|rel| {
        std::fs::read_to_string(config_dir.join(rel))
            .ok()
            .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
            .filter(Value::is_object)
    })
    .unwrap_or_else(|| json!({ "$schema": "https://opencode.ai/config.json" }));
    if let Some(root) = opencode_obj.as_object_mut() {
        root.entry("$schema")
            .or_insert_with(|| json!("https://opencode.ai/config.json"));
        let mcp_map = root
            .entry("mcp")
            .or_insert_with(|| json!({}))
            .as_object_mut();
        if let Some(mcp_map) = mcp_map {
            mcp_map.insert("anyagents".to_string(), opencode_mcp_entry);
        }
    }
    let opencode_json = serde_json::to_vec_pretty(&opencode_obj).map_err(|e| e.to_string())?;
    uploads.push(("opencode.json".to_string(), opencode_json.clone()));
    dirs_to_create.insert(format!("{state_root}/.config/opencode"));
    uploads.push((
        ".config/opencode/opencode.json".to_string(),
        opencode_json.clone(),
    ));
    dirs_to_create.insert(format!("{state_root}/opencode"));
    uploads.push(("opencode/opencode.json".to_string(), opencode_json));

    // 4. Sync agent config & upstream OAuth/subscription credential files from `config_dir`
    //    while causally excluding host-only secrets (`provider.json`, `gateway-token.txt`,
    //    `mcp-capability`, `remote-session-token`, `.anyagents-mcp-url`) and local DB/socket state.
    collect_agent_config_tree(config_dir, state_root, &mut uploads, &mut dirs_to_create);

    // 5. Repo clone/checkout spec (so guest checks out git repo on first boot).
    let repo_spec = read_synced_repo_spec(config_dir);
    if let Some(ref spec) = repo_spec {
        let spec_bytes = serde_json::to_vec_pretty(spec).map_err(|e| e.to_string())?;
        uploads.push(("repo-spec.json".to_string(), spec_bytes));
    }

    // 6. Agent metadata, filtered per-agent `launch_env`, Gateway URL + per-agent gateway token
    //    (strictly excluding host coordinator secrets and protected sandbox runtime vars).
    let (agent_name, agent_model, agent_launch_env, gateway_env) =
        if let Ok(aid) = agent_id_str.parse::<AgentId>() {
            let (name, model, launch_env) = store
                .and_then(|s| s.agent(&aid).ok())
                .or_else(|| {
                    data_dir
                        .and_then(|d| Store::open(d.join("state.db")).ok())
                        .and_then(|s| s.agent(&aid).ok())
                })
                .map(|a| (Some(a.name), a.model, a.launch_env))
                .unwrap_or((None, None, Vec::new()));
            let gw_env = if let Some(d) = data_dir {
                crate::gateway::sandbox_env(d, &aid)?
            } else {
                Vec::new()
            };
            (name, model, launch_env, gw_env)
        } else {
            (None, None, Vec::new(), Vec::new())
        };
    let mut env_map: BTreeMap<String, String> = BTreeMap::new();
    for (k, v) in filter_sandbox_launch_env(&agent_launch_env) {
        env_map.insert(k, v);
    }
    for (k, v) in gateway_env {
        env_map.insert(k, v);
    }
    env_map.insert("HOME".into(), state_root.to_string());
    env_map.insert("XDG_CONFIG_HOME".into(), format!("{state_root}/.config"));
    env_map.insert("XDG_DATA_HOME".into(), format!("{state_root}/.local/share"));
    env_map.insert("XDG_CACHE_HOME".into(), format!("{state_root}/.cache"));
    let sandbox_data_dir = sandbox_exec_env::sandbox_data_dir_from_state_root(state_root);
    env_map.insert("ANYAGENTS_AGENT_DATA_DIR".into(), sandbox_data_dir.clone());
    env_map.insert("ANYAGENTS_SANDBOX_DATA_DIR".into(), sandbox_data_dir);
    env_map.insert("ANYAGENTS_GUEST_STATE_DIR".into(), state_root.to_string());
    env_map.insert("ANYAGENTS_GUEST_WORKDIR".into(), workdir.to_string());
    env_map.insert("ANYAGENTS_MCP_URL".into(), mcp_url.clone());
    env_map.insert("ANYAGENTS_MCP_TOKEN_FILE".into(), token_path_in_vm.clone());
    let env: Vec<(String, String)> = env_map.into_iter().collect();

    let session_id = read_persisted_session_id(config_dir, harness);
    let guest_cfg = json!({
        "agent_id": agent_id_str,
        "agent_name": agent_name,
        "harness": harness,
        "model": agent_model,
        "session_id": session_id,
        "workdir": workdir,
        "extra_args": extra_args,
        "env": env,
        "mcp_url": mcp_url,
        "mcp_token_file": token_path_in_vm,
        "repo_spec": repo_spec,
    });
    uploads.push((
        "guest-config.json".to_string(),
        serde_json::to_vec_pretty(&guest_cfg).map_err(|e| e.to_string())?,
    ));

    // Normalize relative directory entries collected during skill walk into absolute dirs.
    let mut abs_dirs = std::collections::BTreeSet::new();
    for dir in dirs_to_create {
        if dir.starts_with('/') {
            abs_dirs.insert(dir);
        } else {
            abs_dirs.insert(format!("{state_root}/{dir}"));
        }
    }
    Ok(CollectedBundle {
        files: uploads,
        dirs: abs_dirs,
    })
}

/// Sync skills (dereferencing symlinks), instructions, harness MCP configs targeting
/// `<state_dir>` (`/home/user/agent-data/state` or legacy `/home/user/agent-state`) +
/// `anyagents-guest mcp-proxy`, repo checkout spec, Gateway origin/token, and refreshed
/// OAuth/provider credentials into the agent's Sylphx Volume.
pub(crate) fn sync_agent_bundle_to_sylphx(
    lease: &SylphxLeaseConfig,
    data_dir: Option<&Path>,
    config_dir: &Path,
    agent_id_str: &str,
    harness: &str,
    extra_args: &[String],
) -> Result<(), String> {
    let client = lease.client();
    let resource = lease.active_data_resource()?;
    let state_root = lease.state_dir();
    let CollectedBundle {
        files: rel_uploads,
        dirs: dirs_to_create,
    } = collect_agent_bundle_entries(
        &state_root,
        &lease.workdir,
        None,
        data_dir,
        config_dir,
        agent_id_str,
        harness,
        extra_args,
    )?;

    // Incremental SHA-256 diff so unchanged skills/configs are not re-uploaded every turn.
    let manifest_path = config_dir.join(".sylphx-bundle-hashes.json");
    let mut manifest: BundleManifest = std::fs::read_to_string(&manifest_path)
        .ok()
        .and_then(|raw| serde_json::from_str(&raw).ok())
        .filter(|m: &BundleManifest| m.volume_id == lease.volume_id)
        .unwrap_or_else(|| BundleManifest {
            volume_id: lease.volume_id.clone(),
            files: HashMap::new(),
        });

    let mut changed: Vec<(String, Vec<u8>, String)> = Vec::new();
    for (rel_path, bytes) in rel_uploads {
        let remote_path = format!("{state_root}/{rel_path}");
        let hash = sha256_hex(&bytes);
        if manifest.files.get(&remote_path).map(String::as_str) != Some(hash.as_str()) {
            changed.push((remote_path, bytes, hash));
        }
    }
    if changed.is_empty() {
        return Ok(());
    }

    let mut mkdir_argv = vec!["mkdir".to_string(), "-p".to_string()];
    mkdir_argv.extend(dirs_to_create);
    let mk_out = client.exec(
        resource,
        &mkdir_argv,
        Some("/"),
        &BTreeMap::new(),
        Some(20_000),
    )?;
    if mk_out.exit_code != 0 {
        return Err(format!(
            "cannot prepare remote directories under {state_root}: {}",
            mk_out.stderr.trim()
        ));
    }

    for (remote_path, bytes, hash) in changed {
        client.write_file(resource, &remote_path, &bytes)?;
        manifest.files.insert(remote_path, hash);
    }
    let manifest_bytes = serde_json::to_vec_pretty(&manifest).map_err(|e| e.to_string())?;
    let _ = anyagents_harness::atomic_write_nonempty_file(&manifest_path, &manifest_bytes, 0o600);
    Ok(())
}

/// Sync agent bundle into any [`AsyncSandboxProvider`] in a single `.tar.gz` batch.
#[allow(clippy::too_many_arguments)]
pub(crate) fn sync_agent_bundle_tar_to_provider(
    provider: &dyn AsyncSandboxProvider,
    handle: &SandboxHandle,
    store: Option<&Store>,
    data_dir: Option<&Path>,
    config_dir: &Path,
    agent_id_str: &str,
    harness: &str,
    extra_args: &[String],
) -> Result<(), String> {
    let CollectedBundle {
        files: rel_uploads, ..
    } = collect_agent_bundle_entries(
        &handle.state_dir,
        &handle.workdir,
        store,
        data_dir,
        config_dir,
        agent_id_str,
        harness,
        extra_args,
    )?;
    let vol_key = handle
        .volume_id
        .clone()
        .unwrap_or_else(|| handle.sandbox_id.clone());
    let manifest_path = config_dir.join(".sandbox-bundle-hashes.json");
    let mut manifest: BundleManifest = std::fs::read_to_string(&manifest_path)
        .ok()
        .and_then(|raw| serde_json::from_str(&raw).ok())
        .filter(|m: &BundleManifest| m.volume_id == vol_key)
        .unwrap_or_else(|| BundleManifest {
            volume_id: vol_key,
            files: HashMap::new(),
        });

    let mut changed = Vec::new();
    let mut new_hashes = Vec::new();
    for (rel_path, bytes) in rel_uploads {
        let hash = sha256_hex(&bytes);
        if manifest.files.get(&rel_path).map(String::as_str) != Some(hash.as_str()) {
            new_hashes.push((rel_path.clone(), hash));
            changed.push((rel_path, bytes));
        }
    }
    if changed.is_empty() {
        return Ok(());
    }
    let tar_gz = build_bundle_tar_gz(&changed)?;
    block_on_provider(provider.sync_bundle_tar(handle, &handle.state_dir, &tar_gz))?;
    for (rel_path, hash) in new_hashes {
        manifest.files.insert(rel_path, hash);
    }
    let manifest_bytes = serde_json::to_vec_pretty(&manifest).map_err(|e| e.to_string())?;
    let _ = anyagents_harness::atomic_write_nonempty_file(&manifest_path, &manifest_bytes, 0o600);
    Ok(())
}

/// Drive `sandbox-exec` turns through any non-Sylphx [`AsyncSandboxProvider`]
/// (`docker`, `e2b`, `modal`, `daytona`, `cloudflare`, `http`) using single-tarball
/// bundle sync and `provider.exec`.
#[allow(clippy::too_many_arguments)]
fn run_provider_sandbox<R: BufRead + Send + 'static>(
    provider: Arc<dyn AsyncSandboxProvider>,
    handle: SandboxHandle,
    store: Option<&Store>,
    data_dir: Option<&Path>,
    config_dir: &Path,
    agent_id_str: &str,
    harness: &str,
    program_arg: Option<&str>,
    resume: Option<&str>,
    oneshot: bool,
    extra_args: &[String],
    reader: R,
    out_writer: Arc<Mutex<dyn Write + Send>>,
    heartbeat_sink: Option<(Arc<Store>, AgentId)>,
) -> Result<(), Error> {
    sync_agent_bundle_tar_to_provider(
        provider.as_ref(),
        &handle,
        store,
        data_dir,
        config_dir,
        agent_id_str,
        harness,
        extra_args,
    )
    .map_err(Error::Message)?;

    let program = program_arg
        .filter(|p| !p.trim().is_empty())
        .unwrap_or(default_remote_program(harness));

    if oneshot {
        let argv = if extra_args.is_empty() {
            vec![program.to_string()]
        } else {
            let mut v = vec![program.to_string()];
            v.extend(extra_args.iter().cloned());
            v
        };
        let req = ExecRequest::argv(argv)
            .with_cwd(&handle.workdir)
            .with_env("HOME", &handle.state_dir)
            .with_env("ANYAGENTS_GUEST_STATE_DIR", &handle.state_dir)
            .with_env("ANYAGENTS_GUEST_WORKDIR", &handle.workdir)
            .with_timeout_ms(300_000);
        let out = block_on_provider(provider.exec(&handle, &req)).map_err(Error::Message)?;
        if !out.stdout.is_empty() {
            let mut guard = out_writer.lock().unwrap_or_else(|e| e.into_inner());
            write!(guard, "{}", out.stdout)?;
            guard.flush()?;
        }
        if !out.stderr.is_empty() {
            eprint!("{}", out.stderr);
        }
        if out.exit_code != 0 {
            return Err(Error::Message(format!(
                "sandbox provider `{}` command exited with code {}",
                provider.kind(),
                out.exit_code
            )));
        }
        return Ok(());
    }

    let mut session_id = resume
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| read_persisted_session_id(config_dir, harness));

    for line in reader.lines() {
        let Ok(line) = line else { break };
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(event) = serde_json::from_str::<Value>(trimmed) else {
            continue;
        };
        if event.get("type").and_then(Value::as_str) != Some("user") {
            continue;
        }
        {
            let mut stdout = out_writer.lock().unwrap_or_else(|e| e.into_inner());
            writeln!(stdout, "{trimmed}")?;
            stdout.flush()?;
        }
        let _ = sync_agent_bundle_tar_to_provider(
            provider.as_ref(),
            &handle,
            store,
            data_dir,
            config_dir,
            agent_id_str,
            harness,
            extra_args,
        );
        let prompt_text = extract_user_prompt(&event);
        let cmd_str = build_remote_command_string(
            harness,
            program,
            session_id.as_deref(),
            &prompt_text,
            extra_args,
        );
        let req = ExecRequest::shell(cmd_str)
            .with_cwd(&handle.workdir)
            .with_env("HOME", &handle.state_dir)
            .with_env("ANYAGENTS_GUEST_STATE_DIR", &handle.state_dir)
            .with_env("ANYAGENTS_GUEST_WORKDIR", &handle.workdir)
            .with_timeout_ms(300_000);
        match block_on_provider(provider.exec(&handle, &req)) {
            Ok(out) => {
                let outcome = parse_remote_exec_output(
                    harness,
                    session_id.as_deref(),
                    &out.stdout,
                    &out.stderr,
                    out.exit_code,
                );
                session_id = Some(outcome.session_id.clone());
                persist_session_id(config_dir, harness, &outcome.session_id);
                let mut stdout = out_writer.lock().unwrap_or_else(|e| e.into_inner());
                for raw_line in &outcome.forwarded_stream_lines {
                    if let Some((ref hb_store, ref hb_agent)) = heartbeat_sink
                        && let Ok(val) = serde_json::from_str::<Value>(raw_line)
                    {
                        let _ = ingest_guest_heartbeat_value(hb_store, hb_agent, &val);
                    }
                    writeln!(stdout, "{raw_line}")?;
                }
                if outcome.forwarded_stream_lines.is_empty() {
                    let assistant_event = json!({
                        "type": "assistant",
                        "session_id": outcome.session_id,
                        "message": {
                            "role": "assistant",
                            "content": [{ "type": "text", "text": outcome.reply_text }]
                        }
                    });
                    writeln!(stdout, "{assistant_event}")?;
                }
                if !outcome.forwarded_has_result {
                    let result_event = json!({
                        "type": "result",
                        "subtype": if outcome.is_error { "error" } else { "success" },
                        "is_error": outcome.is_error,
                        "session_id": outcome.session_id,
                        "result": outcome.reply_text,
                        "usage": {
                            "input_tokens": outcome.input_tokens,
                            "output_tokens": outcome.output_tokens,
                        }
                    });
                    writeln!(stdout, "{result_event}")?;
                }
                stdout.flush()?;
            }
            Err(err_msg) => {
                let fallback_sid = session_id
                    .clone()
                    .unwrap_or_else(|| format!("{harness}-remote-session"));
                let result_event = json!({
                    "type": "result",
                    "subtype": "error",
                    "is_error": true,
                    "session_id": fallback_sid,
                    "result": err_msg,
                });
                let mut stdout = out_writer.lock().unwrap_or_else(|e| e.into_inner());
                writeln!(stdout, "{result_event}")?;
                stdout.flush()?;
            }
        }
    }
    Ok(())
}

fn build_sandbox_toml_config(guest_bin: &str, mcp_url: &str, token_path: &str) -> String {
    format!(
        "always_approve = true\n\n[mcp_servers.anyagents]\ncommand = {cmd}\nargs = [\"mcp-proxy\", \"--url\", {url}, \"--token-file\", {tok}]\n",
        cmd = serde_json::to_string(guest_bin).unwrap_or_else(|_| "\"anyagents-guest\"".into()),
        url = serde_json::to_string(mcp_url).unwrap_or_default(),
        tok = serde_json::to_string(token_path).unwrap_or_default(),
    )
}

fn read_synced_repo_spec(config_dir: &Path) -> Option<SyncedRepoSpec> {
    for rel in ["repo-spec.json", ".anyagents-managed-worktree.json"] {
        let path = config_dir.join(rel);
        let Ok(raw) = std::fs::read_to_string(&path) else {
            continue;
        };
        let Ok(val) = serde_json::from_str::<Value>(&raw) else {
            continue;
        };
        let repo = val
            .get("repo")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty());
        if let Some(repo) = repo {
            return Some(SyncedRepoSpec {
                repo: repo.to_string(),
                branch: val
                    .get("branch")
                    .and_then(Value::as_str)
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty()),
                base: val
                    .get("base")
                    .and_then(Value::as_str)
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty()),
            });
        }
    }
    None
}

const MAX_AGENT_BUNDLE_FILE_BYTES: u64 = 4 * 1024 * 1024;

fn is_excluded_agent_sync_rel(rel: &str) -> bool {
    let trimmed = rel.trim_start_matches('/');
    if trimmed.is_empty() || trimmed.contains("..") {
        return true;
    }
    let first = trimmed.split('/').next().unwrap_or("");
    if matches!(
        first,
        "provider.json"
            | "gateway-token.txt"
            | "mcp-capability"
            | "remote-session-token"
            | ".anyagents-mcp-url"
            | "sandbox.json"
            | "sandbox-sylphx.json"
            | ".sandbox-bundle-hashes.json"
            | ".sylphx-bundle-hashes.json"
            | ".last-cleanup"
            | "guest-config.json"
            | "repo-spec.json"
            | ".mcp-token"
            | "skills"
            | ".agents"
            | "memory"
            | "history"
            | "turns"
            | "work"
            | "tmp"
            | "cache"
            | "node_modules"
            | "target"
            | "data-home"
            | "state-home"
            | "memory-imports"
    ) {
        return true;
    }
    if first.starts_with(".seat-before-") {
        return true;
    }
    let lower = trimmed.to_ascii_lowercase();
    lower.ends_with(".db")
        || lower.ends_with(".db-wal")
        || lower.ends_with(".db-shm")
        || lower.ends_with(".sqlite")
        || lower.ends_with(".sqlite3")
        || lower.ends_with(".sqlite3-wal")
        || lower.ends_with(".sqlite3-shm")
        || lower.ends_with(".sock")
        || lower.ends_with(".lock")
        || lower.ends_with(".tmp")
}

/// Materialize an agent's persisted PostgreSQL `agent_files` (`config` and decrypted `secrets`)
/// into the ephemeral `config_dir` so stateless replicas can build and sync the full remote
/// sandbox bundle without local disk state.
pub(crate) fn hydrate_agent_files_from_store(
    store: &Store,
    agent_id: &AgentId,
    config_dir: &Path,
) -> Result<usize, String> {
    if !store.is_postgres() {
        return Ok(0);
    }
    let blobs = store.blob_store();
    let mut hydrated = 0usize;
    for (ns, mode) in [
        ("config", 0o644u32),
        ("markers", 0o644u32),
        ("secrets", 0o600u32),
    ] {
        let entries = blobs
            .get_agent_files_all(agent_id, ns)
            .map_err(|e| format!("batch load agent_files({ns}) failed: {e}"))?;
        for blob in entries {
            if blob.content.len() as u64 > MAX_AGENT_BUNDLE_FILE_BYTES
                || is_excluded_agent_sync_rel(&blob.path)
            {
                continue;
            }
            let dest = config_dir.join(&blob.path);
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| format!("create dir {}: {e}", parent.display()))?;
            }
            std::fs::write(&dest, &blob.content)
                .map_err(|e| format!("write hydrated {}: {e}", dest.display()))?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt as _;
                let _ = std::fs::set_permissions(&dest, std::fs::Permissions::from_mode(mode));
            }
            hydrated += 1;
        }
    }
    Ok(hydrated)
}

/// Hydrate `<config_dir>/sandbox-sylphx.json` from PostgreSQL `agent_sandbox_bindings`
/// so stateless coordinator replicas handling remote MCP `workspace.*` calls always resolve
/// the agent's active remote sandbox lease without relying on local ephemeral disk state.
///
/// Returns the active sandbox's `workdir` when a `ready` binding exists.
pub(crate) fn hydrate_sandbox_lease_file_from_store(
    store: &Store,
    agent_id: &AgentId,
    config_dir: &Path,
) -> Option<String> {
    let binding = store.sandbox_binding(agent_id).ok().flatten()?;
    if binding.lease_state != "ready" {
        return None;
    }
    let handle = sandbox_handle_from_binding(&binding)?;
    if handle.provider == ProviderKind::Sylphx
        && let Ok(reg) = resolve_registry_for_binding(Some(store.data_dir()), &binding)
        && let Ok(lease_cfg) = sylphx_lease_from_handle(
            &handle,
            &reg,
            Some(&binding),
            Some(store.data_dir()),
            config_dir,
        )
    {
        let _ = std::fs::create_dir_all(config_dir);
        let _ = lease_cfg.save_to_config_dir(config_dir);
    }
    Some(handle.workdir)
}

/// Materialize an agent's persisted PostgreSQL `agent_files(namespace = 'memory')`
/// blobs into `<data_dir>/agents/<agent_id>/memory/` so stateless replicas serve
/// `memory(list/read/write/delete)` and `conversation(topic=...)` identically across
/// scale-to-zero and replica switches.
pub(crate) fn hydrate_agent_memory_from_store(store: &Store, agent_id: &AgentId) -> usize {
    if !store.is_postgres() {
        return 0;
    }
    let blobs = store.blob_store();
    let Ok(entries) = blobs.list_agent_files(agent_id, "memory") else {
        return 0;
    };
    if entries.is_empty() {
        return 0;
    }
    let mem_dir = store
        .data_dir()
        .join("agents")
        .join(agent_id.as_str())
        .join("memory");
    let _ = std::fs::create_dir_all(&mem_dir);
    let mut needs_sync = false;
    for meta in &entries {
        if anyagents_core::stateless::validate_blob_path(&meta.path).is_err()
            || meta.size_bytes as u64 > MAX_AGENT_BUNDLE_FILE_BYTES
        {
            continue;
        }
        let dest = mem_dir.join(&meta.path);
        let local_mtime_ms = std::fs::metadata(&dest)
            .ok()
            .and_then(|m| m.modified().ok())
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_millis() as i64);
        if !local_mtime_ms.is_some_and(|local_ms| local_ms >= meta.updated_at_ms) {
            needs_sync = true;
            break;
        }
    }
    if !needs_sync {
        return 0;
    }
    let Ok(all_blobs) = blobs.get_agent_files_all(agent_id, "memory") else {
        return 0;
    };
    let mut hydrated = 0usize;
    for blob in all_blobs {
        if anyagents_core::stateless::validate_blob_path(&blob.path).is_err()
            || blob.content.len() as u64 > MAX_AGENT_BUNDLE_FILE_BYTES
        {
            continue;
        }
        let dest = mem_dir.join(&blob.path);
        let local_mtime_ms = std::fs::metadata(&dest)
            .ok()
            .and_then(|m| m.modified().ok())
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_millis() as i64);
        if local_mtime_ms.is_some_and(|local_ms| local_ms >= blob.updated_at_ms) {
            continue;
        }
        if let Some(parent) = dest.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        if std::fs::write(&dest, &blob.content).is_ok() {
            hydrated += 1;
        }
    }
    hydrated
}

/// Persist a single learned-memory file (`<slug>.md` or `MEMORY.md`) back into
/// PostgreSQL `agent_files(namespace = 'memory')` when running on PostgreSQL.
pub(crate) fn persist_agent_memory_file_to_store(
    store: &Store,
    agent_id: &AgentId,
    filename: &str,
    content: &[u8],
) {
    if !store.is_postgres() {
        return;
    }
    let _ = store
        .blob_store()
        .put_agent_file(agent_id, "memory", filename, content, false);
}

/// Remove a deleted learned-memory topic file from PostgreSQL `agent_files(namespace = 'memory')`.
pub(crate) fn delete_agent_memory_file_from_store(
    store: &Store,
    agent_id: &AgentId,
    filename: &str,
) {
    if !store.is_postgres() {
        return;
    }
    let _ = store
        .blob_store()
        .delete_agent_file(agent_id, "memory", filename);
}

fn collect_agent_config_tree(
    config_dir: &Path,
    state_root: &str,
    uploads: &mut Vec<(String, Vec<u8>)>,
    dirs: &mut std::collections::BTreeSet<String>,
) {
    let existing: std::collections::HashSet<String> =
        uploads.iter().map(|(p, _)| p.clone()).collect();
    let mut stack = vec![(config_dir.to_path_buf(), String::new(), 0usize)];
    while let Some((src, rel_prefix, depth)) = stack.pop() {
        if depth > 8 || !src.exists() {
            continue;
        }
        let Ok(entries) = std::fs::read_dir(&src) else {
            continue;
        };
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            let rel = if rel_prefix.is_empty() {
                name
            } else {
                format!("{rel_prefix}/{name}")
            };
            if is_excluded_agent_sync_rel(&rel) || existing.contains(&rel) {
                continue;
            }
            let path = entry.path();
            let Ok(meta) = std::fs::symlink_metadata(&path) else {
                continue;
            };
            if meta.file_type().is_symlink() {
                continue;
            }
            if meta.is_dir() {
                stack.push((path, rel, depth + 1));
            } else if meta.is_file()
                && meta.len() <= MAX_AGENT_BUNDLE_FILE_BYTES
                && let Ok(bytes) = std::fs::read(&path)
            {
                if let Some(parent) = Path::new(&rel).parent().and_then(|p| p.to_str())
                    && !parent.is_empty()
                {
                    dirs.insert(format!("{state_root}/{parent}"));
                }
                uploads.push((rel, bytes));
            }
        }
    }
}

fn collect_dereferenced_tree(
    src_root: &Path,
    dst_prefix: &str,
    uploads: &mut Vec<(String, Vec<u8>)>,
    dirs: &mut std::collections::BTreeSet<String>,
) {
    let mut stack = vec![(src_root.to_path_buf(), dst_prefix.to_string(), 0usize)];
    while let Some((src, dst, depth)) = stack.pop() {
        if depth > 16 || !src.exists() {
            continue;
        }
        let Ok(entries) = std::fs::read_dir(&src) else {
            continue;
        };
        dirs.insert(dst.clone());
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with('.') {
                continue;
            }
            let entry_path = entry.path();
            // Follow symlinks (`std::fs::metadata` / `canonicalize`) so skill symlinks resolve to real files.
            let Ok(meta) = std::fs::metadata(&entry_path) else {
                continue;
            };
            let target_path = format!("{dst}/{name}");
            if meta.is_dir() {
                stack.push((entry_path, target_path, depth + 1));
            } else if meta.is_file()
                && let Ok(bytes) = std::fs::read(&entry_path)
            {
                uploads.push((target_path, bytes));
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn run_sylphx_sandbox<R: BufRead + Send + 'static>(
    lease: SylphxLeaseConfig,
    data_dir: Option<&Path>,
    config_dir: &Path,
    agent_id_str: &str,
    harness: &str,
    program_arg: Option<&str>,
    resume: Option<&str>,
    oneshot: bool,
    extra_args: &[String],
    reader: R,
    out_writer: Arc<Mutex<dyn Write + Send>>,
    heartbeat_sink: Option<(Arc<Store>, AgentId)>,
) -> Result<(), Error> {
    sync_agent_bundle_to_sylphx(
        &lease,
        data_dir,
        config_dir,
        agent_id_str,
        harness,
        extra_args,
    )
    .map_err(Error::Message)?;

    if oneshot {
        let cmd_argv = if extra_args.is_empty() {
            vec![
                program_arg
                    .unwrap_or(default_remote_program(harness))
                    .to_string(),
            ]
        } else if let Some(p) = program_arg {
            let mut v = vec![p.to_string()];
            v.extend(extra_args.iter().cloned());
            v
        } else {
            extra_args.to_vec()
        };
        let out = lease
            .client()
            .exec(
                lease.active_data_resource().map_err(Error::Message)?,
                &cmd_argv,
                Some(&lease.workdir),
                &BTreeMap::new(),
                None,
            )
            .map_err(Error::Message)?;
        if !out.stdout.is_empty() {
            let mut guard = out_writer.lock().unwrap_or_else(|e| e.into_inner());
            write!(guard, "{}", out.stdout)?;
            guard.flush()?;
        }
        if !out.stderr.is_empty() {
            eprint!("{}", out.stderr);
        }
        if out.exit_code != 0 {
            return Err(Error::Message(format!(
                "Sylphx `:exec` exited with code {}",
                out.exit_code
            )));
        }
        return Ok(());
    }

    // Background lease TTL renewal thread (`:renew` while sandbox-exec is running).
    let renew_stop = Arc::new((Mutex::new(false), Condvar::new()));
    let renew_cfg = config_dir.to_path_buf();
    let renew_flag = Arc::clone(&renew_stop);
    let renew_interval = lease_renew_interval();
    let renew_thread = std::thread::Builder::new()
        .name("anyagents-sylphx-renew".into())
        .spawn(move || {
            let (lock, cv) = &*renew_flag;
            loop {
                let guard = lock.lock().unwrap_or_else(|e| e.into_inner());
                let (stop, _) = cv
                    .wait_timeout_while(guard, renew_interval, |stopped| !*stopped)
                    .unwrap_or_else(|e| e.into_inner());
                if *stop {
                    break;
                }
                drop(stop);
                let _ = renew_lease_for_config_dir(&renew_cfg);
            }
        })
        .ok();

    let res = run_sylphx_envd_bridge(
        &lease,
        data_dir,
        config_dir,
        agent_id_str,
        harness,
        program_arg,
        resume,
        extra_args,
        reader,
        out_writer,
        heartbeat_sink,
    );

    if let Ok(mut stopped) = renew_stop.0.lock() {
        *stopped = true;
        renew_stop.1.notify_all();
    }
    if let Some(h) = renew_thread {
        let _ = h.join();
    }
    res
}

#[allow(clippy::too_many_arguments)]
fn run_sylphx_envd_bridge<R: BufRead + Send + 'static>(
    lease: &SylphxLeaseConfig,
    data_dir: Option<&Path>,
    config_dir: &Path,
    agent_id_str: &str,
    harness: &str,
    program_arg: Option<&str>,
    resume: Option<&str>,
    extra_args: &[String],
    reader: R,
    out_writer: Arc<Mutex<dyn Write + Send>>,
    heartbeat_sink: Option<(Arc<Store>, AgentId)>,
) -> Result<(), Error> {
    let client = lease.client();
    let envd_url = lease.resolved_envd_url().map_err(Error::Message)?;
    let envd_token = lease.envd_token.clone();
    let guest_bin = guest_binary_path();
    let state_dir = lease.state_dir();

    let mut guest_args = vec![
        "serve".to_string(),
        "--harness".to_string(),
        harness.to_string(),
        "--state-dir".to_string(),
        state_dir.clone(),
        "--workdir".to_string(),
        lease.workdir.clone(),
    ];
    if let Some(prog) = program_arg.map(str::trim).filter(|s| !s.is_empty()) {
        // Avoid passing host absolute paths from non-container orchestrators as-is unless explicit.
        let normalized_prog =
            if prog.starts_with('/') && !prog.starts_with("/usr/") && !prog.starts_with("/home/") {
                default_remote_program(harness)
            } else {
                prog
            };
        guest_args.push("--program".to_string());
        guest_args.push(normalized_prog.to_string());
    }
    let session_id = resume
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| read_persisted_session_id(config_dir, harness));
    if let Some(sid) = session_id {
        guest_args.push("--session".to_string());
        guest_args.push(sid);
    }
    if !extra_args.is_empty() {
        guest_args.push("--".to_string());
        guest_args.extend(extra_args.iter().cloned());
    }

    let mut guest_env = BTreeMap::new();
    guest_env.insert("HOME".to_string(), state_dir.clone());
    guest_env.insert("ANYAGENTS_GUEST_STATE_DIR".to_string(), state_dir);
    guest_env.insert("ANYAGENTS_GUEST_WORKDIR".to_string(), lease.workdir.clone());

    type EnvdStreamSlot = Arc<(Mutex<(Option<u32>, Vec<Vec<u8>>, bool)>, Condvar)>;
    let pid_slot: EnvdStreamSlot =
        Arc::new((Mutex::new((None, Vec::new(), false)), Condvar::new()));
    let pid_for_stream = Arc::clone(&pid_slot);

    let cfg_clone = config_dir.to_path_buf();
    let harness_clone = harness.to_string();
    let client_for_stream = client.clone();
    let envd_url_for_stream = envd_url.clone();
    let envd_tok_for_stream = envd_token.clone();
    let workdir_for_stream = lease.workdir.clone();
    let out_for_stream = Arc::clone(&out_writer);
    let hb_for_stream = heartbeat_sink.clone();
    let stream_thread = std::thread::Builder::new()
        .name("anyagents-sylphx-envd-stdout".into())
        .spawn(move || -> Result<(), String> {
            let mut stdout_buf = Vec::new();
            let client_send = client_for_stream.clone();
            let url_send = envd_url_for_stream.clone();
            let tok_send = envd_tok_for_stream.clone();
            let res = client_for_stream.start_envd_stream(
                &envd_url_for_stream,
                envd_tok_for_stream.as_deref(),
                &guest_bin,
                &guest_args,
                Some(&workdir_for_stream),
                &guest_env,
                true,
                |ev| match ev {
                    EnvdProcessEvent::Start { pid } => {
                        let queued_inputs = {
                            let mut st = pid_for_stream.0.lock().unwrap_or_else(|e| e.into_inner());
                            st.0 = Some(pid);
                            pid_for_stream.1.notify_all();
                            std::mem::take(&mut st.1)
                        };
                        for chunk in queued_inputs {
                            let _ = client_send.send_envd_input(
                                &url_send,
                                tok_send.as_deref(),
                                pid,
                                &chunk,
                            );
                        }
                    }
                    EnvdProcessEvent::Data { stdout, stderr, .. } => {
                        if !stderr.is_empty() {
                            let s = String::from_utf8_lossy(&stderr);
                            eprint!("{s}");
                        }
                        if !stdout.is_empty() {
                            stdout_buf.extend_from_slice(&stdout);
                            while let Some(pos) = stdout_buf.iter().position(|&b| b == b'\n') {
                                let line_bytes: Vec<u8> = stdout_buf.drain(..=pos).collect();
                                if let Ok(line_str) = std::str::from_utf8(&line_bytes) {
                                    let trimmed = line_str.trim();
                                    if !trimmed.is_empty() {
                                        if let Ok(val) = serde_json::from_str::<Value>(trimmed) {
                                            if let Some(sid) = val
                                                .get("session_id")
                                                .and_then(Value::as_str)
                                                .map(str::trim)
                                                .filter(|s| !s.is_empty())
                                            {
                                                persist_session_id(&cfg_clone, &harness_clone, sid);
                                            }
                                            if let Some((ref hb_store, ref hb_agent)) =
                                                hb_for_stream
                                            {
                                                let _ = ingest_guest_heartbeat_value(
                                                    hb_store, hb_agent, &val,
                                                );
                                            }
                                        }
                                        let mut out = out_for_stream
                                            .lock()
                                            .unwrap_or_else(|e| e.into_inner());
                                        let _ = writeln!(out, "{trimmed}");
                                        let _ = out.flush();
                                    }
                                }
                            }
                        }
                    }
                    EnvdProcessEvent::End { .. } => {
                        let mut st = pid_for_stream.0.lock().unwrap_or_else(|e| e.into_inner());
                        st.2 = true;
                        pid_for_stream.1.notify_all();
                    }
                    EnvdProcessEvent::Keepalive => {}
                },
            );
            let mut st = pid_for_stream.0.lock().unwrap_or_else(|e| e.into_inner());
            st.2 = true;
            pid_for_stream.1.notify_all();
            res.map(|_| ())
        })
        .map_err(|e| Error::Message(e.to_string()))?;

    // Wait briefly for `Start { pid }` or immediate stream error so fallback works cleanly.
    let initial_ready = {
        let mut guard = pid_slot.0.lock().unwrap_or_else(|e| e.into_inner());
        let wait_deadline = std::time::Instant::now() + Duration::from_secs(10);
        while guard.0.is_none() && !guard.2 {
            let now = std::time::Instant::now();
            if now >= wait_deadline {
                break;
            }
            guard = pid_slot
                .1
                .wait_timeout(guard, wait_deadline - now)
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
        (guard.0, guard.2)
    };

    if initial_ready.0.is_none() && initial_ready.1 {
        let stream_err = stream_thread.join().ok().and_then(Result::err);
        if let Some(err_msg) = stream_err {
            eprintln!(
                "sandbox-exec: envd stream unavailable ({err_msg}), using Sylphx `:exec` turn loop"
            );
        }
        return run_sylphx_exec_fallback_loop(
            lease,
            data_dir,
            config_dir,
            agent_id_str,
            harness,
            program_arg.unwrap_or(default_remote_program(harness)),
            resume,
            extra_args,
            reader,
            out_writer,
        );
    }

    for line in reader.lines() {
        let Ok(line) = line else { break };
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Ok(event) = serde_json::from_str::<Value>(trimmed)
            && event.get("type").and_then(Value::as_str) == Some("user")
        {
            let _ = sync_agent_bundle_to_sylphx(
                lease,
                data_dir,
                config_dir,
                agent_id_str,
                harness,
                extra_args,
            );
        }
        let mut payload = trimmed.as_bytes().to_vec();
        payload.push(b'\n');

        let pid_now = {
            let mut st = pid_slot.0.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(pid) = st.0 {
                Some(pid)
            } else if !st.2 {
                st.1.push(payload.clone());
                None
            } else {
                None
            }
        };
        if let Some(pid) = pid_now {
            client
                .send_envd_input(&envd_url, envd_token.as_deref(), pid, &payload)
                .map_err(Error::Message)?;
        }
    }

    // Clean shutdown: signal guest process when coordinator closes stdin.
    let pid_opt = pid_slot.0.lock().unwrap_or_else(|e| e.into_inner()).0;
    if let Some(pid) = pid_opt {
        let _ = client.send_envd_signal(&envd_url, envd_token.as_deref(), pid, EnvdSignal::SigTerm);
    }
    let _ = stream_thread.join();
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_sylphx_exec_fallback_loop<R: BufRead + Send + 'static>(
    lease: &SylphxLeaseConfig,
    data_dir: Option<&Path>,
    config_dir: &Path,
    agent_id_str: &str,
    harness: &str,
    program: &str,
    resume: Option<&str>,
    extra_args: &[String],
    reader: R,
    out_writer: Arc<Mutex<dyn Write + Send>>,
) -> Result<(), Error> {
    let mut session_id = resume
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| read_persisted_session_id(config_dir, harness));
    let client = lease.client();
    let resource = lease.active_data_resource().map_err(Error::Message)?;
    let state_dir = lease.state_dir();

    for line in reader.lines() {
        let Ok(line) = line else { break };
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(event) = serde_json::from_str::<Value>(trimmed) else {
            continue;
        };
        if event.get("type").and_then(Value::as_str) != Some("user") {
            continue;
        }
        {
            let mut stdout = out_writer.lock().unwrap_or_else(|e| e.into_inner());
            writeln!(stdout, "{trimmed}")?;
            stdout.flush()?;
        }

        let _ = sync_agent_bundle_to_sylphx(
            lease,
            data_dir,
            config_dir,
            agent_id_str,
            harness,
            extra_args,
        );
        let prompt_text = extract_user_prompt(&event);
        let cmd_str = build_remote_command_string(
            harness,
            program,
            session_id.as_deref(),
            &prompt_text,
            extra_args,
        );
        let mut exec_env = BTreeMap::new();
        exec_env.insert("HOME".to_string(), state_dir.clone());
        exec_env.insert("ANYAGENTS_GUEST_STATE_DIR".to_string(), state_dir.clone());
        exec_env.insert("ANYAGENTS_GUEST_WORKDIR".to_string(), lease.workdir.clone());
        match client.exec(
            resource,
            &["/bin/sh".into(), "-lc".into(), cmd_str],
            Some(&lease.workdir),
            &exec_env,
            None,
        ) {
            Ok(out) => {
                let outcome = parse_remote_exec_output(
                    harness,
                    session_id.as_deref(),
                    &out.stdout,
                    &out.stderr,
                    out.exit_code,
                );
                session_id = Some(outcome.session_id.clone());
                persist_session_id(config_dir, harness, &outcome.session_id);
                let mut stdout = out_writer.lock().unwrap_or_else(|e| e.into_inner());
                for raw_line in &outcome.forwarded_stream_lines {
                    writeln!(stdout, "{raw_line}")?;
                }
                if outcome.forwarded_stream_lines.is_empty() {
                    let assistant_event = json!({
                        "type": "assistant",
                        "session_id": outcome.session_id,
                        "message": {
                            "role": "assistant",
                            "content": [{ "type": "text", "text": outcome.reply_text }]
                        }
                    });
                    writeln!(stdout, "{assistant_event}")?;
                }
                if !outcome.forwarded_has_result {
                    let result_event = json!({
                        "type": "result",
                        "subtype": if outcome.is_error { "error" } else { "success" },
                        "is_error": outcome.is_error,
                        "session_id": outcome.session_id,
                        "result": outcome.reply_text,
                        "usage": {
                            "input_tokens": outcome.input_tokens,
                            "output_tokens": outcome.output_tokens,
                        }
                    });
                    writeln!(stdout, "{result_event}")?;
                }
                stdout.flush()?;
            }
            Err(err_msg) => {
                let fallback_sid = session_id
                    .clone()
                    .unwrap_or_else(|| format!("{harness}-remote-session"));
                let result_event = json!({
                    "type": "result",
                    "subtype": "error",
                    "is_error": true,
                    "session_id": fallback_sid,
                    "result": err_msg,
                });
                let mut stdout = out_writer.lock().unwrap_or_else(|e| e.into_inner());
                writeln!(stdout, "{result_event}")?;
                stdout.flush()?;
            }
        }
    }
    Ok(())
}

fn parse_remote_exec_output(
    canonical: &str,
    session_id: Option<&str>,
    stdout_str: &str,
    stderr_str: &str,
    exit_code: i32,
) -> RemoteTurnOutcome {
    let mut resolved_sid = session_id
        .map(str::to_string)
        .unwrap_or_else(|| format!("{canonical}-remote-1"));
    let mut forwarded_stream_lines = Vec::new();
    let mut forwarded_has_result = false;
    let mut extracted_reply = String::new();
    let mut input_tokens = 0u64;
    let mut output_tokens = 0u64;

    for raw_line in stdout_str.lines() {
        let l = raw_line.trim();
        if l.is_empty() {
            continue;
        }
        if let Ok(val) = serde_json::from_str::<Value>(l)
            && let Some(t) = val.get("type").and_then(Value::as_str)
            && matches!(t, "assistant" | "result" | "system")
        {
            if let Some(sid) = val
                .get("session_id")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                resolved_sid = sid.to_string();
            }
            if t == "result" {
                forwarded_has_result = true;
                if let Some(r) = val.get("result").and_then(Value::as_str) {
                    extracted_reply = r.to_string();
                }
                input_tokens = val
                    .pointer("/usage/input_tokens")
                    .and_then(Value::as_u64)
                    .unwrap_or(input_tokens);
                output_tokens = val
                    .pointer("/usage/output_tokens")
                    .and_then(Value::as_u64)
                    .unwrap_or(output_tokens);
            }
            forwarded_stream_lines.push(l.to_string());
        }
    }

    if !forwarded_stream_lines.is_empty() {
        if extracted_reply.is_empty() {
            extracted_reply = "completed".to_string();
        }
        return RemoteTurnOutcome {
            session_id: resolved_sid,
            reply_text: extracted_reply,
            is_error: exit_code != 0,
            input_tokens,
            output_tokens,
            forwarded_stream_lines,
            forwarded_has_result,
        };
    }

    let is_error = exit_code != 0 && stdout_str.trim().is_empty();
    let reply_text = if !stdout_str.trim().is_empty() {
        stdout_str.trim().to_string()
    } else if !stderr_str.trim().is_empty() {
        stderr_str.trim().to_string()
    } else {
        "completed".to_string()
    };
    RemoteTurnOutcome {
        session_id: resolved_sid,
        reply_text,
        is_error,
        input_tokens: 0,
        output_tokens: 0,
        forwarded_stream_lines: Vec::new(),
        forwarded_has_result: false,
    }
}

fn build_remote_command_string(
    harness: &str,
    program: &str,
    session_id: Option<&str>,
    prompt_text: &str,
    extra_args: &[String],
) -> String {
    let quoted_prog = shell_quote(program);
    let quoted_prompt = shell_quote(prompt_text);
    let mut parts = vec![quoted_prog];
    for arg in extra_args {
        parts.push(shell_quote(arg));
    }
    match harness {
        "claude-code" => {
            parts.push("-p".into());
            parts.push("--output-format".into());
            parts.push("stream-json".into());
            parts.push("--verbose".into());
            parts.push("--dangerously-skip-permissions".into());
            if let Some(sid) = session_id {
                parts.push("--resume".into());
                parts.push(shell_quote(sid));
            }
            parts.push(quoted_prompt);
        }
        "codex" => {
            parts.push("exec".into());
            parts.push("--dangerously-bypass-approvals-and-sandbox".into());
            parts.push(quoted_prompt);
        }
        "spiron" => {
            parts.push("--headless".into());
            parts.push("--json".into());
            if let Some(sid) = session_id {
                parts.push("--session".into());
                parts.push(shell_quote(sid));
            }
            parts.push(quoted_prompt);
        }
        _ => {
            if let Some(sid) = session_id {
                parts.push("--resume".into());
                parts.push(shell_quote(sid));
            }
            parts.push(quoted_prompt);
        }
    }
    parts.join(" ")
}

fn shell_quote(raw: &str) -> String {
    format!("'{}'", raw.replace('\'', "'\"'\"'"))
}

fn extract_user_prompt(event: &Value) -> String {
    if let Some(items) = event.pointer("/message/content").and_then(Value::as_array) {
        let mut parts = Vec::new();
        for item in items {
            if let Some(t) = item.get("text").and_then(Value::as_str)
                && !t.is_empty()
            {
                parts.push(t);
            }
        }
        if !parts.is_empty() {
            return parts.join("\n");
        }
    }
    if let Some(s) = event.pointer("/message/content").and_then(Value::as_str) {
        return s.to_string();
    }
    if let Some(s) = event.get("text").and_then(Value::as_str) {
        return s.to_string();
    }
    String::new()
}

fn session_filenames_for(harness: &str) -> Vec<String> {
    let canonical = normalize_harness_name(harness);
    let mut files = vec![format!("{canonical}-session-id")];
    if let Some(mapped) = crate::create::harness_session_filename(canonical)
        && !files.iter().any(|f| f == mapped)
    {
        files.push(mapped.to_string());
    }
    files
}

fn read_persisted_session_id(config: &Path, harness: &str) -> Option<String> {
    for fname in session_filenames_for(harness) {
        if let Ok(raw) = std::fs::read_to_string(config.join(fname)) {
            let trimmed = raw.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

fn persist_session_id(config: &Path, harness: &str, session_id: &str) {
    let trimmed = session_id.trim();
    if trimmed.is_empty() {
        return;
    }
    for fname in session_filenames_for(harness) {
        let path = config.join(fname);
        if let Err(err) = anyagents_harness::atomic_write_nonempty_file(
            &path,
            format!("{trimmed}\n").as_bytes(),
            0o664,
        ) {
            eprintln!("cannot persist {}: {err}", path.display());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Router,
        http::StatusCode,
        routing::{post, put},
    };
    use std::sync::{Arc, Mutex};

    #[test]
    fn raw_argv_parser_extracts_sandbox_exec_flags_after_claude_flags() {
        let argv: Vec<String> = vec![
            "anyagents",
            "-p",
            "--input-format",
            "stream-json",
            "--output-format",
            "stream-json",
            "--verbose",
            "--replay-user-messages",
            "sandbox-exec",
            "--config",
            "/tmp/agent-cfg",
            "--harness",
            "claude-code",
            "--program",
            "claude",
            "--resume",
            "sess-remote-42",
        ]
        .into_iter()
        .map(String::from)
        .collect();

        let parsed = try_parse_from_raw_argv(&argv).expect("should detect sandbox-exec");
        assert_eq!(parsed.config, Some(PathBuf::from("/tmp/agent-cfg")));
        assert_eq!(parsed.harness.as_deref(), Some("claude-code"));
        assert_eq!(parsed.program.as_deref(), Some("claude"));
        assert_eq!(parsed.resume.as_deref(), Some("sess-remote-42"));
    }

    #[tokio::test]
    async fn sandbox_exec_materializes_mcp_json_and_dispatches_remote_turns() {
        #[derive(Default)]
        struct MockSandboxState {
            mcp_files: Mutex<Vec<Value>>,
            exec_payloads: Mutex<Vec<Value>>,
        }

        let state = Arc::new(MockSandboxState::default());
        let app = Router::new()
            .route(
                "/v1/sandboxes/sbx-exec-1/files",
                put({
                    let st = Arc::clone(&state);
                    move |body: axum::body::Bytes| {
                        let st = Arc::clone(&st);
                        async move {
                            if let Ok(val) = serde_json::from_slice::<Value>(&body) {
                                st.mcp_files.lock().unwrap().push(val);
                            }
                            (StatusCode::OK, axum::Json(json!({"ok": true})))
                        }
                    }
                }),
            )
            .route(
                "/v1/sandboxes/sbx-exec-1/exec",
                post({
                    let st = Arc::clone(&state);
                    move |axum::Json(payload): axum::Json<Value>| {
                        let st = Arc::clone(&st);
                        async move {
                            st.exec_payloads.lock().unwrap().push(payload.clone());
                            let prompt = payload
                                .get("prompt")
                                .and_then(Value::as_str)
                                .unwrap_or("");
                            let body = if prompt.contains("stream") {
                                json!({
                                    "exit_code": 0,
                                    "stdout": format!(
                                        "{}\n{}\n",
                                        json!({
                                            "type": "assistant",
                                            "session_id": "remote-sess-stream-9",
                                            "message": {
                                                "role": "assistant",
                                                "content": [{"type": "text", "text": "streamed remote reply"}]
                                            }
                                        }),
                                        json!({
                                            "type": "result",
                                            "subtype": "success",
                                            "is_error": false,
                                            "session_id": "remote-sess-stream-9",
                                            "result": "streamed remote reply",
                                            "usage": {"input_tokens": 12, "output_tokens": 34}
                                        })
                                    ),
                                    "stderr": ""
                                })
                            } else {
                                json!({
                                    "exit_code": 0,
                                    "session_id": "remote-sess-plain-7",
                                    "stdout": "plain remote stdout reply\n",
                                    "stderr": ""
                                })
                            };
                            (StatusCode::OK, axum::Json(body))
                        }
                    }
                }),
            );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let dir = tempfile::tempdir().unwrap();
        let cfg_dir = dir.path().join("cfg");
        std::fs::create_dir_all(&cfg_dir).unwrap();
        // The local capability is present and must never reach the sandbox.
        std::fs::write(cfg_dir.join("mcp-capability"), "cap-local-never-sent\n").unwrap();
        let remote_token = format!("aars_{}", "1".repeat(64));
        std::fs::write(
            cfg_dir.join(crate::remote::TOKEN_FILE),
            format!("{remote_token}\n"),
        )
        .unwrap();
        std::fs::write(
            cfg_dir.join(".anyagents-mcp-url"),
            "https://mcp.example.test/mcp\n",
        )
        .unwrap();
        let sbx = SandboxConfig {
            endpoint: format!("http://{addr}"),
            sandbox_id: "sbx-exec-1".into(),
            token: Some("sbx-secret".into()),
            workdir: "/workspace".into(),
            remote_spawn: true,
        };
        sbx.write_to_config_dir(&cfg_dir).unwrap();

        let sbx_clone = sbx.clone();
        let cfg_clone = cfg_dir.clone();
        tokio::task::spawn_blocking(move || {
            let mut written = None;
            refresh_remote_mcp_config(&sbx_clone, Some(&cfg_clone), &mut written);
            // An unchanged token is not written again.
            refresh_remote_mcp_config(&sbx_clone, Some(&cfg_clone), &mut written);
        })
        .await
        .unwrap();

        let mcp_files = state.mcp_files.lock().unwrap().clone();
        assert_eq!(mcp_files.len(), 1);
        assert_eq!(
            mcp_files[0]
                .pointer("/mcpServers/anyagents/url")
                .and_then(Value::as_str),
            Some("https://mcp.example.test/mcp")
        );
        assert_eq!(
            mcp_files[0]
                .pointer("/mcpServers/anyagents/headers/Authorization")
                .and_then(Value::as_str),
            Some(format!("Bearer {remote_token}").as_str())
        );
        assert!(!mcp_files[0].to_string().contains("cap-local-never-sent"));

        // After the server rotates the token, the next turn rewrites the file.
        let rotated = format!("aars_{}", "2".repeat(64));
        std::fs::write(
            cfg_dir.join(crate::remote::TOKEN_FILE),
            format!("{rotated}\n"),
        )
        .unwrap();
        let sbx_clone = sbx.clone();
        let cfg_clone = cfg_dir.clone();
        let first = remote_token.clone();
        tokio::task::spawn_blocking(move || {
            let mut written = Some(first);
            refresh_remote_mcp_config(&sbx_clone, Some(&cfg_clone), &mut written);
        })
        .await
        .unwrap();
        let mcp_files = state.mcp_files.lock().unwrap().clone();
        assert_eq!(mcp_files.len(), 2);
        assert_eq!(
            mcp_files[1]
                .pointer("/mcpServers/anyagents/headers/Authorization")
                .and_then(Value::as_str),
            Some(format!("Bearer {rotated}").as_str())
        );

        // Plain stdout wrapping
        let sbx_clone = sbx.clone();
        let plain_out = tokio::task::spawn_blocking(move || {
            dispatch_remote_sandbox_turn(
                &sbx_clone,
                "claude-code",
                "claude",
                None,
                "run plain check",
                &[],
            )
            .unwrap()
        })
        .await
        .unwrap();
        assert_eq!(plain_out.session_id, "remote-sess-plain-7");
        assert_eq!(plain_out.reply_text, "plain remote stdout reply");
        assert!(plain_out.forwarded_stream_lines.is_empty());

        // Native stream-json stdout forwarding
        let sbx_clone = sbx.clone();
        let stream_out = tokio::task::spawn_blocking(move || {
            dispatch_remote_sandbox_turn(
                &sbx_clone,
                "claude-code",
                "claude",
                Some("remote-sess-plain-7"),
                "run stream check",
                &[],
            )
            .unwrap()
        })
        .await
        .unwrap();
        assert_eq!(stream_out.session_id, "remote-sess-stream-9");
        assert_eq!(stream_out.reply_text, "streamed remote reply");
        assert_eq!(stream_out.forwarded_stream_lines.len(), 2);
        assert!(stream_out.forwarded_has_result);
        assert_eq!(stream_out.input_tokens, 12);
        assert_eq!(stream_out.output_tokens, 34);

        server_handle.abort();
    }

    #[tokio::test]
    async fn create_agent_auto_provisions_remote_sandbox_from_provider_config() {
        let recorded = Arc::new(Mutex::new(Vec::<Value>::new()));
        let app = Router::new().route(
            "/v1/sandboxes",
            post({
                let rec = Arc::clone(&recorded);
                move |axum::Json(payload): axum::Json<Value>| {
                    let rec = Arc::clone(&rec);
                    async move {
                        rec.lock().unwrap().push(payload);
                        (
                            StatusCode::CREATED,
                            axum::Json(json!({
                                "sandbox_id": "sbx-auto-created-99",
                                "workdir": "/workspace",
                                "remote_spawn": true
                            })),
                        )
                    }
                }
            }),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::write(
            data_dir.join("sandbox-provider.json"),
            serde_json::to_vec(&json!({
                "endpoint": format!("http://{addr}"),
                "token": "prov-auto-token",
                "default_workdir": "/workspace",
                "remote_spawn": true
            }))
            .unwrap(),
        )
        .unwrap();

        let data_clone = data_dir.clone();
        let created = tokio::task::spawn_blocking(move || {
            let store = anyagents_core::Store::open(data_clone.join("state.db")).unwrap();
            store
                .hold_capability_key("0123456789abcdef0123456789abcdef")
                .unwrap();
            crate::create::create(
                &store,
                &data_clone,
                crate::create::CreateRequest {
                    name: "remote-worker".into(),
                    role: None,
                    harness: "claude-code".into(),
                    model: None,
                    profile: None,
                    repo: None,
                    branch: None,
                    base: None,
                    workdir: None,
                    workdir_default: None,
                    description: None,
                    instructions: None,
                    parent_id: None,
                },
            )
            .unwrap()
        })
        .await
        .unwrap();

        let cfg_dir = anyagents_core::agent_config_dir(&data_dir, &created.id);
        let sbx = crate::sandbox_providers::sandbox_config_from_config_dir(&cfg_dir)
            .expect("create_agent should write sandbox.json to agent config_dir");
        assert_eq!(sbx.sandbox_id, "sbx-auto-created-99");
        assert!(sbx.remote_spawn);
        assert_eq!(sbx.workdir, "/workspace");

        let reqs = recorded.lock().unwrap().clone();
        assert_eq!(reqs.len(), 1);
        assert_eq!(reqs[0]["name"], "remote-worker");
        assert_eq!(reqs[0]["agent_id"], created.id.as_str());

        server_handle.abort();
    }

    #[tokio::test]
    async fn sylphx_bundle_sync_dereferences_skills_redacts_secrets_and_targets_agent_state() {
        let written: Arc<Mutex<HashMap<String, Vec<u8>>>> = Arc::new(Mutex::new(HashMap::new()));
        let written_ref = Arc::clone(&written);
        let exec_cmds: Arc<Mutex<Vec<Vec<String>>>> = Arc::new(Mutex::new(Vec::new()));
        let exec_ref = Arc::clone(&exec_cmds);

        let app = Router::new()
            .route(
                "/v1/orgs/o1/projects/p1/envs/e1/leases/lease_sync:exec",
                post({
                    let exec_ref = Arc::clone(&exec_ref);
                    move |axum::Json(payload): axum::Json<Value>| {
                        let exec_ref = Arc::clone(&exec_ref);
                        async move {
                            let cmd: Vec<String> = payload["command"]
                                .as_array()
                                .unwrap()
                                .iter()
                                .filter_map(Value::as_str)
                                .map(str::to_string)
                                .collect();
                            exec_ref.lock().unwrap().push(cmd);
                            (
                                StatusCode::OK,
                                axum::Json(json!({ "exit_code": 0, "stdout": "", "stderr": "" })),
                            )
                        }
                    }
                }),
            )
            .route(
                "/v1/orgs/o1/projects/p1/envs/e1/leases/lease_sync:writeFile",
                post({
                    let written_ref = Arc::clone(&written_ref);
                    move |axum::Json(payload): axum::Json<Value>| {
                        let written_ref = Arc::clone(&written_ref);
                        async move {
                            let path = payload["path"].as_str().unwrap().to_string();
                            let bytes =
                                agent_tools::base64_decode(payload["content"].as_str().unwrap())
                                    .unwrap();
                            written_ref.lock().unwrap().insert(path, bytes);
                            (StatusCode::OK, axum::Json(json!({})))
                        }
                    }
                }),
            );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let srv = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let dir = tempfile::tempdir().unwrap();
        let data = dir.path();
        let store = Store::open(data.join("state.db")).unwrap();
        let agent = store
            .create_agent(anyagents_core::NewAgent::new("sbx-sync", "codex"))
            .unwrap();
        let cfg_dir = anyagents_core::agent_config_dir(data, &agent.id);
        std::fs::create_dir_all(&cfg_dir).unwrap();

        // Store raw provider secret -> gateway must emit proxy URL/token, NEVER raw key.
        crate::gateway::ProviderImage::store(
            data,
            &agent,
            "https://provider.example.test/v1",
            "gpt-5",
            "raw-upstream-secret-never-sent",
        )
        .unwrap();
        std::fs::write(cfg_dir.join("mcp-capability"), "local-cap-never-sent\n").unwrap();
        let remote_tok = format!("aars_{}", "9".repeat(64));
        std::fs::write(
            cfg_dir.join(crate::remote::TOKEN_FILE),
            format!("{remote_tok}\n"),
        )
        .unwrap();
        std::fs::write(
            cfg_dir.join(".anyagents-mcp-url"),
            "https://coordinator.example.test/v1/webhooks/mcp\n",
        )
        .unwrap();
        std::fs::write(cfg_dir.join("SYSTEM_PROMPT.md"), "You are sandboxed.").unwrap();
        std::fs::create_dir_all(cfg_dir.join(".config/opencode")).unwrap();
        std::fs::write(
            cfg_dir.join(".config/opencode/opencode.json"),
            r#"{"provider":{"custom":{"options":{"baseURL":"https://llm.example.test/v1"}}}}"#,
        )
        .unwrap();
        std::fs::write(
            cfg_dir.join(".config/opencode/custom.key"),
            "key-file-val\n",
        )
        .unwrap();
        std::fs::write(
            cfg_dir.join("repo-spec.json"),
            r#"{"repo":"https://github.com/example/repo.git","branch":"main"}"#,
        )
        .unwrap();

        // Real symlinked skill file must be dereferenced and uploaded by bytes.
        let real_skill = data.join("canonical-skill");
        std::fs::create_dir_all(&real_skill).unwrap();
        std::fs::write(real_skill.join("SKILL.md"), "# Real Skill\nDetails\n").unwrap();
        let skill_mount = cfg_dir.join(".agents/skills");
        std::fs::create_dir_all(&skill_mount).unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(&real_skill, skill_mount.join("my-skill")).unwrap();

        let lease = SylphxLeaseConfig {
            endpoint: format!("http://{addr}"),
            token: Some("sbx-tok".into()),
            parent: "orgs/o1/projects/p1/envs/e1".into(),
            template: "agent".into(),
            shape: "linux-m".into(),
            volume_size_gib: Some(20),
            workdir: "/home/user/workspace".into(),
            mount_path: "/home/user/agent-state".into(),
            volume_id: "vol_sync".into(),
            volume_resource: "orgs/o1/projects/p1/envs/e1/volumes/vol_sync".into(),
            lease_id: Some("lease_sync".into()),
            lease_resource: Some("orgs/o1/projects/p1/envs/e1/leases/lease_sync".into()),
            data_plane_resource: Some("orgs/o1/projects/p1/envs/e1/leases/lease_sync".into()),
            envd_url: None,
            envd_token: None,
            expires_at: None,
        };
        lease.save_to_config_dir(&cfg_dir).unwrap();

        let aid_str = agent.id.as_str().to_string();
        let data_clone = data.to_path_buf();
        let cfg_clone = cfg_dir.clone();
        let lease_clone = lease.clone();
        tokio::task::spawn_blocking(move || {
            let _env = crate::server::tests::env_lock();
            unsafe {
                std::env::set_var(
                    crate::gateway::ENV_PUBLIC_GATEWAY_URL,
                    "https://gw.example.test",
                );
            }
            let r1 = sync_agent_bundle_to_sylphx(
                &lease_clone,
                Some(&data_clone),
                &cfg_clone,
                &aid_str,
                "codex",
                &[],
            );
            // Second immediate sync must be an incremental no-op!
            let r2 = r1.and_then(|_| {
                sync_agent_bundle_to_sylphx(
                    &lease_clone,
                    Some(&data_clone),
                    &cfg_clone,
                    &aid_str,
                    "codex",
                    &[],
                )
            });
            unsafe {
                std::env::remove_var(crate::gateway::ENV_PUBLIC_GATEWAY_URL);
            }
            r2.unwrap();
        })
        .await
        .unwrap();

        let files = written.lock().unwrap();
        #[cfg(unix)]
        assert_eq!(
            files
                .get("/home/user/agent-state/.agents/skills/my-skill/SKILL.md")
                .map(|b| String::from_utf8_lossy(b).into_owned())
                .as_deref(),
            Some("# Real Skill\nDetails\n")
        );
        let guest_cfg_raw =
            String::from_utf8(files["/home/user/agent-state/guest-config.json"].clone()).unwrap();
        assert!(guest_cfg_raw.contains("https://gw.example.test"));
        assert!(!guest_cfg_raw.contains("raw-upstream-secret-never-sent"));
        assert!(!guest_cfg_raw.contains("local-cap-never-sent"));

        let mcp_json_raw =
            String::from_utf8(files["/home/user/agent-state/.mcp.json"].clone()).unwrap();
        assert!(mcp_json_raw.contains("mcp-proxy"));
        assert!(!mcp_json_raw.contains("profile="));

        let spiron_mcp_raw =
            String::from_utf8(files["/home/user/agent-state/spiron-mcp.json"].clone()).unwrap();
        assert_eq!(spiron_mcp_raw, mcp_json_raw);

        assert!(files.contains_key("/home/user/agent-state/repo-spec.json"));
        // Custom provider config + key files sync and merge `mcp.anyagents` across all opencode paths.
        assert_eq!(
            files
                .get("/home/user/agent-state/.config/opencode/custom.key")
                .map(|b| String::from_utf8_lossy(b).into_owned())
                .as_deref(),
            Some("key-file-val\n")
        );
        for oc_path in [
            "/home/user/agent-state/opencode.json",
            "/home/user/agent-state/.config/opencode/opencode.json",
            "/home/user/agent-state/opencode/opencode.json",
        ] {
            let oc_val: Value = serde_json::from_slice(&files[oc_path]).unwrap();
            assert_eq!(
                oc_val["provider"]["custom"]["options"]["baseURL"],
                "https://llm.example.test/v1"
            );
            assert_eq!(oc_val["mcp"]["anyagents"]["enabled"], true);
        }
        // Host-only secrets must never be uploaded verbatim.
        assert!(!files.contains_key("/home/user/agent-state/provider.json"));
        assert!(!files.contains_key("/home/user/agent-state/gateway-token.txt"));
        assert!(!files.contains_key("/home/user/agent-state/mcp-capability"));
        assert!(!files.contains_key("/home/user/agent-state/remote-session-token"));
        assert!(!files.contains_key("/home/user/agent-state/.anyagents-mcp-url"));
        // Exactly one mkdir exec batch due to SHA-256 manifest caching on 2nd sync.
        assert_eq!(exec_cmds.lock().unwrap().len(), 1);
        assert!(lease.workdir == "/home/user/workspace");
        assert!(!crate::sandbox_providers::sylphx::SYLPHX_STATE_FILE.is_empty());

        srv.abort();
    }

    fn open_test_store(dir: &Path, label: &str) -> (Arc<Store>, bool) {
        if let Some(url) = std::env::var("ANYAGENTS_TEST_POSTGRES_URL")
            .or_else(|_| std::env::var("TEST_DATABASE_URL"))
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
        {
            let tag = format!(
                "{label}-{}-{}",
                std::process::id(),
                anyagents_core::now_ms()
            );
            let store = Store::open_postgres_isolated(&url, dir, tag)
                .expect("open isolated PostgreSQL test store");
            return (Arc::new(store), true);
        }
        let store = Store::open(dir.join("state.db")).expect("open SQLite test store");
        (Arc::new(store), false)
    }

    struct EnvVarGuard {
        saved: Vec<(&'static str, Option<std::ffi::OsString>)>,
    }

    impl EnvVarGuard {
        fn set(pairs: &[(&'static str, Option<&str>)]) -> Self {
            let mut saved = Vec::with_capacity(pairs.len());
            for &(k, v) in pairs {
                saved.push((k, std::env::var_os(k)));
                match v {
                    Some(val) => unsafe { std::env::set_var(k, val) },
                    None => unsafe { std::env::remove_var(k) },
                }
            }
            Self { saved }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            for (k, prev) in self.saved.drain(..).rev() {
                match prev {
                    Some(val) => unsafe { std::env::set_var(k, val) },
                    None => unsafe { std::env::remove_var(k) },
                }
            }
        }
    }

    #[tokio::test]
    async fn stateless_provider_registry_byo_image_and_template_persists_to_binding_store() {
        let ensure_reqs: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
        let ensure_ref = Arc::clone(&ensure_reqs);

        let app = Router::new().route(
            "/v1/sandboxes/ensure",
            post(move |axum::Json(payload): axum::Json<Value>| {
                let ensure_ref = Arc::clone(&ensure_ref);
                async move {
                    ensure_ref.lock().unwrap().push(payload.clone());
                    (
                        StatusCode::OK,
                        axum::Json(json!({
                            "sandbox_id": "sbx-byo-lease-1",
                            "volume_id": "vol-byo-persist-1",
                            "mount_path": "/home/user/agent-data",
                            "state_dir": "/home/user/agent-data/state",
                            "workdir": "/home/user/agent-data/workspace",
                            "transport_url": "https://sandbox.example.test/sbx-byo-lease-1/envd",
                            "metadata": {
                                "volume_resource": "volumes/vol-byo-persist-1",
                                "lease_resource": "leases/sbx-byo-lease-1"
                            }
                        })),
                    )
                }
            }),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let srv = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().to_path_buf();
        let endpoint = format!("http://{addr}");
        let byo_image = "ghcr.io/example/anyagents-sandbox@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

        let (binding, off_host, is_pg) = tokio::task::spawn_blocking(move || {
            let _env_lock = crate::server::tests::env_lock();
            let _env_guard = EnvVarGuard::set(&[
                ("ANYAGENTS_SANDBOX_PROVIDER", Some("http")),
                ("ANYAGENTS_SANDBOX_HTTP_URL", Some(endpoint.as_str())),
                ("ANYAGENTS_SANDBOX_IMAGE", Some(byo_image)),
                ("ANYAGENTS_SANDBOX_TEMPLATE", Some("dev")),
                ("ANYAGENTS_SANDBOX_IDLE_TTL_MS", Some("120000")),
                ("ANYAGENTS_SANDBOX_MAX_LEASE_TTL_MS", Some("900000")),
            ]);
            let (store, is_pg) = open_test_store(&data_dir, "byo");
            let tag = format!("byo_agent_{}", anyagents_core::now_ms());
            let agent = store
                .create_agent(anyagents_core::NewAgent::new(&tag, "claude-code"))
                .unwrap();
            let cfg_dir = anyagents_core::agent_config_dir(&data_dir, &agent.id);
            let binding = ensure_sandbox_for_agent(
                &store,
                &agent,
                &cfg_dir,
                Some("/home/user/agent-data/workspace"),
                &BTreeMap::new(),
            )
            .expect("ensure_sandbox_for_agent")
            .expect("remote provider binding");
            let reloaded = store
                .sandbox_binding(&agent.id)
                .unwrap()
                .expect("persisted SandboxBinding");
            let off_host = crate::remote::runs_off_host_in(&store, &agent, &cfg_dir);
            assert_eq!(binding.lease_id, reloaded.lease_id);
            store.delete_agent(&agent.id).unwrap();
            (reloaded, off_host, is_pg)
        })
        .await
        .unwrap();

        let _ = is_pg;
        assert!(
            off_host,
            "ProviderRegistry agent must be recognized as off-host"
        );
        assert_eq!(binding.provider, "http");
        assert_eq!(binding.image.as_deref(), Some(byo_image));
        assert_eq!(binding.template.as_deref(), Some("dev"));
        assert_eq!(binding.idle_ttl_ms, 120_000);
        assert_eq!(binding.volume_id, "vol-byo-persist-1");
        assert_eq!(binding.lease_id.as_deref(), Some("sbx-byo-lease-1"));
        assert_eq!(binding.lease_state, "ready");
        assert_eq!(binding.remote_workdir, "/home/user/agent-data/workspace");

        let sent = ensure_reqs.lock().unwrap();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0]["image"].as_str(), Some(byo_image));
        assert_eq!(sent[0]["template"].as_str(), Some("dev"));
        assert_eq!(sent[0]["lease"]["idle_ttl_ms"].as_u64(), Some(120_000));
        assert_eq!(sent[0]["lease"]["max_lease_ttl_ms"].as_u64(), Some(900_000));
        assert_eq!(
            sent[0]["volume"]["mount_path"].as_str(),
            Some("/home/user/agent-data")
        );

        srv.abort();
    }

    #[tokio::test]
    async fn stateless_guest_heartbeat_updates_inflight_children_and_blocks_idle_release() {
        let release_calls: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let destroy_calls: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let rel_ref = Arc::clone(&release_calls);
        let del_ref = Arc::clone(&destroy_calls);

        let app = Router::new()
            .route(
                "/v1/sandboxes/{id}/release",
                post({
                    let rel_ref = Arc::clone(&rel_ref);
                    move |axum::extract::Path(id): axum::extract::Path<String>| {
                        let rel_ref = Arc::clone(&rel_ref);
                        async move {
                            rel_ref.lock().unwrap().push(id);
                            (StatusCode::OK, axum::Json(json!({ "ok": true })))
                        }
                    }
                }),
            )
            .route(
                "/v1/sandboxes/{id}",
                axum::routing::delete({
                    let del_ref = Arc::clone(&del_ref);
                    move |axum::extract::Path(id): axum::extract::Path<String>| {
                        let del_ref = Arc::clone(&del_ref);
                        async move {
                            del_ref.lock().unwrap().push(id);
                            (StatusCode::OK, axum::Json(json!({ "ok": true })))
                        }
                    }
                }),
            );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let srv = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().to_path_buf();
        let endpoint = format!("http://{addr}");

        tokio::task::spawn_blocking(move || {
            let _env_lock = crate::server::tests::env_lock();
            let (store, is_pg) = open_test_store(&data_dir, "hb-busy");
            let tag = format!("hb_busy_{}", anyagents_core::now_ms());
            let agent = store
                .create_agent(anyagents_core::NewAgent::new(&tag, "claude-code"))
                .unwrap();

            let t0: i64 = 1_763_506_800_000;
            let mut binding = SandboxBinding::new(agent.id.clone(), "http");
            binding.volume_id = "vol-keep-data-busy".to_string();
            binding.volume_resource = "volumes/vol-keep-data-busy".to_string();
            binding.lease_id = Some("sbx-busy-1".to_string());
            binding.lease_state = "ready".to_string();
            binding.idle_ttl_ms = 60_000;
            binding.idle_since_ms = Some(t0);
            binding.spec_json = json!({
                "endpoint": endpoint,
                "mount_path": "/home/user/agent-data",
                "state_dir": "/home/user/agent-data/state",
            })
            .to_string();
            store.sandbox_binding_store().upsert(&binding).unwrap();

            // Drive an InProcessRemoteHarness emitting a real `anyagents-guest` NDJSON
            // heartbeat reporting 2 running background child subprocesses (`inflight_children: 2`).
            let t1 = t0 + 10_000;
            let mut harness =
                crate::vendor::InProcessRemoteHarness::start_with_worker_and_heartbeat(
                    false,
                    "hb-test",
                    Some((Arc::clone(&store), agent.id.clone())),
                    move |_reader, mut writer| {
                        let frame = json!({
                            "type": "system",
                            "subtype": "heartbeat",
                            "pid": 4242,
                            "timestamp_ms": t1,
                            "harness": "claude-code",
                            "session_id": "sess-hb-1",
                            "turn_state": "idle",
                            "active_turns": 0,
                            "queued_turns": 0,
                            "inflight_children": 2,
                            "descendant_processes": 3,
                            "busy": true
                        });
                        writeln!(writer, "{frame}")?;
                        writer.flush()?;
                        Ok(())
                    },
                )
                .expect("start InProcessRemoteHarness with heartbeat sink");

            // Wait for reader thread to ingest the frame and exit cleanly.
            let deadline = std::time::Instant::now() + Duration::from_secs(3);
            while anyagents_harness::Harness::health(&harness).process_alive
                && std::time::Instant::now() < deadline
            {
                std::thread::sleep(Duration::from_millis(10));
            }
            anyagents_harness::Harness::stop(&mut harness);

            if is_pg {
                let hb = store
                    .runtime_heartbeat(&agent.id)
                    .unwrap()
                    .expect("agent_runtime heartbeat row in PG");
                assert_eq!(hb.heartbeat_at_ms, Some(t1));
                assert_eq!(hb.inflight_children, 2);
                assert_eq!(hb.idle_since_ms, None);
            }
            let after_hb = store.sandbox_binding(&agent.id).unwrap().unwrap();
            assert_eq!(after_hb.last_heartbeat_ms, Some(t1));
            assert_eq!(
                after_hb.idle_since_ms, None,
                "busy heartbeat with inflight_children > 0 must clear idle_since_ms"
            );

            // Simulate a race where `agent_sandbox_bindings.idle_since_ms` was backdated
            // past `idle_ttl_ms` while `agent_runtime.inflight_children == 2` in PG.
            if is_pg {
                store
                    .sandbox_binding_store()
                    .update_lease_state(&agent.id, "ready", Some(t0))
                    .unwrap();
                let provider: Arc<dyn AsyncSandboxProvider> = Arc::new(
                    crate::sandbox_providers::providers::GenericHttpOciProvider::new(
                        crate::sandbox_providers::providers::GenericHttpOciConfig::new(endpoint),
                    )
                    .unwrap(),
                );
                let report =
                    sweep_idle_sandboxes_with_resolver(&store, &data_dir, t0 + 600_000, |_| {
                        Ok(Some(Arc::clone(&provider)))
                    })
                    .unwrap();
                assert!(
                    report.released.is_empty(),
                    "sweeper MUST NOT release sandbox while inflight_children > 0: {report:?}"
                );
                assert!(
                    report.skipped_busy.contains(&agent.id),
                    "agent with inflight_children > 0 must be recorded in skipped_busy: {report:?}"
                );
                let still_ready = store.sandbox_binding(&agent.id).unwrap().unwrap();
                assert_eq!(still_ready.lease_state, "ready");
                assert_eq!(still_ready.lease_id.as_deref(), Some("sbx-busy-1"));
                assert_eq!(still_ready.idle_since_ms, None);
            }

            store.delete_agent(&agent.id).unwrap();
        })
        .await
        .unwrap();

        assert!(
            release_calls.lock().unwrap().is_empty(),
            "provider.release MUST NOT be called while inflight_children > 0"
        );
        assert!(
            destroy_calls.lock().unwrap().is_empty(),
            "provider.destroy MUST NEVER be called by idle sweeper"
        );
        srv.abort();
    }

    #[tokio::test]
    async fn stateless_idle_sweeper_releases_idle_sandbox_and_preserves_volume_when_children_exit()
    {
        let release_calls: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let destroy_calls: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let rel_ref = Arc::clone(&release_calls);
        let del_ref = Arc::clone(&destroy_calls);

        let app = Router::new()
            .route(
                "/v1/sandboxes/{id}/release",
                post({
                    let rel_ref = Arc::clone(&rel_ref);
                    move |axum::extract::Path(id): axum::extract::Path<String>| {
                        let rel_ref = Arc::clone(&rel_ref);
                        async move {
                            rel_ref.lock().unwrap().push(id);
                            (StatusCode::OK, axum::Json(json!({ "ok": true })))
                        }
                    }
                }),
            )
            .route(
                "/v1/sandboxes/{id}",
                axum::routing::delete({
                    let del_ref = Arc::clone(&del_ref);
                    move |axum::extract::Path(id): axum::extract::Path<String>| {
                        let del_ref = Arc::clone(&del_ref);
                        async move {
                            del_ref.lock().unwrap().push(id);
                            (StatusCode::OK, axum::Json(json!({ "ok": true })))
                        }
                    }
                }),
            );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let srv = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().to_path_buf();
        let endpoint = format!("http://{addr}");
        let byo_image = "ghcr.io/example/anyagents-sandbox@sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";

        tokio::task::spawn_blocking(move || {
            let _env_lock = crate::server::tests::env_lock();
            let (store, is_pg) = open_test_store(&data_dir, "idle-sweep");
            let tag = format!("idle_sweep_{}", anyagents_core::now_ms());
            let agent = store
                .create_agent(anyagents_core::NewAgent::new(&tag, "claude-code"))
                .unwrap();

            let mut binding = SandboxBinding::new(agent.id.clone(), "http");
            binding.image = Some(byo_image.to_string());
            binding.template = Some("dev".to_string());
            binding.volume_id = "vol-preserve-idle-1".to_string();
            binding.volume_resource = "volumes/vol-preserve-idle-1".to_string();
            binding.lease_id = Some("sbx-idle-1".to_string());
            binding.lease_state = "ready".to_string();
            binding.idle_ttl_ms = 60_000;
            binding.spec_json = json!({
                "endpoint": endpoint,
                "mount_path": "/home/user/agent-data",
                "state_dir": "/home/user/agent-data/state",
            })
            .to_string();
            store.sandbox_binding_store().upsert(&binding).unwrap();

            let provider: Arc<dyn AsyncSandboxProvider> = Arc::new(
                crate::sandbox_providers::providers::GenericHttpOciProvider::new(
                    crate::sandbox_providers::providers::GenericHttpOciConfig::new(endpoint),
                )
                .unwrap(),
            );

            // 1. Guest reports idle (`inflight_children: 0`, `active_turns: 0`, `busy: false`) at T2.
            let t2: i64 = 1_763_507_000_000;
            let idle_frame_1 = json!({
                "type": "system",
                "subtype": "heartbeat",
                "timestamp_ms": t2,
                "turn_state": "idle",
                "active_turns": 0,
                "queued_turns": 0,
                "inflight_children": 0,
                "descendant_processes": 1,
                "busy": false
            });
            assert!(ingest_guest_heartbeat_value(
                &store,
                &agent.id,
                &idle_frame_1
            ));

            // 2. Subsequent idle heartbeat 5s later must keep `idle_since_ms` anchored at T2.
            let idle_frame_2 = json!({
                "type": "system",
                "subtype": "heartbeat",
                "timestamp_ms": t2 + 5_000,
                "turn_state": "idle",
                "active_turns": 0,
                "queued_turns": 0,
                "inflight_children": 0,
                "descendant_processes": 1,
                "busy": false
            });
            assert!(ingest_guest_heartbeat_value(
                &store,
                &agent.id,
                &idle_frame_2
            ));

            if is_pg {
                let hb = store.runtime_heartbeat(&agent.id).unwrap().unwrap();
                assert_eq!(hb.heartbeat_at_ms, Some(t2 + 5_000));
                assert_eq!(hb.inflight_children, 0);
                assert_eq!(hb.idle_since_ms, Some(t2));
            }
            let b_idle = store.sandbox_binding(&agent.id).unwrap().unwrap();
            assert_eq!(b_idle.idle_since_ms, Some(t2));

            // 3. Within warm-hold grace period (T2 + 30s < T2 + 60s), sweeper keeps compute warm.
            let warm_report =
                sweep_idle_sandboxes_with_resolver(&store, &data_dir, t2 + 30_000, |_| {
                    Ok(Some(Arc::clone(&provider)))
                })
                .unwrap();
            assert!(warm_report.released.is_empty());

            // 4. An active turn blocks idle release even after `idle_ttl_ms` expires.
            let rt_start = store
                .transition_runtime(
                    &agent.id,
                    RuntimeState::Released,
                    RuntimeState::Starting,
                    anyagents_core::RuntimeChange::default(),
                )
                .unwrap();
            let rt_live = store
                .transition_runtime(
                    &agent.id,
                    RuntimeState::Starting,
                    RuntimeState::Live,
                    anyagents_core::RuntimeChange::default(),
                )
                .unwrap();
            assert_eq!(rt_start.generation, rt_live.generation);
            let open_turn = store.begin_turn(&agent.id, rt_live.generation, 1).unwrap();
            let active_turn_report =
                sweep_idle_sandboxes_with_resolver(&store, &data_dir, t2 + 120_000, |_| {
                    Ok(Some(Arc::clone(&provider)))
                })
                .unwrap();
            assert!(active_turn_report.released.is_empty());
            assert!(active_turn_report.skipped_busy.contains(&agent.id));

            // 5. Once the turn finishes and idle_ttl_ms elapses with inflight_children == 0,
            //    the sweeper releases compute while preserving the persistent volume & image metadata!
            store
                .end_turn(
                    &agent.id,
                    &open_turn.turn_id,
                    anyagents_core::TurnOutcome::Completed,
                )
                .unwrap();
            assert!(ingest_guest_heartbeat_value(
                &store,
                &agent.id,
                &idle_frame_1
            ));

            let release_report =
                sweep_idle_sandboxes_with_resolver(&store, &data_dir, t2 + 120_000, |_| {
                    Ok(Some(Arc::clone(&provider)))
                })
                .unwrap();
            assert!(
                release_report.failed.is_empty(),
                "unexpected sweep failures: {:?}",
                release_report.failed
            );
            assert!(
                release_report.released.contains(&agent.id),
                "expected agent {} in released: {release_report:?}",
                agent.id
            );

            let detached = store.sandbox_binding(&agent.id).unwrap().unwrap();
            assert_eq!(detached.lease_state, "detached");
            assert_eq!(detached.lease_id, None);
            assert_eq!(detached.idle_since_ms, None);
            assert_eq!(
                detached.volume_id, "vol-preserve-idle-1",
                "persistent /home/user/agent-data volume MUST survive compute release"
            );
            assert_eq!(detached.volume_resource, "volumes/vol-preserve-idle-1");
            assert_eq!(detached.remote_workdir, "/home/user/agent-data/workspace");
            assert_eq!(detached.image.as_deref(), Some(byo_image));
            assert_eq!(detached.template.as_deref(), Some("dev"));

            store.delete_agent(&agent.id).unwrap();
        })
        .await
        .unwrap();

        assert_eq!(
            release_calls.lock().unwrap().as_slice(),
            &["sbx-idle-1".to_string()],
            "idle sweeper must invoke provider.release exactly once"
        );
        assert!(
            destroy_calls.lock().unwrap().is_empty(),
            "idle sweeper must NEVER invoke provider.destroy"
        );
        srv.abort();
    }

    #[test]
    fn stateless_dockerfile_sandbox_dev_and_oci_workflow_targets_present() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let dockerfile = std::fs::read_to_string(root.join("Dockerfile.sandbox")).unwrap();
        assert!(dockerfile.contains("-p anyagents-guest --bin anyagents-guest"));
        assert!(dockerfile.contains("-p anyagents-mcp-browser --bin anyagents-mcp-browser"));
        assert!(dockerfile.contains("-p anyagents-mcp-web --bin anyagents-mcp-web"));
        assert!(dockerfile.contains("/usr/local/bin/anyagents-mcp-web"));
        assert!(dockerfile.contains("/usr/local/bin/anyagents-mcp-browser"));
        assert!(dockerfile.contains("select(.name == \"agent-tools\")"));
        assert!(dockerfile.contains("--bin agent-code-mode"));
        assert!(dockerfile.contains("/usr/local/bin/agent-code-mode"));
        assert!(dockerfile.contains("FROM sandbox-base AS sandbox-dev"));
        assert!(dockerfile.contains("https://sh.rustup.rs"));
        assert!(dockerfile.contains("COPY --from=bun-bin /usr/local/bin/bun /usr/local/bin/bun"));
        assert!(dockerfile.contains("FROM sandbox-dev AS sandbox-full"));
        assert!(dockerfile.contains("\"spiron\""));

        let workflow =
            std::fs::read_to_string(root.join(".github/workflows/docker-publish.yml")).unwrap();
        assert!(workflow.contains("github.event.repository.private == false"));
        assert!(workflow.contains("file: Dockerfile"));
        assert!(workflow.contains("file: Dockerfile.sandbox"));
        assert!(workflow.contains("target: sandbox-base"));
        assert!(workflow.contains("target: sandbox-dev"));
        assert!(workflow.contains("target: sandbox-full"));
        assert!(workflow.contains("anyagents-sandbox"));
        assert!(workflow.contains("steps.build.outputs.digest"));
    }

    #[test]
    fn filter_sandbox_launch_env_forwards_agent_vars_and_blocks_coordinator_secrets() {
        let raw = vec![
            ("ANYAGENTS_SCOPE".into(), "repo".into()),
            ("ANYAGENTS_LIFECYCLE".into(), "service".into()),
            (
                "CUSTOM_TOOL_ENDPOINT".into(),
                "https://example.invalid".into(),
            ),
            ("ANYAGENTS_OPERATOR_TOKEN".into(), "op-secret".into()),
            ("ANYAGENTS_DATABASE_URL".into(), "postgres://secret".into()),
            ("ANYAGENTS_MASTER_KEY".into(), "master-secret".into()),
            ("ANYAGENTS_CAPABILITY_KEY".into(), "cap-secret".into()),
            ("ANYAGENTS_TICK_TOKEN".into(), "tick-secret".into()),
            ("ANYAGENTS_SANDBOX_TOKEN".into(), "sbx-secret".into()),
            ("HOME".into(), "/wrong".into()),
            ("ANYAGENTS_MCP_URL".into(), "https://evil.invalid".into()),
            ("ANYAGENTS_GATEWAY_TOKEN".into(), "stale-gw".into()),
        ];
        let filtered = filter_sandbox_launch_env(&raw);
        let map: BTreeMap<String, String> = filtered.into_iter().collect();
        assert_eq!(map.get("ANYAGENTS_SCOPE").map(String::as_str), Some("repo"));
        assert_eq!(
            map.get("ANYAGENTS_LIFECYCLE").map(String::as_str),
            Some("service")
        );
        assert_eq!(
            map.get("CUSTOM_TOOL_ENDPOINT").map(String::as_str),
            Some("https://example.invalid")
        );
        for blocked in [
            "ANYAGENTS_OPERATOR_TOKEN",
            "ANYAGENTS_DATABASE_URL",
            "ANYAGENTS_MASTER_KEY",
            "ANYAGENTS_CAPABILITY_KEY",
            "ANYAGENTS_TICK_TOKEN",
            "ANYAGENTS_SANDBOX_TOKEN",
            "HOME",
            "ANYAGENTS_MCP_URL",
            "ANYAGENTS_GATEWAY_TOKEN",
        ] {
            assert!(
                !map.contains_key(blocked),
                "coordinator/protected var `{blocked}` must not be forwarded"
            );
        }
    }
}
