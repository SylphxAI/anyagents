//! Remote Sandbox Process Spawn (`anyagents sandbox-exec`).
//!
//! Bridges `ClaudeHarness` (`stream-json` on stdin/stdout) to a remote sandbox
//! container (`POST /v1/sandboxes/{sandbox_id}/exec`), materializing `.mcp.json`
//! inside the remote workspace with the agent's Streamable HTTP `/mcp` URL and
//! bearer capability token.

use std::collections::{BTreeMap, HashMap};
use std::io::{BufRead, BufReader, Read as _, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use anyagents_core::{AgentId, Error, Store};
use anyagents_harness::normalize_harness_name;
use anyagents_tools::sylphx::{
    EnvdProcessEvent, EnvdSignal, SandboxDriver, SandboxMode, SylphxLeaseConfig,
    SylphxProviderConfig, ensure_leased_for_config_dir, renew_lease_for_config_dir,
};
use anyagents_tools::{RemoteSandboxWorkspace, SandboxConfig, WorkspaceBackend, sha256_hex};
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

    let out_writer: Arc<Mutex<dyn Write + Send>> = Arc::new(Mutex::new(writer));

    // Check native Sylphx Sandboxes v1 driver first (either already persisted
    // `sandbox-sylphx.json` or configured via `sandbox-provider.json` / env).
    if let Some(cfg_dir) = config.as_deref() {
        let mut lease_cfg = SylphxLeaseConfig::from_config_dir(cfg_dir);
        let sylphx_provider =
            SylphxProviderConfig::resolve(data_dir.as_deref()).map_err(Error::Message)?;
        if lease_cfg.is_some()
            || sylphx_provider
                .as_ref()
                .is_some_and(|p| p.driver == SandboxDriver::Sylphx)
        {
            let aid = agent_id_str.as_deref().unwrap_or("agent");
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
                aid_or_default(agent_id_str.as_deref()),
                &harness,
                args.program.as_deref(),
                args.resume.as_deref(),
                args.oneshot,
                &args.args,
                reader,
                out_writer,
            );
        }
    }

    let mode = SandboxMode::from_env();
    let sbx = SandboxConfig::resolve(config.as_deref()).ok_or_else(|| {
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
        "dsh" => "dsh",
        "grok" => "grok",
        _ => "claude",
    }
}

/// Write the sandbox's `.mcp.json`: the server's public MCP URL under the
/// `remote` profile and the agent's current remote session token. Returns the
/// token written, so a caller rewrites the file only after a rotation. The
/// agent's local mailbox capability never enters the sandbox.
pub fn materialize_remote_mcp_config(
    sbx: &SandboxConfig,
    config_dir: &Path,
) -> Result<String, String> {
    let capability = crate::remote::session_token(config_dir)?;
    let mcp_base = crate::cloud_harness::resolve_public_mcp_url(config_dir)?;
    let mcp_url = crate::cloud_harness::mcp_url_with_profile(&mcp_base, "remote");
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

/// Sync skills (dereferencing symlinks), instructions, harness MCP configs targeting
/// `/home/user/agent-state` + `anyagents-guest mcp-proxy`, repo checkout spec, Gateway
/// origin/token, and refreshed OAuth/provider credentials into the agent's Sylphx Volume.
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
    let state_root = lease.mount_path.trim_end_matches('/');

    let mut uploads: Vec<(String, Vec<u8>)> = Vec::new();
    let mut dirs_to_create: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    dirs_to_create.insert(state_root.to_string());
    dirs_to_create.insert(lease.workdir.clone());

    // 1. Agent instructions (`SYSTEM_PROMPT.md`, `AGENTS.md`, `CLAUDE.md`).
    for fname in ["SYSTEM_PROMPT.md", "AGENTS.md", "CLAUDE.md"] {
        let src = config_dir.join(fname);
        if let Ok(bytes) = std::fs::read(&src) {
            uploads.push((format!("{state_root}/{fname}"), bytes));
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
        collect_dereferenced_tree(
            &root,
            &format!("{state_root}/.agents/skills"),
            &mut uploads,
            &mut dirs_to_create,
        );
    }

    // 3. Remote MCP token + harness configs targeting `/home/user/agent-state` & `mcp-proxy`.
    let mcp_base = crate::cloud_harness::resolve_public_mcp_url(config_dir)?;
    let mcp_url = crate::cloud_harness::mcp_url_with_profile(&mcp_base, "full");
    let mcp_token = crate::remote::session_token(config_dir)?;
    let token_path_in_vm = format!("{state_root}/.mcp-token");
    uploads.push((
        token_path_in_vm.clone(),
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
    let stdio_mcp_json = serde_json::to_vec_pretty(&json!({
        "mcpServers": {
            "anyagents": {
                "command": guest_bin,
                "args": proxy_args,
            }
        }
    }))
    .map_err(|e| e.to_string())?;
    let mut stdio_mcp_bytes = stdio_mcp_json;
    stdio_mcp_bytes.push(b'\n');
    for target in [".mcp.json", ".claude.json", "mcp.json", "spiron-mcp.json"] {
        uploads.push((format!("{state_root}/{target}"), stdio_mcp_bytes.clone()));
    }
    dirs_to_create.insert(format!("{state_root}/.gemini/config"));
    uploads.push((
        format!("{state_root}/.gemini/config/mcp_config.json"),
        stdio_mcp_bytes.clone(),
    ));
    dirs_to_create.insert(format!("{state_root}/.omp/agent"));
    uploads.push((
        format!("{state_root}/.omp/agent/mcp.json"),
        stdio_mcp_bytes.clone(),
    ));

    let toml_cfg = build_sandbox_toml_config(&guest_bin, &mcp_url, &token_path_in_vm);
    uploads.push((
        format!("{state_root}/config.toml"),
        toml_cfg.as_bytes().to_vec(),
    ));
    dirs_to_create.insert(format!("{state_root}/.grok"));
    uploads.push((
        format!("{state_root}/.grok/config.toml"),
        toml_cfg.as_bytes().to_vec(),
    ));

    let opencode_json = serde_json::to_vec_pretty(&json!({
        "$schema": "https://opencode.ai/config.json",
        "mcp": {
            "anyagents": {
                "type": "local",
                "command": [guest_bin, "mcp-proxy", "--url", mcp_url, "--token-file", token_path_in_vm],
                "enabled": true,
            }
        }
    }))
    .map_err(|e| e.to_string())?;
    uploads.push((format!("{state_root}/opencode.json"), opencode_json));

    let dsh_patch = format!(
        "mcpServers:\n  anyagents:\n    command: {guest_bin}\n    args:\n      - mcp-proxy\n      - --url\n      - {mcp_url}\n      - --token-file\n      - {token_path_in_vm}\n"
    );
    uploads.push((
        format!("{state_root}/dsh-mcp-patch.yml"),
        dsh_patch.into_bytes(),
    ));

    // 4. Sync refreshed upstream OAuth/subscription credential files (NEVER provider.json or local mcp-capability).
    for cred_rel in [
        ".credentials.json",
        ".claude/.credentials.json",
        "auth.json",
        ".codex/auth.json",
        ".config/sylphx/auth.json",
        ".spiron/auth.json",
        "muse/auth.json",
        ".config/muse/auth.json",
        ".config/opencode/auth.json",
        ".grok/auth.json",
        ".gemini/antigravity-cli/antigravity-oauth-token",
    ] {
        let local_cred = config_dir.join(cred_rel);
        if local_cred.is_file()
            && let Ok(bytes) = std::fs::read(&local_cred)
        {
            let remote_path = format!("{state_root}/{cred_rel}");
            if let Some(parent) = Path::new(&remote_path).parent().and_then(|p| p.to_str()) {
                dirs_to_create.insert(parent.to_string());
            }
            uploads.push((remote_path, bytes));
        }
    }

    // 5. Repo clone/checkout spec (so guest checks out git repo on first boot).
    let repo_spec = read_synced_repo_spec(config_dir);
    if let Some(ref spec) = repo_spec {
        let spec_bytes = serde_json::to_vec_pretty(spec).map_err(|e| e.to_string())?;
        uploads.push((format!("{state_root}/repo-spec.json"), spec_bytes));
    }

    // 6. Agent metadata, Gateway URL + per-agent gateway token (without raw upstream API keys).
    let (agent_name, agent_model, gateway_env) = if let Some(d) = data_dir {
        if let Ok(aid) = agent_id_str.parse::<AgentId>() {
            let (name, model) = Store::open(d.join("state.db"))
                .ok()
                .and_then(|s| s.agent(&aid).ok())
                .map(|a| (Some(a.name), a.model))
                .unwrap_or((None, None));
            let gw_env = crate::gateway::sandbox_env(d, &aid)?;
            (name, model, gw_env)
        } else {
            (None, None, Vec::new())
        }
    } else {
        (None, None, Vec::new())
    };
    let mut env = gateway_env;
    env.push(("HOME".into(), state_root.to_string()));
    env.push(("ANYAGENTS_GUEST_STATE_DIR".into(), state_root.to_string()));
    env.push(("ANYAGENTS_GUEST_WORKDIR".into(), lease.workdir.clone()));
    env.push(("ANYAGENTS_MCP_URL".into(), mcp_url.clone()));
    env.push(("ANYAGENTS_MCP_TOKEN_FILE".into(), token_path_in_vm.clone()));

    let session_id = read_persisted_session_id(config_dir, harness);
    let guest_cfg = json!({
        "agent_id": agent_id_str,
        "agent_name": agent_name,
        "harness": harness,
        "model": agent_model,
        "session_id": session_id,
        "workdir": lease.workdir,
        "extra_args": extra_args,
        "env": env,
        "mcp_url": mcp_url,
        "mcp_token_file": token_path_in_vm,
        "repo_spec": repo_spec,
    });
    uploads.push((
        format!("{state_root}/guest-config.json"),
        serde_json::to_vec_pretty(&guest_cfg).map_err(|e| e.to_string())?,
    ));

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
    for (remote_path, bytes) in uploads {
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
    let renew_thread = std::thread::Builder::new()
        .name("anyagents-sylphx-renew".into())
        .spawn(move || {
            let renew_interval = Duration::from_secs(6 * 3600);
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
) -> Result<(), Error> {
    let client = lease.client();
    let envd_url = lease.resolved_envd_url().map_err(Error::Message)?;
    let envd_token = lease.envd_token.clone();
    let guest_bin = guest_binary_path();

    let mut guest_args = vec![
        "serve".to_string(),
        "--harness".to_string(),
        harness.to_string(),
        "--state-dir".to_string(),
        lease.mount_path.clone(),
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
    guest_env.insert("HOME".to_string(), lease.mount_path.clone());
    guest_env.insert(
        "ANYAGENTS_GUEST_STATE_DIR".to_string(),
        lease.mount_path.clone(),
    );
    guest_env.insert("ANYAGENTS_GUEST_WORKDIR".to_string(), lease.workdir.clone());

    let pid_slot: Arc<(Mutex<(Option<u32>, Vec<Vec<u8>>, bool)>, Condvar)> =
        Arc::new((Mutex::new((None, Vec::new(), false)), Condvar::new()));
    let pid_for_stream = Arc::clone(&pid_slot);

    let cfg_clone = config_dir.to_path_buf();
    let harness_clone = harness.to_string();
    let client_for_stream = client.clone();
    let envd_url_for_stream = envd_url.clone();
    let envd_tok_for_stream = envd_token.clone();
    let workdir_for_stream = lease.workdir.clone();
    let out_for_stream = Arc::clone(&out_writer);
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
                                        if let Ok(val) = serde_json::from_str::<Value>(trimmed)
                                            && let Some(sid) = val
                                                .get("session_id")
                                                .and_then(Value::as_str)
                                                .map(str::trim)
                                                .filter(|s| !s.is_empty())
                                        {
                                            persist_session_id(&cfg_clone, &harness_clone, sid);
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
        exec_env.insert("HOME".to_string(), lease.mount_path.clone());
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
            Some("https://mcp.example.test/mcp?profile=remote")
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
        let sbx = SandboxConfig::from_config_dir(&cfg_dir)
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
                            let bytes = anyagents_tools::base64_decode(
                                payload["content_base64"].as_str().unwrap(),
                            )
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
        assert!(mcp_json_raw.contains("profile=full"));

        assert!(files.contains_key("/home/user/agent-state/repo-spec.json"));
        // Exactly one mkdir exec batch due to SHA-256 manifest caching on 2nd sync.
        assert_eq!(exec_cmds.lock().unwrap().len(), 1);
        assert!(lease.workdir == "/home/user/workspace");
        assert!(!anyagents_tools::sylphx::SYLPHX_STATE_FILE.is_empty());

        srv.abort();
    }
}
