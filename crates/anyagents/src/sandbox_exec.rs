//! Remote Sandbox Process Spawn (`anyagents sandbox-exec`).
//!
//! Bridges `ClaudeHarness` (`stream-json` on stdin/stdout) to a remote sandbox
//! container (`POST /v1/sandboxes/{sandbox_id}/exec`), materializing `.mcp.json`
//! inside the remote workspace with the agent's Streamable HTTP `/mcp` URL and
//! bearer capability token.

use std::io::{BufRead, BufReader, Read as _, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyagents_core::Error;
use anyagents_harness::normalize_harness_name;
use anyagents_tools::{RemoteSandboxWorkspace, SandboxConfig, WorkspaceBackend};
use clap::Args;
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
    let config = args
        .config
        .or_else(|| std::env::var_os("ANYAGENTS_AGENT_CONFIG").map(PathBuf::from));
    let sbx = SandboxConfig::resolve(config.as_deref()).ok_or_else(|| {
        Error::Message(
            "sandbox-exec requires a configured remote sandbox (`sandbox.json` or `ANYAGENTS_SANDBOX_API_URL`)"
                .into(),
        )
    })?;

    let raw_harness = args
        .harness
        .or_else(|| std::env::var("ANYAGENTS_AGENT_HARNESS").ok())
        .unwrap_or_else(|| "claude-code".to_string());
    let harness = normalize_harness_name(&raw_harness).to_string();
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
            print!("{}", out.stdout);
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

    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();

    for line in BufReader::new(stdin.lock()).lines() {
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

        // Echo the user message back immediately so `ClaudeHarness` records the
        // message marker as echoed.
        writeln!(stdout, "{trimmed}")?;
        stdout.flush()?;

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
}
