//! Outbound Cloud Harness Adapter (`spiron-cloud`, `claude-cloud`,
//! `openai-cloud`, `cursor-cloud`).
//!
//! Speaks the `stream-json` wire protocol on stdin/stdout so `ClaudeHarness`
//! and `anyagents-supervisor` supervise cloud agent runs with full turn
//! lifecycle, inbox delivery, and session resumption while dispatching each
//! turn over HTTP with the agent's Streamable HTTP `/mcp` capability attached.

use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicU64, Ordering},
    mpsc,
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyagents_core::Error;
use anyagents_harness::normalize_harness_name;
use clap::Args;
use serde_json::{Value, json};

mod acp;
pub mod serverless;
mod spiron;

#[derive(Args, Clone, Debug)]
pub struct CloudHarnessArgs {
    /// Agent configuration directory containing the server-issued remote session token,
    /// the MCP callback URL and credentials.
    #[arg(long)]
    pub config: Option<PathBuf>,
    /// Cloud harness identifier (`spiron-cloud`, `claude-cloud`, `openai-cloud`, `cursor-cloud`).
    #[arg(long)]
    pub harness: Option<String>,
    /// Model identifier to request from the cloud harness.
    #[arg(long)]
    pub model: Option<String>,
    /// Existing cloud session ID to resume.
    #[arg(long)]
    pub resume: Option<String>,
}

/// Extract `CloudHarnessArgs` when `anyagents` is invoked via `ClaudeHarness`
/// with leading flags (`-p --input-format stream-json ... cloud-harness --config ...`).
pub fn try_parse_from_raw_argv(argv: &[String]) -> Option<CloudHarnessArgs> {
    let pos = argv.iter().position(|arg| arg == "cloud-harness")?;
    let mut config: Option<PathBuf> = None;
    let mut harness: Option<String> = None;
    let mut model: Option<String> = None;
    let mut resume: Option<String> = None;

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
            "--model" if i + 1 < argv.len() => {
                model = Some(argv[i + 1].clone());
                i += 2;
            }
            "--resume" if i + 1 < argv.len() => {
                resume = Some(argv[i + 1].clone());
                i += 2;
            }
            _ => {
                i += 1;
            }
        }
    }

    Some(CloudHarnessArgs {
        config,
        harness,
        model,
        resume,
    })
}

pub fn run(args: CloudHarnessArgs) -> Result<(), Error> {
    run_with_io(args, BufReader::new(std::io::stdin()), std::io::stdout())
}

pub fn run_with_io<R: BufRead + Send + 'static, W: Write + Send + 'static>(
    args: CloudHarnessArgs,
    reader: R,
    writer: W,
) -> Result<(), Error> {
    let config = args
        .config
        .or_else(|| std::env::var_os("ANYAGENTS_AGENT_CONFIG").map(PathBuf::from))
        .ok_or_else(|| Error::Message("cloud-harness requires `--config <path>`".into()))?;
    let raw_harness = args
        .harness
        .or_else(|| std::env::var("ANYAGENTS_AGENT_HARNESS").ok())
        .unwrap_or_else(|| "spiron-cloud".to_string());
    let harness = normalize_harness_name(&raw_harness).to_string();
    let model = args.model.filter(|m| !m.trim().is_empty());
    let resume = args
        .resume
        .filter(|s| !s.trim().is_empty())
        .or_else(|| read_persisted_session_id(&config, &harness));

    // A peer that speaks `acp/v1` runs the session itself (`acp`).
    if let Some(peer) =
        acp::config(read_cloud_json(&config).as_ref(), &harness).map_err(Error::Message)?
    {
        let instructions = build_cloud_system_prompt(&config);
        return acp::serve(
            &peer,
            &config,
            &harness,
            resume,
            model,
            instructions,
            reader,
            writer,
        )
        .map_err(Error::Message);
    }

    if harness == "spiron-cloud" {
        return spiron::serve(&config, model.as_deref(), resume.as_deref(), reader, writer)
            .map_err(Error::Message);
    }

    let session_id = Arc::new(Mutex::new(resume));
    let writer = Arc::new(Mutex::new(writer));

    enum InputMsg {
        Line(Value),
        Eof,
    }

    let (in_tx, in_rx) = mpsc::sync_channel::<InputMsg>(64);
    std::thread::spawn(move || {
        for line in reader.lines() {
            let Ok(line) = line else { break };
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let Ok(event) = serde_json::from_str::<Value>(trimmed) else {
                continue;
            };
            if in_tx.send(InputMsg::Line(event)).is_err() {
                return;
            }
        }
        let _ = in_tx.send(InputMsg::Eof);
    });

    struct ActiveTurn {
        cancel_flag: Arc<TurnCancel>,
        active_job_id: Arc<Mutex<Option<String>>>,
        done_rx: mpsc::Receiver<Result<CloudTurnOutcome, CloudErrorDetail>>,
        handle: Option<std::thread::JoinHandle<()>>,
        cancelled: bool,
    }

    let mut active_turn: Option<ActiveTurn> = None;
    let mut pending_steers: VecDeque<Value> = VecDeque::new();
    let mut eof = false;

    let emit_turn_result = |outcome_res: Result<CloudTurnOutcome, CloudErrorDetail>,
                            sid_shared: &Arc<Mutex<Option<String>>>,
                            cfg: &Path,
                            harness_name: &str,
                            writer_shared: &Arc<Mutex<W>>| {
        match outcome_res {
            Ok(outcome) => {
                *sid_shared.lock().unwrap() = Some(outcome.session_id.clone());
                persist_session_id(cfg, harness_name, &outcome.session_id);
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
                let result_event = json!({
                    "type": "result",
                    "subtype": "success",
                    "is_error": false,
                    "session_id": outcome.session_id,
                    "result": outcome.reply_text,
                    "usage": {
                        "input_tokens": outcome.input_tokens,
                        "output_tokens": outcome.output_tokens,
                    }
                });
                let mut out = writer_shared.lock().unwrap();
                let _ = writeln!(out, "{assistant_event}");
                let _ = writeln!(out, "{result_event}");
                let _ = out.flush();
            }
            Err(detail) => {
                let fallback_sid = sid_shared
                    .lock()
                    .unwrap()
                    .clone()
                    .unwrap_or_else(|| format!("{harness_name}-session"));
                emit_structured_cloud_error(
                    &mut *writer_shared.lock().unwrap(),
                    &fallback_sid,
                    &detail,
                    None,
                );
            }
        }
    };

    loop {
        // 1. Drain all available stdin lines without ever blocking on an active HTTP turn.
        let mut recv_disconnected = false;
        while !eof {
            let next = if active_turn.is_none() && pending_steers.is_empty() {
                match in_rx.recv() {
                    Ok(msg) => Some(msg),
                    Err(_) => {
                        recv_disconnected = true;
                        None
                    }
                }
            } else {
                match in_rx.try_recv() {
                    Ok(msg) => Some(msg),
                    Err(mpsc::TryRecvError::Empty) => None,
                    Err(mpsc::TryRecvError::Disconnected) => {
                        recv_disconnected = true;
                        None
                    }
                }
            };
            if recv_disconnected {
                eof = true;
                break;
            }
            let Some(msg) = next else { break };
            match msg {
                InputMsg::Eof => {
                    eof = true;
                    break;
                }
                InputMsg::Line(event) => {
                    let event_type = event.get("type").and_then(Value::as_str).unwrap_or("");
                    if event_type == "control_request" || event_type == "cancel" {
                        let is_interrupt = event_type == "cancel"
                            || event
                                .pointer("/request/subtype")
                                .and_then(Value::as_str)
                                .is_some_and(|s| s == "interrupt" || s == "cancel");
                        if is_interrupt {
                            pending_steers.clear();
                            if let Some(turn) = active_turn.as_mut()
                                && !turn.cancelled
                            {
                                turn.cancelled = true;
                                turn.cancel_flag.cancel();
                                if let Some(job_id) = turn.active_job_id.lock().unwrap().clone() {
                                    let base_url = resolve_cloud_base_url(&config, &harness);
                                    let auth_token = resolve_cloud_auth_token(&config, &harness);
                                    let _ = cancel_cloud_turn(
                                        &base_url,
                                        &harness,
                                        auth_token.as_deref(),
                                        &job_id,
                                    );
                                }
                            }
                            if let Some(req_id) = event.get("request_id").and_then(Value::as_str) {
                                let ack = json!({
                                    "type": "control_response",
                                    "response": {
                                        "subtype": "success",
                                        "request_id": req_id
                                    }
                                });
                                let mut out = writer.lock().unwrap();
                                let _ = writeln!(out, "{ack}");
                                let _ = out.flush();
                            }
                        }
                    } else if event_type == "user" {
                        pending_steers.push_back(event);
                    }
                }
            }
        }

        // 2. Poll active turn completion without blocking stdin reads.
        if let Some(turn) = active_turn.as_mut()
            && let Ok(outcome_res) = turn.done_rx.try_recv()
        {
            if let Some(handle) = turn.handle.take() {
                let _ = handle.join();
            }
            active_turn = None;
            emit_turn_result(outcome_res, &session_id, &config, &harness, &writer);
        }

        // 3. Start next queued steer when no turn is currently active.
        if active_turn.is_none()
            && let Some(event) = pending_steers.pop_front()
        {
            // The line starts its turn now: echo it back as Claude Code's
            // `--replay-user-messages` does, the proof `ClaudeHarness` waits for.
            {
                let mut echo = event.clone();
                echo["isReplay"] = Value::Bool(true);
                let mut out = writer.lock().unwrap();
                writeln!(out, "{echo}")?;
                out.flush()?;
            }

            let prompt_text = extract_user_prompt(&event);
            let cancel_flag = Arc::new(TurnCancel::default());
            let active_job_id = Arc::new(Mutex::new(None));
            let (done_tx, done_rx) = mpsc::sync_channel(1);

            let cfg_clone = config.clone();
            let harness_clone = harness.clone();
            let model_clone = model.clone();
            let current_sid = session_id.lock().unwrap().clone();
            let cancel_clone = Arc::clone(&cancel_flag);
            let job_clone = Arc::clone(&active_job_id);

            let handle = std::thread::spawn(move || {
                let res = dispatch_cloud_turn_detailed(
                    &cfg_clone,
                    &harness_clone,
                    model_clone.as_deref(),
                    current_sid.as_deref(),
                    &prompt_text,
                    &cancel_clone,
                    &job_clone,
                );
                let _ = done_tx.send(res);
            });

            active_turn = Some(ActiveTurn {
                cancel_flag,
                active_job_id,
                done_rx,
                handle: Some(handle),
                cancelled: false,
            });
            continue;
        }

        if eof && active_turn.is_none() && pending_steers.is_empty() {
            break;
        }

        if let Some(turn) = active_turn.as_mut() {
            match turn.done_rx.recv_timeout(Duration::from_millis(10)) {
                Ok(outcome_res) => {
                    if let Some(handle) = turn.handle.take() {
                        let _ = handle.join();
                    }
                    active_turn = None;
                    emit_turn_result(outcome_res, &session_id, &config, &harness, &writer);
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    if let Some(handle) = turn.handle.take() {
                        let _ = handle.join();
                    }
                    active_turn = None;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
            }
        }
    }

    Ok(())
}

#[derive(Clone, Debug)]
pub struct CloudTurnOutcome {
    pub session_id: String,
    pub reply_text: String,
    pub input_tokens: u64,
    pub output_tokens: u64,
}

fn session_filename_for(harness: &str) -> &'static str {
    match normalize_harness_name(harness) {
        "spiron-cloud" => "spiron-cloud-session-id",
        "claude-cloud" => "claude-cloud-session-id",
        "openai-cloud" => "openai-cloud-session-id",
        "cursor-cloud" => "cursor-cloud-session-id",
        _ => "cloud-session-id",
    }
}

fn read_persisted_session_id(config: &Path, harness: &str) -> Option<String> {
    std::fs::read_to_string(config.join(session_filename_for(harness)))
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn persist_session_id(config: &Path, harness: &str, session_id: &str) {
    let trimmed = session_id.trim();
    if !trimmed.is_empty() {
        let path = config.join(session_filename_for(harness));
        if let Err(err) = anyagents_harness::atomic_write_nonempty_file(
            &path,
            format!("{trimmed}\n").as_bytes(),
            0o664,
        ) {
            eprintln!("cannot persist {}: {err}", path.display());
        }
    }
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

/// The MCP URL this agent's remote session calls back to. The server writes it
/// from `public_url` when it starts the agent ([`crate::remote::MCP_URL_FILE`]);
/// `ANYAGENTS_PUBLIC_MCP_URL` and `cloud.json` `mcp_url` remain as overrides
/// for an adapter run by hand. There is no loopback default: a remote vendor
/// cannot reach this host's loopback, and a token sent there is wasted.
pub fn resolve_public_mcp_url(config: &Path) -> Result<String, String> {
    if let Ok(file_url) = std::fs::read_to_string(config.join(crate::remote::MCP_URL_FILE)) {
        let trimmed = file_url.trim();
        if !trimmed.is_empty() {
            return Ok(trimmed.to_string());
        }
    }
    if let Ok(url) = std::env::var("ANYAGENTS_PUBLIC_MCP_URL") {
        let trimmed = url.trim();
        if !trimmed.is_empty() {
            return Ok(trimmed.to_string());
        }
    }
    if let Some(cloud_cfg) = read_cloud_json(config)
        && let Some(url) = cloud_cfg.get("mcp_url").and_then(Value::as_str)
        && !url.trim().is_empty()
    {
        return Ok(url.trim().to_string());
    }
    Err(format!(
        "no MCP callback URL for this remote agent in {}: set `public_url` under `[server]` in \
         the server's `{}` and start the agent again",
        config.display(),
        crate::remote::CONFIG_FILE
    ))
}

pub(super) fn validate_remote_mcp_url(config: &Path, url: &str) -> Result<String, String> {
    let parsed = url::Url::parse(url.trim()).map_err(|_| "invalid cloud MCP callback URL")?;
    let env_override = std::env::var("ANYAGENTS_ALLOW_LOOPBACK_MCP")
        .ok()
        .is_some_and(|v| matches!(v.trim(), "1" | "true"))
        || std::env::var("ANYAGENTS_TEST_ALLOW_LOOPBACK_MCP")
            .ok()
            .is_some_and(|v| matches!(v.trim(), "1" | "true"));
    let cfg_override = cfg!(test)
        && read_cloud_json(config)
            .and_then(|v| v.get("allow_loopback_mcp").and_then(Value::as_bool))
            == Some(true);
    let loopback_allowed = (env_override || cfg_override)
        && parsed
            .host_str()
            .is_some_and(|h| matches!(h, "127.0.0.1" | "[::1]" | "localhost"));
    if (parsed.scheme() != "https" && !(parsed.scheme() == "http" && loopback_allowed))
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.fragment().is_some()
    {
        return Err("Cloud MCP callback requires HTTPS without credentials or fragment".into());
    }
    Ok(url.trim().to_string())
}

fn read_cloud_json(config: &Path) -> Option<Value> {
    let raw = std::fs::read_to_string(config.join("cloud.json")).ok()?;
    serde_json::from_str(&raw).ok()
}

fn extract_token_from_json_file(path: &Path) -> Option<String> {
    let raw = std::fs::read_to_string(path).ok()?;
    let val: Value = serde_json::from_str(&raw).ok()?;
    for pointer in [
        "/api_key",
        "/apiKey",
        "/token",
        "/bearer_token",
        "/access_token",
        "/accessToken",
        "/jwt",
        "/spiron_api_key",
        "/SPIRON_API_KEY",
        "/OPENAI_API_KEY",
        "/ANTHROPIC_API_KEY",
        "/CURSOR_API_KEY",
        "/SYLPHX_API_KEY",
        "/tokens/access_token",
        "/claudeAiOauth/accessToken",
    ] {
        if let Some(s) = val.pointer(pointer).and_then(Value::as_str) {
            let trimmed = s.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

fn resolve_cloud_base_url(config: &Path, harness: &str) -> String {
    let cloud_json = read_cloud_json(config);
    let json_url = cloud_json
        .as_ref()
        .and_then(|v| {
            v.get("base_url")
                .or_else(|| v.get("endpoint"))
                .and_then(Value::as_str)
        })
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);

    match normalize_harness_name(harness) {
        "spiron-cloud" => std::env::var("ANYAGENTS_SPIRON_CLOUD_URL")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .or_else(|| {
                std::env::var("SPIRON_API_URL")
                    .ok()
                    .filter(|s| !s.trim().is_empty())
            })
            .or_else(|| {
                std::env::var("SPIRON_CLOUD_URL")
                    .ok()
                    .filter(|s| !s.trim().is_empty())
            })
            .or(json_url)
            .unwrap_or_else(|| "https://api.spiron.ai".to_string())
            .trim()
            .trim_end_matches('/')
            .to_string(),
        "claude-cloud" => std::env::var("ANYAGENTS_ANTHROPIC_API_URL")
            .ok()
            .or_else(|| std::env::var("ANTHROPIC_BASE_URL").ok())
            .filter(|s| !s.trim().is_empty())
            .or(json_url)
            .unwrap_or_else(|| "https://api.anthropic.com".to_string())
            .trim_end_matches('/')
            .to_string(),
        "openai-cloud" => std::env::var("ANYAGENTS_OPENAI_API_URL")
            .ok()
            .or_else(|| std::env::var("OPENAI_BASE_URL").ok())
            .filter(|s| !s.trim().is_empty())
            .or(json_url)
            .unwrap_or_else(|| "https://api.openai.com".to_string())
            .trim_end_matches('/')
            .to_string(),
        "cursor-cloud" => std::env::var("ANYAGENTS_CURSOR_API_URL")
            .ok()
            .or_else(|| std::env::var("CURSOR_BASE_URL").ok())
            .filter(|s| !s.trim().is_empty())
            .or(json_url)
            .unwrap_or_else(|| "https://api.cursor.com".to_string())
            .trim_end_matches('/')
            .to_string(),
        _ => json_url.unwrap_or_else(|| "http://127.0.0.1:8788".to_string()),
    }
}

fn resolve_cloud_auth_token(config: &Path, harness: &str) -> Option<String> {
    resolve_cloud_auth_token_checked(config, harness)
        .ok()
        .flatten()
}

pub(super) fn resolve_cloud_auth_token_checked(
    config: &Path,
    harness: &str,
) -> Result<Option<String>, String> {
    let canonical = normalize_harness_name(harness);
    if canonical == "spiron-cloud" {
        let mut candidates: Vec<String> = Vec::new();
        if let Some(token) = extract_token_from_json_file(&config.join("cloud.json")) {
            candidates.push(token);
        }
        for env_key in [
            "ANYAGENTS_SPIRON_API_KEY",
            "SPIRON_API_KEY",
            "SPIRON_TOKEN",
            "SYLPHX_API_KEY",
        ] {
            if let Ok(v) = std::env::var(env_key)
                && !v.trim().is_empty()
            {
                candidates.push(v.trim().to_string());
            }
        }
        if let Ok(raw) = std::fs::read_to_string(config.join(".spiron/key"))
            && !raw.trim().is_empty()
        {
            candidates.push(raw.trim().to_string());
        }
        for rel in [
            ".spiron/auth.json",
            ".spiron/credentials.json",
            ".config/sylphx/auth.json",
            ".config/sylphx/credentials.json",
            "credentials.json",
            "auth.json",
        ] {
            if let Some(tok) = extract_token_from_json_file(&config.join(rel)) {
                candidates.push(tok);
            }
        }
        let mut saw_gateway_key = false;
        for tok in candidates {
            if crate::spiron::is_sylphx_gateway_key(&tok) {
                saw_gateway_key = true;
                continue;
            }
            return Ok(Some(tok));
        }
        if saw_gateway_key {
            return Err(
                "API Error: 401 authentication_error: sign_in_required: only a Sylphx gateway key (sylphx_sk_...) is configured; spiron-cloud requires a Spiron Cloud API key (spiron_sk_...) or JWT"
                    .to_string(),
            );
        }
        return Ok(None);
    }

    if let Some(token) = extract_token_from_json_file(&config.join("cloud.json")) {
        return Ok(Some(token));
    }
    match canonical {
        "claude-cloud" => {
            for env_key in [
                "ANYAGENTS_ANTHROPIC_API_KEY",
                "ANTHROPIC_API_KEY",
                "CLAUDE_API_KEY",
            ] {
                if let Ok(v) = std::env::var(env_key)
                    && !v.trim().is_empty()
                {
                    return Ok(Some(v.trim().to_string()));
                }
            }
            for rel in [
                ".credentials.json",
                ".claude/.credentials.json",
                "auth.json",
            ] {
                if let Some(tok) = extract_token_from_json_file(&config.join(rel)) {
                    return Ok(Some(tok));
                }
            }
            Ok(None)
        }
        "openai-cloud" => {
            for env_key in ["ANYAGENTS_OPENAI_API_KEY", "OPENAI_API_KEY"] {
                if let Ok(v) = std::env::var(env_key)
                    && !v.trim().is_empty()
                {
                    return Ok(Some(v.trim().to_string()));
                }
            }
            for rel in ["auth.json", ".codex/auth.json", "credentials.json"] {
                if let Some(tok) = extract_token_from_json_file(&config.join(rel)) {
                    return Ok(Some(tok));
                }
            }
            Ok(None)
        }
        "cursor-cloud" => {
            for env_key in ["ANYAGENTS_CURSOR_API_KEY", "CURSOR_API_KEY"] {
                if let Ok(v) = std::env::var(env_key)
                    && !v.trim().is_empty()
                {
                    return Ok(Some(v.trim().to_string()));
                }
            }
            for rel in ["cursor-auth.json", "auth.json", "credentials.json"] {
                if let Some(tok) = extract_token_from_json_file(&config.join(rel)) {
                    return Ok(Some(tok));
                }
            }
            Ok(None)
        }
        _ => Ok(None),
    }
}

pub fn disable_builtin_tools_flag(config: &Path) -> bool {
    if std::env::var("ANYAGENTS_CLOUD_DISABLE_BUILTIN_TOOLS")
        .ok()
        .is_some_and(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true"))
    {
        return true;
    }
    read_cloud_json(config)
        .and_then(|v| v.get("disable_builtin_tools").and_then(Value::as_bool))
        .unwrap_or(false)
}

pub fn resolve_cloud_disabled_tools(config: &Path) -> Vec<String> {
    if let Some(cloud_cfg) = read_cloud_json(config)
        && let Some(arr) = cloud_cfg.get("disabled_tools").and_then(Value::as_array)
    {
        let explicit: Vec<String> = arr
            .iter()
            .filter_map(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect();
        if !explicit.is_empty() {
            return explicit;
        }
    }
    if disable_builtin_tools_flag(config) {
        // Only the vendor tools the `remote` MCP profile replaces. File, shell,
        // web, browser and computer tools stay with the vendor: their AnyAgents
        // counterparts run on the control-plane host, which a remote session
        // never reaches.
        return vec!["memory".to_string(), "skills".to_string()];
    }
    Vec::new()
}

pub fn collect_agent_skills_catalog(config: &Path) -> Vec<(String, String)> {
    let mut dirs_to_scan = Vec::new();
    if let Some(root) = config.parent() {
        dirs_to_scan.push(root.join("skills-view"));
        dirs_to_scan.push(root.join("skills"));
    }
    dirs_to_scan.push(config.join(".agents/skills"));
    dirs_to_scan.push(config.join("skills"));

    // Discovery's later roots win; retain this catalog's first-root priority.
    dirs_to_scan.reverse();
    let mut seen = std::collections::BTreeMap::new();
    for (slug, path) in anyagents_core::host_skills::discover(&dirs_to_scan) {
        let Ok(raw_md) = std::fs::read_to_string(path.join("SKILL.md")) else {
            continue;
        };
        let (_, desc) = anyagents_core::learned_memory::parse_topic_frontmatter(&slug, &raw_md);
        seen.insert(slug, desc);
    }
    seen.into_iter().collect()
}

pub fn build_cloud_system_prompt(config: &Path) -> Option<String> {
    let mut sections = Vec::new();

    let base_prompt = std::fs::read_to_string(config.join("SYSTEM_PROMPT.md"))
        .or_else(|_| std::fs::read_to_string(config.join("AGENTS.md")))
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    let has_memory = base_prompt
        .as_deref()
        .is_some_and(|p| p.contains("<learned_memory>") || p.contains("<memory>"));
    let has_skills = base_prompt
        .as_deref()
        .is_some_and(|p| p.contains("<available_skills>") || p.contains("<mounted_skills>"));

    if let Some(base) = base_prompt {
        sections.push(base);
    }

    if !has_memory {
        let mut memory_candidates = vec![config.join("memory/MEMORY.md")];
        if let Some(root) = config.parent() {
            memory_candidates.insert(0, root.join("memory/MEMORY.md"));
        }
        for mem_path in memory_candidates {
            if let Ok(raw_mem) = std::fs::read_to_string(&mem_path) {
                let trimmed = raw_mem.trim();
                if !trimmed.is_empty() {
                    sections.push(format!("<learned_memory>\n{trimmed}\n</learned_memory>"));
                    break;
                }
            }
        }
    }

    if !has_skills {
        let skills = collect_agent_skills_catalog(config);
        if !skills.is_empty() {
            let mut block = String::from(
                "<available_skills>\n\
                 When a task matches a skill's description below, call the `skills` MCP tool with `{\"action\": \"read\", \"name\": \"<skill-name>\"}` to load its full `SKILL.md` instructions before proceeding:\n",
            );
            for (name, desc) in skills {
                block.push_str(&format!("- `{name}`: {desc}\n"));
            }
            block.push_str("</available_skills>");
            sections.push(block);
        }
    }

    if sections.is_empty() {
        None
    } else {
        Some(sections.join("\n\n"))
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn dispatch_cloud_turn(
    config: &Path,
    harness: &str,
    model: Option<&str>,
    session_id: Option<&str>,
    prompt_text: &str,
) -> Result<CloudTurnOutcome, String> {
    let cancel_flag = TurnCancel::default();
    let active_job_id = Mutex::new(None);
    dispatch_cloud_turn_with_cancel(
        config,
        harness,
        model,
        session_id,
        prompt_text,
        &cancel_flag,
        &active_job_id,
    )
}

pub(super) const DEFAULT_CLOUD_IDLE_TIMEOUT_SECS: u64 = 120;

pub(super) fn cloud_idle_timeout() -> Duration {
    Duration::from_secs(
        std::env::var("ANYAGENTS_CLOUD_IDLE_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(DEFAULT_CLOUD_IDLE_TIMEOUT_SECS)
            .clamp(1, 3_600),
    )
}

pub(super) fn now_epoch_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_millis()).ok())
        .unwrap_or(0)
}

#[derive(Clone, Debug)]
pub(super) struct CloudErrorDetail {
    pub status: Option<u16>,
    pub code: &'static str,
    pub message: String,
    pub quota_reset_at_ms: Option<i64>,
}

impl CloudErrorDetail {
    pub(super) fn generic(message: impl Into<String>) -> Self {
        Self {
            status: None,
            code: "cloud_error",
            message: message.into(),
            quota_reset_at_ms: None,
        }
    }
}

pub(super) fn emit_structured_cloud_error(
    out: &mut impl Write,
    session_id: &str,
    detail: &CloudErrorDetail,
    completed_uuids: Option<&[Value]>,
) {
    if detail.code == "rate_limit" {
        let mut rate_ev = json!({
            "type": "rate_limit",
            "category": "rate_limit",
            "apiErrorStatus": detail.status.unwrap_or(429),
            "session_id": session_id,
        });
        if let Some(reset_ms) = detail.quota_reset_at_ms {
            rate_ev["quota_reset_at_ms"] = json!(reset_ms);
            rate_ev["resets_at_ms"] = json!(reset_ms);
        }
        let _ = writeln!(out, "{rate_ev}");
    }
    let mut result_ev = json!({
        "type": "result",
        "subtype": "error",
        "is_error": true,
        "error": detail.code,
        "session_id": session_id,
        "result": detail.message,
    });
    if let Some(status) = detail.status {
        result_ev["api_status"] = json!(status);
        result_ev["apiErrorStatus"] = json!(status);
    }
    if let Some(reset_ms) = detail.quota_reset_at_ms {
        result_ev["quota_reset_at_ms"] = json!(reset_ms);
        result_ev["resets_at_ms"] = json!(reset_ms);
    }
    if let Some(uuids) = completed_uuids {
        result_ev["completed_message_uuids"] = Value::Array(uuids.to_vec());
    }
    let _ = writeln!(out, "{result_ev}");
    let _ = out.flush();
}

pub(super) fn parse_retry_after_ms(header_val: Option<&str>, now_ms: i64) -> Option<i64> {
    let raw = header_val?.trim();
    if raw.is_empty() {
        return None;
    }
    if let Ok(secs) = raw.parse::<f64>()
        && secs.is_finite()
        && secs >= 0.0
    {
        return Some(now_ms.saturating_add((secs * 1000.0).round() as i64));
    }
    if let Some(ms) = anyagents_harness::quota::parse_rfc3339_utc_ms(raw) {
        return Some(ms);
    }
    parse_http_date_ms(raw)
}

fn parse_http_date_ms(raw: &str) -> Option<i64> {
    // IMF-fixdate: "Wed, 21 Oct 2015 07:28:00 GMT"
    let after_comma = raw.split_once(',').map(|(_, r)| r.trim()).unwrap_or(raw);
    let mut parts = after_comma.split_whitespace();
    let day: i64 = parts.next()?.parse().ok()?;
    let month: i64 = match parts.next()? {
        "Jan" => 1,
        "Feb" => 2,
        "Mar" => 3,
        "Apr" => 4,
        "May" => 5,
        "Jun" => 6,
        "Jul" => 7,
        "Aug" => 8,
        "Sep" => 9,
        "Oct" => 10,
        "Nov" => 11,
        "Dec" => 12,
        _ => return None,
    };
    let year: i64 = parts.next()?.parse().ok()?;
    let hms = parts.next()?;
    let tz = parts.next()?;
    if !tz.eq_ignore_ascii_case("GMT") && !tz.eq_ignore_ascii_case("UTC") {
        return None;
    }
    let mut t = hms.split(':');
    let hour: i64 = t.next()?.parse().ok()?;
    let min: i64 = t.next()?.parse().ok()?;
    let sec: i64 = t.next()?.parse().ok()?;
    if !(1..=31).contains(&day) || hour > 23 || min > 59 || sec > 60 {
        return None;
    }
    // Howard Hinnant days_from_civil (UTC epoch 1970-01-01)
    let y = year - i64::from(month <= 2);
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let doy = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    let secs = days
        .checked_mul(86_400)?
        .checked_add(hour * 3_600 + min * 60 + sec)?;
    secs.checked_mul(1_000)
}

fn extract_reset_from_json_and_headers(
    body_json: Option<&Value>,
    body_text: &str,
    retry_after_header: Option<&str>,
    now_ms: i64,
) -> Option<i64> {
    if let Some(val) = body_json {
        for ptr in [
            "/quota_reset_at_ms",
            "/resets_at_ms",
            "/reset_at_ms",
            "/error/quota_reset_at_ms",
            "/error/resets_at_ms",
            "/error/reset_at_ms",
        ] {
            if let Some(ms) = val.pointer(ptr).and_then(Value::as_i64) {
                return Some(ms);
            }
        }
        for ptr in ["/resets_at", "/reset_at", "/resetsAt", "/error/resets_at"] {
            if let Some(n) = val.pointer(ptr).and_then(Value::as_i64) {
                return Some(if n >= 1_000_000_000_000 { n } else { n * 1_000 });
            }
            if let Some(s) = val.pointer(ptr).and_then(Value::as_str)
                && let Some(ms) = anyagents_harness::quota::parse_rfc3339_utc_ms(s)
            {
                return Some(ms);
            }
        }
        for ptr in [
            "/retry_after_ms",
            "/error/retry_after_ms",
            "/retry_delay_ms",
        ] {
            if let Some(delay) = val.pointer(ptr).and_then(Value::as_i64)
                && delay >= 0
            {
                return Some(now_ms.saturating_add(delay));
            }
        }
        for ptr in ["/retry_after", "/error/retry_after"] {
            if let Some(secs) = val.pointer(ptr).and_then(Value::as_f64)
                && secs.is_finite()
                && secs >= 0.0
            {
                return Some(now_ms.saturating_add((secs * 1000.0).round() as i64));
            }
        }
    }
    parse_retry_after_ms(retry_after_header, now_ms)
        .or_else(|| anyagents_harness::quota::reset_at_ms(body_text, now_ms))
}

pub(super) fn classify_http_error(
    status: u16,
    retry_after_header: Option<&str>,
    raw_body: &str,
    canonical: &str,
    endpoint: &str,
) -> CloudErrorDetail {
    let now_ms = now_epoch_ms();
    let parsed: Option<Value> = serde_json::from_str(raw_body).ok();
    let trimmed = raw_body.trim();
    match status {
        429 => {
            let quota_reset_at_ms = extract_reset_from_json_and_headers(
                parsed.as_ref(),
                trimmed,
                retry_after_header,
                now_ms,
            );
            CloudErrorDetail {
                status: Some(429),
                code: "rate_limit",
                message: if trimmed.is_empty() {
                    format!("API Error: 429 rate_limit from {canonical} at {endpoint}")
                } else {
                    format!("API Error: 429 rate_limit from {canonical}: {trimmed}")
                },
                quota_reset_at_ms,
            }
        }
        401 => CloudErrorDetail {
            status: Some(401),
            code: "authentication_failed",
            message: if trimmed.is_empty() {
                format!("API Error: 401 authentication_error from {canonical} at {endpoint}")
            } else {
                format!("API Error: 401 authentication_error from {canonical}: {trimmed}")
            },
            quota_reset_at_ms: None,
        },
        503 => CloudErrorDetail {
            status: Some(503),
            code: "service_unavailable",
            message: if trimmed.is_empty() {
                format!("API Error: 503 service_unavailable from {canonical} at {endpoint}")
            } else {
                format!("API Error: 503 service_unavailable from {canonical}: {trimmed}")
            },
            quota_reset_at_ms: None,
        },
        _ => CloudErrorDetail {
            status: Some(status),
            code: "cloud_error",
            message: format!("cloud harness `{canonical}` returned HTTP {status}: {trimmed}"),
            quota_reset_at_ms: None,
        },
    }
}

pub fn dispatch_cloud_turn_with_cancel(
    config: &Path,
    harness: &str,
    model: Option<&str>,
    session_id: Option<&str>,
    prompt_text: &str,
    cancel_flag: &TurnCancel,
    active_job_id: &Mutex<Option<String>>,
) -> Result<CloudTurnOutcome, String> {
    dispatch_cloud_turn_detailed(
        config,
        harness,
        model,
        session_id,
        prompt_text,
        cancel_flag,
        active_job_id,
    )
    .map_err(|d| d.message)
}

fn dispatch_cloud_turn_detailed(
    config: &Path,
    harness: &str,
    model: Option<&str>,
    session_id: Option<&str>,
    prompt_text: &str,
    cancel_flag: &TurnCancel,
    active_job_id: &Mutex<Option<String>>,
) -> Result<CloudTurnOutcome, CloudErrorDetail> {
    if cancel_flag.is_cancelled() {
        return Err(CloudErrorDetail::generic(
            "cancelled by supervisor interrupt",
        ));
    }

    // The remote session token the server issued for this agent, read before
    // every turn so a rotation takes effect at once. The agent's local mailbox
    // capability is never handed to a vendor.
    if normalize_harness_name(harness) == "spiron-cloud" {
        return spiron::dispatch_detailed(
            config,
            model,
            session_id,
            prompt_text,
            cancel_flag,
            active_job_id,
        );
    }
    let capability = crate::remote::session_token(config).map_err(CloudErrorDetail::generic)?;
    let mcp_base_url = resolve_public_mcp_url(config).map_err(CloudErrorDetail::generic)?;
    let mcp_base_url =
        validate_remote_mcp_url(config, &mcp_base_url).map_err(CloudErrorDetail::generic)?;
    let base_url = resolve_cloud_base_url(config, harness);
    let auth_token =
        resolve_cloud_auth_token_checked(config, harness).map_err(|msg| CloudErrorDetail {
            status: Some(401),
            code: "authentication_failed",
            message: msg,
            quota_reset_at_ms: None,
        })?;
    let canonical = normalize_harness_name(harness);

    let mcp_url = mcp_base_url;
    let system_prompt = build_cloud_system_prompt(config);
    let disabled_tools = resolve_cloud_disabled_tools(config);

    let (endpoint, payload) = match canonical {
        "claude-cloud" => {
            let mut body = json!({
                "model": model.unwrap_or("claude-sonnet-4-6"),
                "max_tokens": 8192,
                "messages": [
                    {
                        "role": "user",
                        "content": prompt_text,
                    }
                ],
                "mcp_servers": [
                    {
                        "type": "url",
                        "name": "anyagents",
                        "url": mcp_url,
                        "authorization_token": capability,
                    }
                ],
            });
            if let Some(sys) = system_prompt {
                body["system"] = Value::String(sys);
            }
            if !disabled_tools.is_empty() {
                let configs: Vec<Value> = disabled_tools
                    .iter()
                    .map(|name| json!({ "name": name, "enabled": false }))
                    .collect();
                body["toolset_configs"] = Value::Array(configs);
            }
            (format!("{base_url}/v1/messages"), body)
        }
        "openai-cloud" => {
            let mut body = json!({
                "model": model.unwrap_or("gpt-5.4"),
                "input": prompt_text,
                "previous_response_id": session_id,
                "tools": [
                    {
                        "type": "mcp",
                        "server_label": "anyagents",
                        "server_url": mcp_url,
                        "headers": {
                            "Authorization": format!("Bearer {capability}"),
                        },
                        "require_approval": "never",
                    }
                ],
            });
            if let Some(sys) = system_prompt {
                body["instructions"] = Value::String(sys);
            }
            (format!("{base_url}/v1/responses"), body)
        }
        "cursor-cloud" => {
            let mut body = json!({
                "prompt": {
                    "text": prompt_text,
                },
                "model": model.unwrap_or("claude-4.6-opus"),
                "session_id": session_id,
                "mcp_servers": [
                    {
                        "name": "anyagents",
                        "url": mcp_url,
                        "bearer_token": capability,
                    }
                ],
            });
            if let Some(sys) = system_prompt {
                body["system_prompt"] = Value::String(sys);
            }
            if !disabled_tools.is_empty() {
                body["config"] = json!({
                    "permissions": {
                        "deny": disabled_tools,
                    }
                });
            }
            (format!("{base_url}/v0/agents"), body)
        }
        other => {
            return Err(CloudErrorDetail::generic(format!(
                "unsupported cloud harness `{other}`"
            )));
        }
    };

    let idle_timeout = cloud_idle_timeout();
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .timeout_connect(Some(Duration::from_secs(15)))
        .timeout_send_request(Some(Duration::from_secs(30)))
        .timeout_send_body(Some(Duration::from_secs(30)))
        .timeout_recv_response(Some(idle_timeout))
        .timeout_recv_body(None)
        .timeout_global(None)
        .http_status_as_error(false)
        .build()
        .into();

    let mut req = agent
        .post(&endpoint)
        .header("content-type", "application/json")
        .header("accept", "text/event-stream, application/json");
    if let Some(ref tok) = auth_token {
        req = req.header("authorization", &format!("Bearer {tok}"));
        if canonical == "claude-cloud" {
            req = req.header("x-api-key", tok);
        }
    }
    if canonical == "claude-cloud" {
        req = req
            .header("anthropic-version", "2023-06-01")
            .header("anthropic-beta", "mcp-client-2025-04-04");
    }

    let resp = req.send_json(&payload).map_err(|e| {
        CloudErrorDetail::generic(format!(
            "cloud harness `{canonical}` request to {endpoint} failed: {e}"
        ))
    })?;
    let status = resp.status().as_u16();
    let retry_after = resp
        .headers()
        .get("retry-after")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let content_type = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_ascii_lowercase();

    if !(200..300).contains(&status) {
        let mut raw_body = String::new();
        let _ = resp
            .into_body()
            .into_reader()
            .take(64 * 1024)
            .read_to_string(&mut raw_body);
        return Err(classify_http_error(
            status,
            retry_after.as_deref(),
            &raw_body,
            canonical,
            &endpoint,
        ));
    }

    if content_type.contains("text/event-stream") {
        let reader = BufReader::new(resp.into_body().into_reader());
        return read_sse_cloud_stream(reader, canonical, session_id, idle_timeout, cancel_flag);
    }

    let raw_body =
        read_body_with_idle_timeout(resp.into_body().into_reader(), idle_timeout, cancel_flag)
            .map_err(|msg| {
                CloudErrorDetail::generic(format!(
                    "failed reading response body from {endpoint}: {msg}"
                ))
            })?;

    let trimmed_body = raw_body.trim_start();
    if trimmed_body.starts_with("event:") || trimmed_body.starts_with("data:") {
        return parse_sse_cloud_response(&raw_body, canonical, session_id)
            .map_err(CloudErrorDetail::generic);
    }

    let mut parsed: Value = serde_json::from_str(&raw_body).map_err(|e| {
        CloudErrorDetail::generic(format!(
            "cloud harness `{canonical}` returned invalid JSON from {endpoint}: {e}"
        ))
    })?;

    if let Some(initial_status) = parsed.get("status").and_then(Value::as_str)
        && matches!(
            initial_status.trim().to_ascii_lowercase().as_str(),
            "queued" | "running" | "in_progress" | "pending"
        )
        && let Some(job_id) = extract_cloud_run_id(&parsed)
    {
        *active_job_id.lock().unwrap() = Some(job_id.clone());
        parsed = poll_cloud_job_until_done_detailed(
            &agent,
            &base_url,
            canonical,
            auth_token.as_deref(),
            &job_id,
            cancel_flag,
        )?;
    }

    let resolved_session_id = extract_cloud_session_id(&parsed, canonical)
        .or_else(|| session_id.map(str::to_string))
        .unwrap_or_else(|| format!("{canonical}-session-1"));

    let reply_text = extract_cloud_reply_text(&parsed).map_err(CloudErrorDetail::generic)?;
    let input_tokens = parsed
        .pointer("/usage/input_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let output_tokens = parsed
        .pointer("/usage/output_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);

    Ok(CloudTurnOutcome {
        session_id: resolved_session_id,
        reply_text,
        input_tokens,
        output_tokens,
    })
}

fn read_body_with_idle_timeout<R: Read + Send + 'static>(
    reader: R,
    idle_timeout: Duration,
    cancel_flag: &TurnCancel,
) -> Result<String, String> {
    let (tx, rx) = mpsc::sync_channel::<Result<Vec<u8>, String>>(8);
    std::thread::spawn(move || {
        let mut r = reader;
        let mut buf = [0u8; 8192];
        loop {
            match r.read(&mut buf) {
                Ok(0) => {
                    let _ = tx.send(Ok(Vec::new()));
                    return;
                }
                Ok(n) => {
                    if tx.send(Ok(buf[..n].to_vec())).is_err() {
                        return;
                    }
                }
                Err(e) => {
                    let _ = tx.send(Err(e.to_string()));
                    return;
                }
            }
        }
    });
    let mut out = Vec::new();
    let mut last_activity = Instant::now();
    loop {
        if cancel_flag.is_cancelled() {
            return Err("cancelled by supervisor interrupt".into());
        }
        match rx.recv_timeout(Duration::from_millis(50)) {
            Ok(Ok(chunk)) if chunk.is_empty() => {
                return String::from_utf8(out).map_err(|e| format!("invalid UTF-8 body: {e}"));
            }
            Ok(Ok(chunk)) => {
                last_activity = Instant::now();
                out.extend_from_slice(&chunk);
            }
            Ok(Err(e)) => return Err(e),
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return String::from_utf8(out).map_err(|e| format!("invalid UTF-8 body: {e}"));
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if last_activity.elapsed() >= idle_timeout {
                    return Err(format!(
                        "cloud HTTP body idle timeout after {}s",
                        idle_timeout.as_secs()
                    ));
                }
            }
        }
    }
}

pub(super) fn extract_cloud_run_id(parsed: &Value) -> Option<String> {
    for key in ["run_id", "task_id", "receipt_id", "response_id", "id"] {
        if let Some(s) = parsed.get(key).and_then(Value::as_str) {
            let trimmed = s.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

pub(super) fn extract_cloud_session_id(parsed: &Value, canonical: &str) -> Option<String> {
    for key in ["session_id", "agent_id", "conversation_id"] {
        if let Some(s) = parsed.get(key).and_then(Value::as_str) {
            let trimmed = s.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    if matches!(canonical, "claude-cloud" | "openai-cloud") {
        for key in ["response_id", "id"] {
            if let Some(s) = parsed.get(key).and_then(Value::as_str) {
                let trimmed = s.trim();
                if !trimmed.is_empty() {
                    return Some(trimmed.to_string());
                }
            }
        }
    } else if parsed.get("run_id").is_none()
        && parsed
            .get("status")
            .and_then(Value::as_str)
            .is_none_or(|st| {
                !matches!(
                    st.trim().to_ascii_lowercase().as_str(),
                    "queued" | "running" | "in_progress" | "pending"
                )
            })
        && let Some(s) = parsed.get("id").and_then(Value::as_str)
    {
        let trimmed = s.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }
    None
}

pub fn cloud_job_status_url(base_url: &str, harness: &str, job_id: &str) -> Result<String, String> {
    let base = base_url.trim_end_matches('/');
    Ok(match normalize_harness_name(harness) {
        "spiron-cloud" => {
            let job_id = spiron::validate_id(job_id, "job_id")?;
            format!("{base}/v1/message-receipts/{job_id}")
        }
        "claude-cloud" => format!("{base}/v1/messages/{job_id}"),
        "openai-cloud" => format!("{base}/v1/responses/{job_id}"),
        "cursor-cloud" => format!("{base}/v0/agents/{job_id}"),
        _ => return Err("unsupported cloud harness".into()),
    })
}

pub fn cancel_spiron_run(
    base_url: &str,
    auth_token: Option<&str>,
    agent_id: &str,
    run_id: &str,
    receipt_id: Option<&str>,
) -> Result<(), String> {
    let base = base_url.trim_end_matches('/');
    let agent_id = spiron::validate_id(agent_id, "agent_id")?;
    let run_id = spiron::validate_id(run_id, "run_id")?;
    let run_cancel_url = format!("{base}/v1/agents/{agent_id}/runs/{run_id}/cancel");
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(15)))
        .http_status_as_error(false)
        .build()
        .into();
    let mut req = agent
        .post(&run_cancel_url)
        .header("content-type", "application/json");
    if let Some(tok) = auth_token {
        req = req.header("authorization", &format!("Bearer {tok}"));
    }
    let resp = req
        .send_empty()
        .map_err(|e| format!("cancel request to {run_cancel_url} failed: {e}"))?;
    let status = resp.status().as_u16();
    if status == 409 || (200..300).contains(&status) {
        return Ok(());
    }
    if matches!(status, 404 | 405 | 501)
        && let Some(receipt) = receipt_id
    {
        return cancel_cloud_turn(base, "spiron-cloud", auth_token, receipt);
    }
    Err(format!("Spiron run cancellation returned HTTP {status}"))
}

pub fn cancel_cloud_turn(
    base_url: &str,
    harness: &str,
    auth_token: Option<&str>,
    job_id: &str,
) -> Result<(), String> {
    let canonical = normalize_harness_name(harness);
    let cancel_url = if canonical == "spiron-cloud" {
        let base = base_url.trim_end_matches('/');
        let job_id = spiron::validate_id(job_id, "job_id")?;
        format!("{base}/v1/message-receipts/{job_id}:cancel")
    } else {
        format!(
            "{}/cancel",
            cloud_job_status_url(base_url, canonical, job_id)?
        )
    };
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(15)))
        .http_status_as_error(false)
        .build()
        .into();
    let mut req = agent
        .post(&cancel_url)
        .header("content-type", "application/json");
    if let Some(tok) = auth_token {
        req = req.header("authorization", &format!("Bearer {tok}"));
        if canonical == "claude-cloud" {
            req = req.header("x-api-key", tok);
        }
    }
    let mut resp = if canonical == "spiron-cloud" {
        req.send_empty()
    } else {
        req.send_json(json!({ "action": "cancel" }))
    }
    .map_err(|e| format!("cancel request to {cancel_url} failed: {e}"))?;
    let status = resp.status().as_u16();
    if canonical == "spiron-cloud" {
        if status == 409 {
            return Ok(());
        } // Completion won the cancellation race.
        if status != 200 {
            return Err(format!("Spiron cancellation returned HTTP {status}"));
        }
        let receipt: Value = resp
            .body_mut()
            .read_json()
            .map_err(|_| "invalid Spiron cancel receipt")?;
        if receipt.get("id").and_then(Value::as_str) != Some(job_id)
            || !matches!(
                receipt.get("status").and_then(Value::as_str),
                Some(
                    "submitted"
                        | "working"
                        | "input_required"
                        | "canceled"
                        | "completed"
                        | "failed"
                )
            )
        {
            return Err("Spiron cancellation was not confirmed".into());
        }
        return Ok(());
    }
    if !(200..300).contains(&status) {
        let body = resp.body_mut().read_to_string().unwrap_or_default();
        return Err(format!("cancel returned HTTP {status}: {}", body.trim()));
    }
    Ok(())
}

/// A turn's cancellation: set once when the supervisor interrupts the turn,
/// and waited on by the turn between status polls, so an interrupt ends the
/// wait at once instead of at the next poll.
#[derive(Default)]
pub struct TurnCancel {
    cancelled: Mutex<bool>,
    signal: Condvar,
}

impl TurnCancel {
    pub fn cancel(&self) {
        *self.cancelled.lock().unwrap_or_else(|e| e.into_inner()) = true;
        self.signal.notify_all();
    }

    pub fn is_cancelled(&self) -> bool {
        *self.cancelled.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Wait up to `timeout`; `true` when the turn was cancelled.
    fn wait(&self, timeout: Duration) -> bool {
        let cancelled = self.cancelled.lock().unwrap_or_else(|e| e.into_inner());
        *self
            .signal
            .wait_timeout_while(cancelled, timeout, |cancelled| !*cancelled)
            .unwrap_or_else(|e| e.into_inner())
            .0
    }
}

/// The longest wait between two status polls of one cloud job (`250ms..5s` with
/// jitter). Polls start at the configured interval and double up to this.
const MAX_POLL_INTERVAL: Duration = Duration::from_secs(5);
const DEFAULT_INITIAL_POLL_INTERVAL_MS: u64 = 250;
const MAX_TRANSIENT_POLL_ERRORS: u32 = 5;

/// Polls a job may make at its first interval: the whole wait a job gets.
const POLL_BUDGET: u32 = 6_000;

static POLL_JITTER_SEQ: AtomicU64 = AtomicU64::new(0);

pub(super) fn apply_poll_jitter(base: Duration) -> Duration {
    let base_ms = base.as_millis() as u64;
    if base_ms < 25 {
        return base;
    }
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    let seq = POLL_JITTER_SEQ.fetch_add(1, Ordering::Relaxed);
    let bucket = ((nanos ^ seq.wrapping_mul(2_654_435_761)) % 41) as i64 - 20; // -20%..=+20%
    let jittered = ((base_ms as i64) * (100 + bucket) / 100).max(10) as u64;
    Duration::from_millis(jittered)
}

#[cfg_attr(not(test), allow(dead_code))]
fn poll_cloud_job_until_done(
    agent: &ureq::Agent,
    base_url: &str,
    canonical: &str,
    auth_token: Option<&str>,
    job_id: &str,
    cancel_flag: &TurnCancel,
) -> Result<Value, String> {
    poll_cloud_job_until_done_detailed(agent, base_url, canonical, auth_token, job_id, cancel_flag)
        .map_err(|d| d.message)
}

fn poll_cloud_job_until_done_detailed(
    agent: &ureq::Agent,
    base_url: &str,
    canonical: &str,
    auth_token: Option<&str>,
    job_id: &str,
    cancel_flag: &TurnCancel,
) -> Result<Value, CloudErrorDetail> {
    let status_url =
        cloud_job_status_url(base_url, canonical, job_id).map_err(CloudErrorDetail::generic)?;
    let first = Duration::from_millis(
        std::env::var("ANYAGENTS_CLOUD_POLL_INTERVAL_MS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(DEFAULT_INITIAL_POLL_INTERVAL_MS)
            .clamp(10, 5_000),
    );
    let max_interval = MAX_POLL_INTERVAL.max(first);
    let deadline = Instant::now() + first.saturating_mul(POLL_BUDGET);
    let mut interval = first;
    let mut transient_errors = 0u32;

    while Instant::now() < deadline {
        let wait_for = apply_poll_jitter(interval);
        if cancel_flag.wait(wait_for) {
            let _ = cancel_cloud_turn(base_url, canonical, auth_token, job_id);
            return Err(CloudErrorDetail::generic(
                "cancelled by supervisor interrupt",
            ));
        }
        interval = (interval * 2).min(max_interval);

        let mut req = agent.get(&status_url);
        if let Some(tok) = auth_token {
            req = req.header("authorization", &format!("Bearer {tok}"));
            if canonical == "claude-cloud" {
                req = req.header("x-api-key", tok);
            }
        }
        if canonical == "claude-cloud" {
            req = req
                .header("anthropic-version", "2023-06-01")
                .header("anthropic-beta", "mcp-client-2025-04-04");
        }
        let resp = match req.call() {
            Ok(resp) => resp,
            Err(e) => {
                transient_errors += 1;
                if transient_errors <= MAX_TRANSIENT_POLL_ERRORS {
                    continue;
                }
                return Err(CloudErrorDetail::generic(format!(
                    "cloud job poll to {status_url} failed: {e}"
                )));
            }
        };
        let status_code = resp.status().as_u16();
        let retry_after = resp
            .headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let mut body = String::new();
        if let Err(e) = resp
            .into_body()
            .into_reader()
            .take(512 * 1024)
            .read_to_string(&mut body)
        {
            transient_errors += 1;
            if transient_errors <= MAX_TRANSIENT_POLL_ERRORS {
                continue;
            }
            return Err(CloudErrorDetail::generic(format!(
                "failed reading poll body from {status_url}: {e}"
            )));
        }
        if (500..=599).contains(&status_code) {
            transient_errors += 1;
            if transient_errors <= MAX_TRANSIENT_POLL_ERRORS {
                continue;
            }
            return Err(classify_http_error(
                status_code,
                retry_after.as_deref(),
                &body,
                canonical,
                &status_url,
            ));
        }
        if !(200..300).contains(&status_code) {
            return Err(classify_http_error(
                status_code,
                retry_after.as_deref(),
                &body,
                canonical,
                &status_url,
            ));
        }
        transient_errors = 0;

        let poll_val: Value = serde_json::from_str(&body).map_err(|e| {
            CloudErrorDetail::generic(format!(
                "invalid JSON from cloud job poll {status_url}: {e}"
            ))
        })?;
        let Some(raw_st) = poll_val.get("status").and_then(Value::as_str) else {
            return Err(CloudErrorDetail::generic(format!(
                "cloud job poll `{canonical}` returned response missing status"
            )));
        };
        let st = raw_st.trim().to_ascii_lowercase();
        match st.as_str() {
            "queued" | "running" | "in_progress" | "pending" | "submitted" | "working"
            | "input_required" => continue,
            "completed" | "succeeded" | "done" => return Ok(poll_val),
            "failed" | "cancelled" | "canceled" | "error" => {
                let msg = poll_val
                    .get("error")
                    .and_then(|e| {
                        e.get("message")
                            .and_then(Value::as_str)
                            .or_else(|| e.as_str())
                    })
                    .or_else(|| poll_val.get("result").and_then(Value::as_str))
                    .unwrap_or("cloud job failed");
                return Err(CloudErrorDetail::generic(format!(
                    "cloud job {job_id} {st}: {msg}"
                )));
            }
            other => {
                return Err(CloudErrorDetail::generic(format!(
                    "cloud job {job_id} returned unknown status `{other}`"
                )));
            }
        }
    }
    Err(CloudErrorDetail::generic(format!(
        "cloud job {job_id} timed out waiting for completion"
    )))
}

struct SseAccumulator {
    canonical: String,
    fallback_session_id: Option<String>,
    current_event: String,
    data_lines: Vec<String>,
    resolved_sid: Option<String>,
    resolved_run_id: Option<String>,
    delta_chunks: String,
    full_reply: Option<String>,
    input_tokens: u64,
    output_tokens: u64,
    error: Option<CloudErrorDetail>,
    saw_done: bool,
}

impl SseAccumulator {
    fn new(canonical: &str, fallback_session_id: Option<&str>) -> Self {
        Self {
            canonical: canonical.to_string(),
            fallback_session_id: fallback_session_id.map(str::to_string),
            current_event: String::new(),
            data_lines: Vec::new(),
            resolved_sid: None,
            resolved_run_id: None,
            delta_chunks: String::new(),
            full_reply: None,
            input_tokens: 0,
            output_tokens: 0,
            error: None,
            saw_done: false,
        }
    }

    fn feed_line(&mut self, raw_line: &str) {
        let line = raw_line.trim_end_matches(['\r', '\n']);
        if line.is_empty() {
            self.flush_event();
            return;
        }
        if line.starts_with(':') {
            // SSE comment/heartbeat keeps the stream alive without ending the event.
            return;
        }
        if let Some(ev) = line.strip_prefix("event:") {
            self.current_event = ev.trim().to_string();
        } else if let Some(data_str) = line.strip_prefix("data:") {
            let trimmed = data_str.strip_prefix(' ').unwrap_or(data_str);
            self.data_lines.push(trimmed.to_string());
        }
    }

    fn flush_event(&mut self) {
        let event_name = std::mem::take(&mut self.current_event);
        if self.data_lines.is_empty() {
            return;
        }
        let joined = std::mem::take(&mut self.data_lines).join("\n");
        let data_trimmed = joined.trim();
        if data_trimmed.is_empty() {
            return;
        }
        if data_trimmed == "[DONE]" {
            self.saw_done = true;
            return;
        }
        let Ok(val) = serde_json::from_str::<Value>(data_trimmed) else {
            return;
        };

        if let Some(rid) = extract_cloud_run_id(&val) {
            self.resolved_run_id = Some(rid);
        }
        if let Some(sid) = extract_cloud_session_id(&val, &self.canonical).or_else(|| {
            val.pointer("/message/id")
                .or_else(|| val.pointer("/response/id"))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        }) {
            self.resolved_sid = Some(sid);
        }

        let ev_type = val
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or(event_name.as_str());

        match ev_type {
            "error" | "run.failed" | "response.failed" => {
                let status = val
                    .get("status")
                    .or_else(|| val.get("apiErrorStatus"))
                    .or_else(|| val.pointer("/error/status"))
                    .and_then(Value::as_u64)
                    .and_then(|n| u16::try_from(n).ok());
                let msg = val
                    .pointer("/error/message")
                    .and_then(Value::as_str)
                    .or_else(|| val.get("message").and_then(Value::as_str))
                    .or_else(|| val.get("error").and_then(Value::as_str))
                    .unwrap_or(data_trimmed);
                let detail = if let Some(code_status) = status {
                    classify_http_error(code_status, None, data_trimmed, &self.canonical, "sse")
                } else if msg.to_ascii_lowercase().contains("rate_limit") || msg.contains("429") {
                    classify_http_error(429, None, data_trimmed, &self.canonical, "sse")
                } else if msg.contains("401") || msg.to_ascii_lowercase().contains("unauthoriz") {
                    classify_http_error(401, None, data_trimmed, &self.canonical, "sse")
                } else {
                    CloudErrorDetail::generic(msg.to_string())
                };
                self.error = Some(detail);
            }
            "rate_limit" => {
                self.error = Some(classify_http_error(
                    429,
                    None,
                    data_trimmed,
                    &self.canonical,
                    "sse",
                ));
            }
            "content_block_delta" => {
                if let Some(t) = val.pointer("/delta/text").and_then(Value::as_str) {
                    self.delta_chunks.push_str(t);
                }
            }
            "response.output_text.delta" | "content.delta" | "text.delta" | "message.delta" => {
                if let Some(t) = val.pointer("/delta/text").and_then(Value::as_str) {
                    self.delta_chunks.push_str(t);
                } else if let Some(t) = val.get("delta").and_then(Value::as_str) {
                    self.delta_chunks.push_str(t);
                } else if let Some(t) = val.get("text").and_then(Value::as_str) {
                    self.delta_chunks.push_str(t);
                }
            }
            _ => {
                if let Some(t) = val.pointer("/delta/text").and_then(Value::as_str) {
                    self.delta_chunks.push_str(t);
                } else if let Some(t) = val.get("delta").and_then(Value::as_str) {
                    self.delta_chunks.push_str(t);
                } else if let Some(t) = val.get("text").and_then(Value::as_str) {
                    self.delta_chunks.push_str(t);
                }
                for ptr in [
                    "/result",
                    "/output_text",
                    "/reply",
                    "/output/text",
                    "/message/text",
                ] {
                    if let Some(r) = val.pointer(ptr).and_then(Value::as_str)
                        && !r.trim().is_empty()
                    {
                        self.full_reply = Some(r.to_string());
                    }
                }
            }
        }

        for ptr in [
            "/usage/input_tokens",
            "/message/usage/input_tokens",
            "/response/usage/input_tokens",
        ] {
            if let Some(tok) = val.pointer(ptr).and_then(Value::as_u64) {
                self.input_tokens = tok;
            }
        }
        for ptr in [
            "/usage/output_tokens",
            "/message/usage/output_tokens",
            "/response/usage/output_tokens",
        ] {
            if let Some(tok) = val.pointer(ptr).and_then(Value::as_u64) {
                self.output_tokens = tok;
            }
        }
    }

    fn finish(mut self) -> Result<CloudTurnOutcome, CloudErrorDetail> {
        self.flush_event();
        if let Some(err) = self.error {
            return Err(err);
        }
        let reply_text = if !self.delta_chunks.is_empty() {
            self.delta_chunks
        } else if let Some(r) = self.full_reply {
            r
        } else {
            return Err(CloudErrorDetail::generic(format!(
                "cloud harness `{}` SSE stream ended without reply content",
                self.canonical
            )));
        };

        let session_id = self
            .resolved_sid
            .or(self.fallback_session_id)
            .unwrap_or_else(|| format!("{}-session-1", self.canonical));

        Ok(CloudTurnOutcome {
            session_id,
            reply_text,
            input_tokens: self.input_tokens,
            output_tokens: self.output_tokens,
        })
    }
}

pub(super) fn read_sse_cloud_stream<R: BufRead + Send + 'static>(
    reader: R,
    canonical: &str,
    fallback_session_id: Option<&str>,
    idle_timeout: Duration,
    cancel_flag: &TurnCancel,
) -> Result<CloudTurnOutcome, CloudErrorDetail> {
    let (tx, rx) = mpsc::sync_channel::<Result<Option<String>, String>>(64);
    std::thread::spawn(move || {
        let mut r = reader;
        let mut line = String::new();
        loop {
            line.clear();
            match r.read_line(&mut line) {
                Ok(0) => {
                    let _ = tx.send(Ok(None));
                    return;
                }
                Ok(_) => {
                    if tx.send(Ok(Some(line.clone()))).is_err() {
                        return;
                    }
                }
                Err(e) => {
                    let _ = tx.send(Err(e.to_string()));
                    return;
                }
            }
        }
    });

    let mut acc = SseAccumulator::new(canonical, fallback_session_id);
    let mut last_activity = Instant::now();
    loop {
        if cancel_flag.is_cancelled() {
            return Err(CloudErrorDetail::generic(
                "cancelled by supervisor interrupt",
            ));
        }
        match rx.recv_timeout(Duration::from_millis(50)) {
            Ok(Ok(Some(line))) => {
                last_activity = Instant::now();
                acc.feed_line(&line);
                if acc.error.is_some() || acc.saw_done {
                    break;
                }
            }
            Ok(Ok(None)) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
            Ok(Err(e)) => {
                return Err(CloudErrorDetail::generic(format!(
                    "failed reading SSE stream from `{canonical}`: {e}"
                )));
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if last_activity.elapsed() >= idle_timeout {
                    return Err(CloudErrorDetail::generic(format!(
                        "cloud harness `{canonical}` SSE stream idle timeout after {}s",
                        idle_timeout.as_secs()
                    )));
                }
            }
        }
    }
    acc.finish()
}

pub fn parse_sse_cloud_response(
    raw_sse: &str,
    canonical: &str,
    fallback_session_id: Option<&str>,
) -> Result<CloudTurnOutcome, String> {
    let mut acc = SseAccumulator::new(canonical, fallback_session_id);
    for line in raw_sse.lines() {
        acc.feed_line(line);
        if acc.error.is_some() || acc.saw_done {
            break;
        }
    }
    acc.finish().map_err(|d| d.message)
}

fn extract_cloud_reply_text(parsed: &Value) -> Result<String, String> {
    for key in ["result", "output_text", "reply", "text", "response"] {
        if let Some(s) = parsed.get(key).and_then(Value::as_str)
            && !s.trim().is_empty()
        {
            return Ok(s.to_string());
        }
    }
    if let Some(content) = parsed.get("content").and_then(Value::as_array) {
        let mut texts = Vec::new();
        for item in content {
            if let Some(t) = item.get("text").and_then(Value::as_str)
                && !t.is_empty()
            {
                texts.push(t);
            }
        }
        if !texts.is_empty() {
            return Ok(texts.join("\n"));
        }
    }
    if let Some(output) = parsed.get("output").and_then(Value::as_array) {
        let mut texts = Vec::new();
        for item in output {
            if let Some(t) = item.get("text").and_then(Value::as_str)
                && !t.is_empty()
            {
                texts.push(t);
            }
            if let Some(content) = item.get("content").and_then(Value::as_array) {
                for part in content {
                    if let Some(t) = part.get("text").and_then(Value::as_str)
                        && !t.is_empty()
                    {
                        texts.push(t);
                    }
                }
            }
        }
        if !texts.is_empty() {
            return Ok(texts.join("\n"));
        }
    }
    if let Some(s) = parsed.pointer("/message/text").and_then(Value::as_str)
        && !s.trim().is_empty()
    {
        return Ok(s.to_string());
    }
    Err("cloud harness response did not include any reply text".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Router,
        http::{HeaderMap, StatusCode},
        routing::post,
    };
    use std::sync::atomic::Ordering;
    use std::sync::{Arc, Mutex};

    #[test]
    fn cloud_prompt_discovers_synced_skills_and_prefers_direct_slugs() {
        let dir = crate::test_tempdir();
        let config = dir.path().join("config");
        let view = dir.path().join("skills-view");
        std::fs::create_dir_all(&config).unwrap();
        for (relative, description) in [
            ("synced/z/bucketed", "later bucket"),
            ("synced/a/bucketed", "synced trigger"),
            ("synced/a/collision", "must not win"),
            ("collision", "direct trigger"),
            ("synced/.bucket-temp/hidden", "hidden bucket"),
            ("synced/a/.hidden", "hidden skill"),
        ] {
            let path = view.join(relative);
            std::fs::create_dir_all(&path).unwrap();
            std::fs::write(
                path.join("SKILL.md"),
                format!("---\ndescription: {description}\n---\nBody"),
            )
            .unwrap();
        }
        std::fs::write(view.join("synced/manifest.json"), "{}").unwrap();
        std::fs::write(view.join("synced/.last-complete-round"), "1").unwrap();
        let prompt = build_cloud_system_prompt(&config).unwrap();
        assert!(prompt.contains("<available_skills>"));
        assert!(prompt.contains("- `bucketed`: synced trigger"));
        assert!(prompt.contains("- `collision`: direct trigger"));
        assert!(!prompt.contains("must not win"));
        assert!(!prompt.contains("later bucket"));
        assert!(!prompt.contains("hidden"));
        assert!(!prompt.contains("manifest.json"));
    }

    #[tokio::test]
    async fn cloud_harness_dispatches_spiron_claude_openai_and_cursor_turns_with_mcp_capability() {
        #[derive(Default)]
        struct RecordedCalls {
            claude: Mutex<Vec<(String, Value)>>,
            openai: Mutex<Vec<Value>>,
            cursor: Mutex<Vec<Value>>,
        }

        let recorded = Arc::new(RecordedCalls::default());
        let app = Router::new()
            .route(
                "/v1/messages",
                post({
                    let rec = Arc::clone(&recorded);
                    move |headers: HeaderMap, axum::Json(payload): axum::Json<Value>| {
                        let rec = Arc::clone(&rec);
                        async move {
                            let beta = headers
                                .get("anthropic-beta")
                                .and_then(|v| v.to_str().ok())
                                .unwrap_or("")
                                .to_string();
                            rec.claude.lock().unwrap().push((beta, payload));
                            (
                                StatusCode::OK,
                                axum::Json(json!({
                                    "id": "msg_claude_77",
                                    "content": [{ "type": "text", "text": "claude cloud turn ok" }],
                                    "usage": { "input_tokens": 15, "output_tokens": 25 }
                                })),
                            )
                        }
                    }
                }),
            )
            .route(
                "/v1/responses",
                post({
                    let rec = Arc::clone(&recorded);
                    move |axum::Json(payload): axum::Json<Value>| {
                        let rec = Arc::clone(&rec);
                        async move {
                            rec.openai.lock().unwrap().push(payload);
                            (
                                StatusCode::OK,
                                axum::Json(json!({
                                    "id": "resp_openai_88",
                                    "output_text": "openai cloud turn ok",
                                    "usage": { "input_tokens": 19, "output_tokens": 29 }
                                })),
                            )
                        }
                    }
                }),
            )
            .route(
                "/v0/agents",
                post({
                    let rec = Arc::clone(&recorded);
                    move |axum::Json(payload): axum::Json<Value>| {
                        let rec = Arc::clone(&rec);
                        async move {
                            rec.cursor.lock().unwrap().push(payload);
                            (
                                StatusCode::OK,
                                axum::Json(json!({
                                    "session_id": "cursor-sess-66",
                                    "reply": "cursor cloud turn ok"
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
        let cfg = dir.path().join("agent-cfg");
        std::fs::create_dir_all(&cfg).unwrap();
        // The local capability is present and must never reach a vendor: the
        // adapter presents the server-issued remote session token.
        std::fs::write(cfg.join("mcp-capability"), "cap-local-never-sent\n").unwrap();
        let remote = format!("aars_{}", "a".repeat(64));
        std::fs::write(cfg.join(crate::remote::TOKEN_FILE), format!("{remote}\n")).unwrap();
        std::fs::write(
            cfg.join(".anyagents-mcp-url"),
            "https://mcp.example.test/mcp\n",
        )
        .unwrap();
        std::fs::write(
            cfg.join("SYSTEM_PROMPT.md"),
            "# Agent Identity\nYou are test-agent.\n\n<learned_memory>\n- `deploy-rule`: Always verify before claiming.\n</learned_memory>\n",
        )
        .unwrap();
        let skill_dir = cfg.join(".agents/skills/prove-it");
        std::fs::create_dir_all(&skill_dir).unwrap();
        std::fs::write(
            skill_dir.join("SKILL.md"),
            "---\nname: prove-it\ndescription: Use when verifying a live status or root-cause claim.\n---\n# Prove It\nFull instructions here.\n",
        )
        .unwrap();
        std::fs::write(
            cfg.join("cloud.json"),
            serde_json::to_vec(&json!({
                "base_url": format!("http://{addr}"),
                "api_key": "upstream-api-key",
                "disabled_tools": ["bash", "write"]
            }))
            .unwrap(),
        )
        .unwrap();

        // 2. claude-cloud
        let cfg_clone = cfg.clone();
        let claude_out = tokio::task::spawn_blocking(move || {
            dispatch_cloud_turn(
                &cfg_clone,
                "claude-cloud",
                Some("claude-sonnet-4-6"),
                None,
                "ping claude",
            )
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(claude_out.session_id, "msg_claude_77");
        assert_eq!(claude_out.reply_text, "claude cloud turn ok");
        {
            let calls = recorded.claude.lock().unwrap();
            assert_eq!(calls.len(), 1);
            assert_eq!(calls[0].0, "mcp-client-2025-04-04");
            assert_eq!(
                calls[0].1["mcp_servers"][0]["url"],
                "https://mcp.example.test/mcp?profile=remote"
            );
            assert_eq!(
                calls[0].1["mcp_servers"][0]["authorization_token"],
                remote.as_str()
            );
            let sys = calls[0].1["system"].as_str().unwrap();
            assert!(sys.contains("<learned_memory>"), "{sys}");
            assert!(sys.contains("<available_skills>"), "{sys}");
            assert!(sys.contains("prove-it"), "{sys}");
            assert_eq!(
                calls[0].1["toolset_configs"],
                json!([
                    {"name": "bash", "enabled": false},
                    {"name": "write", "enabled": false}
                ])
            );
        }

        // 3. openai-cloud
        let cfg_clone = cfg.clone();
        let openai_out = tokio::task::spawn_blocking(move || {
            dispatch_cloud_turn(
                &cfg_clone,
                "openai-cloud",
                Some("gpt-5.4"),
                Some("resp_prev_1"),
                "ping openai",
            )
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(openai_out.session_id, "resp_openai_88");
        assert_eq!(openai_out.reply_text, "openai cloud turn ok");
        {
            let calls = recorded.openai.lock().unwrap();
            assert_eq!(calls.len(), 1);
            assert_eq!(calls[0]["previous_response_id"], "resp_prev_1");
            assert_eq!(calls[0]["tools"][0]["type"], "mcp");
            assert_eq!(
                calls[0]["tools"][0]["server_url"],
                "https://mcp.example.test/mcp?profile=remote"
            );
            assert_eq!(
                calls[0]["tools"][0]["headers"]["Authorization"],
                format!("Bearer {remote}").as_str()
            );
            let sys = calls[0]["instructions"].as_str().unwrap();
            assert!(sys.contains("<learned_memory>"), "{sys}");
            assert!(sys.contains("<available_skills>"), "{sys}");
            assert!(sys.contains("prove-it"), "{sys}");
        }

        // 4. cursor-cloud
        let cfg_clone = cfg.clone();
        let cursor_out = tokio::task::spawn_blocking(move || {
            dispatch_cloud_turn(
                &cfg_clone,
                "cursor-cloud",
                Some("claude-4.6-opus"),
                None,
                "ping cursor",
            )
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(cursor_out.session_id, "cursor-sess-66");
        assert_eq!(cursor_out.reply_text, "cursor cloud turn ok");
        {
            let calls = recorded.cursor.lock().unwrap();
            assert_eq!(calls.len(), 1);
            assert_eq!(calls[0]["prompt"]["text"], "ping cursor");
            assert_eq!(
                calls[0]["mcp_servers"][0]["url"],
                "https://mcp.example.test/mcp?profile=remote"
            );
            assert_eq!(calls[0]["mcp_servers"][0]["bearer_token"], remote.as_str());
            let sys = calls[0]["system_prompt"].as_str().unwrap();
            assert!(sys.contains("<learned_memory>"), "{sys}");
            assert!(sys.contains("<available_skills>"), "{sys}");
            assert!(sys.contains("prove-it"), "{sys}");
            assert_eq!(
                calls[0]["config"]["permissions"]["deny"],
                json!(["bash", "write"])
            );
        }

        server_handle.abort();
    }

    #[tokio::test]
    async fn cloud_harness_supports_sse_streaming_async_job_polling_and_interrupt_cancel() {
        use axum::routing::get;
        use std::sync::atomic::AtomicUsize;

        let poll_count = Arc::new(AtomicUsize::new(0));
        let cancel_recorded = Arc::new(Mutex::new(Vec::<String>::new()));

        let app = Router::new()
            // 1. Anthropic SSE stream on `/v1/messages`
            .route(
                "/v1/messages",
                post(|| async move {
                    let sse = concat!(
                        "event: message_start\n",
                        "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_sse_101\",\"usage\":{\"input_tokens\":14}}}\n\n",
                        "event: content_block_delta\n",
                        "data: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"Hello \"}}\n\n",
                        "event: content_block_delta\n",
                        "data: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"SSE World\"}}\n\n",
                        "event: message_delta\n",
                        "data: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":28}}\n\n"
                    );
                    (
                        StatusCode::OK,
                        [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
                        sse,
                    )
                }),
            )
            // 2. OpenAI SSE stream on `/v1/responses`
            .route(
                "/v1/responses",
                post(|| async move {
                    let sse = concat!(
                        "event: response.created\n",
                        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_sse_202\"}}\n\n",
                        "event: response.output_text.delta\n",
                        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"OpenAI \"}\n\n",
                        "event: response.output_text.delta\n",
                        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"Stream OK\"}\n\n",
                        "event: response.completed\n",
                        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_sse_202\",\"usage\":{\"input_tokens\":9,\"output_tokens\":18}}}\n\n",
                        "data: [DONE]\n\n"
                    );
                    (
                        StatusCode::OK,
                        [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
                        sse,
                    )
                }),
            )
            // 3. Cursor async job polling (`queued` -> `running` -> `completed`) & cancel (`POST /v0/agents/{id}/cancel`)
            .route(
                "/v0/agents",
                post(|axum::Json(payload): axum::Json<Value>| async move {
                    let prompt = payload.pointer("/prompt/text").and_then(Value::as_str).unwrap_or("");
                    if prompt.contains("slow-cancel") {
                        (
                            StatusCode::ACCEPTED,
                            axum::Json(json!({
                                "id": "run_cancel_55",
                                "status": "running"
                            })),
                        )
                    } else {
                        (
                            StatusCode::ACCEPTED,
                            axum::Json(json!({
                                "id": "run_poll_44",
                                "status": "queued"
                            })),
                        )
                    }
                }),
            )
            .route(
                "/v0/agents/run_poll_44",
                get({
                    let pc = Arc::clone(&poll_count);
                    move || {
                        let pc = Arc::clone(&pc);
                        async move {
                            let step = pc.fetch_add(1, Ordering::SeqCst);
                            if step == 0 {
                                (
                                    StatusCode::OK,
                                    axum::Json(json!({
                                        "id": "run_poll_44",
                                        "status": "running"
                                    })),
                                )
                            } else {
                                (
                                    StatusCode::OK,
                                    axum::Json(json!({
                                        "id": "run_poll_44",
                                        "session_id": "spiron-polled-sess-44",
                                        "status": "completed",
                                        "result": "async polled result ok",
                                        "usage": { "input_tokens": 33, "output_tokens": 44 }
                                    })),
                                )
                            }
                        }
                    }
                }),
            )
            .route(
                "/v0/agents/run_cancel_55",
                get(|| async move {
                    (
                        StatusCode::OK,
                        axum::Json(json!({
                            "id": "run_cancel_55",
                            "status": "running"
                        })),
                    )
                }),
            )
            .route(
                "/v0/agents/run_cancel_55/cancel",
                post({
                    let cr = Arc::clone(&cancel_recorded);
                    move || {
                        let cr = Arc::clone(&cr);
                        async move {
                            cr.lock().unwrap().push("run_cancel_55".into());
                            (
                                StatusCode::OK,
                                axum::Json(json!({
                                    "id": "run_cancel_55",
                                    "status": "cancelled"
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
        let cfg = dir.path().join("agent-cfg");
        std::fs::create_dir_all(&cfg).unwrap();
        std::fs::write(
            cfg.join(crate::remote::TOKEN_FILE),
            format!("aars_{}\n", "b".repeat(64)),
        )
        .unwrap();
        std::fs::write(
            cfg.join(".anyagents-mcp-url"),
            "https://mcp.example.test/mcp\n",
        )
        .unwrap();
        std::fs::write(
            cfg.join("cloud.json"),
            serde_json::to_vec(&json!({
                "base_url": format!("http://{addr}"),
                "api_key": "upstream-key"
            }))
            .unwrap(),
        )
        .unwrap();

        // 1. Verify Anthropic SSE streaming
        let cfg_clone = cfg.clone();
        let claude_sse = tokio::task::spawn_blocking(move || {
            dispatch_cloud_turn(&cfg_clone, "claude-cloud", None, None, "sse test").unwrap()
        })
        .await
        .unwrap();
        assert_eq!(claude_sse.session_id, "msg_sse_101");
        assert_eq!(claude_sse.reply_text, "Hello SSE World");
        assert_eq!(claude_sse.input_tokens, 14);
        assert_eq!(claude_sse.output_tokens, 28);

        // 2. Verify OpenAI Responses SSE streaming
        let cfg_clone = cfg.clone();
        let openai_sse = tokio::task::spawn_blocking(move || {
            dispatch_cloud_turn(&cfg_clone, "openai-cloud", None, None, "sse test").unwrap()
        })
        .await
        .unwrap();
        assert_eq!(openai_sse.session_id, "resp_sse_202");
        assert_eq!(openai_sse.reply_text, "OpenAI Stream OK");
        assert_eq!(openai_sse.input_tokens, 9);
        assert_eq!(openai_sse.output_tokens, 18);

        // 3. Verify Async Job Polling (`queued` -> `running` -> `completed`)
        let cfg_clone = cfg.clone();
        let polled = tokio::task::spawn_blocking(move || {
            dispatch_cloud_turn(&cfg_clone, "cursor-cloud", None, None, "poll test").unwrap()
        })
        .await
        .unwrap();
        assert_eq!(polled.session_id, "spiron-polled-sess-44");
        assert_eq!(polled.reply_text, "async polled result ok");
        assert_eq!(polled.input_tokens, 33);
        assert_eq!(polled.output_tokens, 44);
        assert!(poll_count.load(Ordering::SeqCst) >= 2);

        // 4. Verify concurrent `control_request` `interrupt` mid-turn cancellation
        #[derive(Clone, Default)]
        struct SharedBuf(Arc<Mutex<Vec<u8>>>);
        impl Write for SharedBuf {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        struct DelayedInterruptReader {
            step: usize,
        }
        impl std::io::Read for DelayedInterruptReader {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                let line = match self.step {
                    0 => {
                        self.step = 1;
                        b"{\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"slow-cancel task\"}]}}\n" as &[u8]
                    }
                    1 => {
                        self.step = 2;
                        std::thread::sleep(Duration::from_millis(80));
                        b"{\"type\":\"control_request\",\"request_id\":\"1\",\"request\":{\"subtype\":\"interrupt\"}}\n" as &[u8]
                    }
                    _ => return Ok(0),
                };
                let n = line.len().min(buf.len());
                buf[..n].copy_from_slice(&line[..n]);
                Ok(n)
            }
        }

        let out_buf = SharedBuf::default();
        let out_clone = out_buf.clone();
        let cfg_clone = cfg.clone();
        tokio::task::spawn_blocking(move || {
            run_with_io(
                CloudHarnessArgs {
                    config: Some(cfg_clone),
                    harness: Some("cursor-cloud".into()),
                    model: None,
                    resume: None,
                },
                BufReader::new(DelayedInterruptReader { step: 0 }),
                out_clone,
            )
            .unwrap();
        })
        .await
        .unwrap();

        let recorded_cancels = cancel_recorded.lock().unwrap().clone();
        assert!(
            recorded_cancels.contains(&"run_cancel_55".to_string()),
            "expected POST /v0/agents/run_cancel_55/cancel to be invoked, got {recorded_cancels:?}"
        );
        let stdout_str = String::from_utf8(out_buf.0.lock().unwrap().clone()).unwrap();
        assert!(
            stdout_str.contains("\"is_error\":true") && stdout_str.contains("cancelled"),
            "expected error result event on interrupt, got: {stdout_str}"
        );

        server_handle.abort();
    }
    #[test]
    fn spiron_cloud_defaults_to_api_spiron_ai_and_reads_spiron_credentials() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = tmp.path();
        std::fs::create_dir_all(cfg.join(".spiron")).unwrap();
        std::fs::write(
            cfg.join(".spiron/auth.json"),
            r#"{"access_token":"sp_tok_abc123"}"#,
        )
        .unwrap();
        let url = resolve_cloud_base_url(cfg, "spiron-cloud");
        if std::env::var("ANYAGENTS_SPIRON_CLOUD_URL").is_err()
            && std::env::var("SPIRON_CLOUD_URL").is_err()
        {
            assert_eq!(url, "https://api.spiron.ai");
        }
        let tok = resolve_cloud_auth_token(cfg, "spiron-cloud");
        if std::env::var("ANYAGENTS_SPIRON_API_KEY").is_err()
            && std::env::var("SPIRON_API_KEY").is_err()
            && std::env::var("SYLPHX_API_KEY").is_err()
        {
            assert_eq!(tok.as_deref(), Some("sp_tok_abc123"));
        }
    }

    /// A job status endpoint that reports `running` until `done_after` has
    /// passed, counts its polls, and records a cancel.
    fn job_server(done_after: Option<Duration>) -> (String, Arc<Mutex<(usize, bool)>>) {
        use std::io::{BufRead as _, Read as _};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let seen = Arc::new(Mutex::new((0usize, false)));
        let recorded = Arc::clone(&seen);
        let started = Instant::now();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { return };
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut request_line = String::new();
                let _ = reader.read_line(&mut request_line);
                let mut length = 0usize;
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
                        break;
                    }
                    if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                        length = value.trim().parse().unwrap_or(0);
                    }
                }
                let mut body = vec![0u8; length];
                let _ = reader.read_exact(&mut body);
                let answer = if request_line.contains("/cancel") {
                    recorded.lock().unwrap().1 = true;
                    json!({"status": "cancelled"})
                } else {
                    recorded.lock().unwrap().0 += 1;
                    if done_after.is_some_and(|after| started.elapsed() >= after) {
                        json!({"id": "run_1", "status": "completed", "result": "done"})
                    } else {
                        json!({"id": "run_1", "status": "running"})
                    }
                }
                .to_string();
                let _ = write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{answer}",
                    answer.len()
                );
            }
        });
        (base, seen)
    }

    fn poll_agent() -> ureq::Agent {
        ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(10)))
            .http_status_as_error(false)
            .build()
            .into()
    }

    #[test]
    fn a_long_job_is_polled_less_often_as_it_runs() {
        let (base, seen) = job_server(Some(Duration::from_millis(1_500)));
        let done = poll_cloud_job_until_done(
            &poll_agent(),
            &base,
            "cursor-cloud",
            None,
            "run_1",
            &TurnCancel::default(),
        )
        .unwrap();
        assert_eq!(done["result"], "done");
        // A fixed 50 ms interval asks about thirty times in 1.5 s.
        let polls = seen.lock().unwrap().0;
        assert!((2..=10).contains(&polls), "polled {polls} times");
    }

    #[test]
    fn a_cloud_turn_never_falls_back_to_the_local_capability_or_a_loopback_url() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = tmp.path();
        std::fs::write(cfg.join("mcp-capability"), "cap-local\n").unwrap();
        let err = dispatch_cloud_turn(cfg, "claude-cloud", None, None, "hi").unwrap_err();
        assert!(err.contains("remote session token"), "{err}");

        std::fs::write(
            cfg.join(crate::remote::TOKEN_FILE),
            format!("aars_{}\n", "c".repeat(64)),
        )
        .unwrap();
        if std::env::var_os("ANYAGENTS_PUBLIC_MCP_URL").is_none() {
            let err = resolve_public_mcp_url(cfg).unwrap_err();
            assert!(err.contains("public_url"), "{err}");
            let err = dispatch_cloud_turn(cfg, "claude-cloud", None, None, "hi").unwrap_err();
            assert!(err.contains("public_url"), "{err}");
        }

        // A file that does not hold a remote session token is refused.
        std::fs::write(cfg.join(crate::remote::TOKEN_FILE), "cap-local\n").unwrap();
        assert!(crate::remote::session_token(cfg).is_err());
    }

    #[test]
    fn a_remote_session_keeps_unreplaced_vendor_tools() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = tmp.path();
        std::fs::write(cfg.join("cloud.json"), r#"{"disable_builtin_tools":true}"#).unwrap();
        assert_eq!(resolve_cloud_disabled_tools(cfg), vec!["memory", "skills"]);
    }

    #[test]
    fn an_interrupt_ends_the_wait_between_polls_at_once() {
        let (base, seen) = job_server(None);
        let cancel = Arc::new(TurnCancel::default());
        let interrupt = Arc::clone(&cancel);
        let interrupter = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(1_200));
            interrupt.cancel();
            Instant::now()
        });
        let error =
            poll_cloud_job_until_done(&poll_agent(), &base, "cursor-cloud", None, "run_1", &cancel)
                .unwrap_err();
        let ended = Instant::now();
        let interrupted = interrupter.join().unwrap();
        assert!(error.contains("cancelled"), "{error}");
        // The wait between polls is a second by now; the interrupt cuts it.
        assert!(ended.duration_since(interrupted) < Duration::from_millis(500));
        assert!(seen.lock().unwrap().1, "the remote job was not cancelled");
    }

    #[test]
    fn resolves_spiron_api_url_and_rejects_only_sylphx_gateway_key() {
        let _env = crate::server::tests::env_lock();
        struct EnvGuard(Vec<(&'static str, Option<std::ffi::OsString>)>);
        impl Drop for EnvGuard {
            fn drop(&mut self) {
                for (k, v) in self.0.drain(..) {
                    unsafe {
                        match v {
                            Some(val) => std::env::set_var(k, val),
                            None => std::env::remove_var(k),
                        }
                    }
                }
            }
        }
        let keys = [
            "ANYAGENTS_SPIRON_CLOUD_URL",
            "SPIRON_API_URL",
            "SPIRON_CLOUD_URL",
            "ANYAGENTS_SPIRON_API_KEY",
            "SPIRON_API_KEY",
            "SPIRON_TOKEN",
            "SYLPHX_API_KEY",
        ];
        let _guard = EnvGuard(keys.into_iter().map(|k| (k, std::env::var_os(k))).collect());
        for k in keys {
            unsafe {
                std::env::remove_var(k);
            }
        }
        let tmp = tempfile::tempdir().unwrap();
        let cfg = tmp.path();
        assert_eq!(
            resolve_cloud_base_url(cfg, "spiron-cloud"),
            "https://api.spiron.ai"
        );

        unsafe {
            std::env::set_var("SPIRON_API_URL", "https://custom.spiron.example/");
        }
        assert_eq!(
            resolve_cloud_base_url(cfg, "spiron-cloud"),
            "https://custom.spiron.example"
        );

        std::fs::create_dir_all(cfg.join(".config/sylphx")).unwrap();
        std::fs::write(
            cfg.join(".config/sylphx/auth.json"),
            r#"{"access_token":"sylphx_sk_gateway_only"}"#,
        )
        .unwrap();
        let err = resolve_cloud_auth_token_checked(cfg, "spiron-cloud").unwrap_err();
        assert!(
            err.contains("401") && err.contains("sylphx_sk_") && err.contains("spiron_sk_"),
            "{err}"
        );

        std::fs::create_dir_all(cfg.join(".spiron")).unwrap();
        std::fs::write(cfg.join(".spiron/key"), "spiron_sk_cloud_secret_123\n").unwrap();
        assert_eq!(
            resolve_cloud_auth_token_checked(cfg, "spiron-cloud")
                .unwrap()
                .as_deref(),
            Some("spiron_sk_cloud_secret_123")
        );
    }

    #[test]
    fn remote_cloud_backends_reject_http_loopback_mcp_unless_overridden_and_separate_run_vs_session()
     {
        let _env = crate::server::tests::env_lock();
        let tmp = tempfile::tempdir().unwrap();
        let cfg = tmp.path();
        std::fs::write(
            cfg.join(crate::remote::TOKEN_FILE),
            format!("aars_{}", "d".repeat(64)),
        )
        .unwrap();
        std::fs::write(
            cfg.join(crate::remote::MCP_URL_FILE),
            "http://127.0.0.1:8787/mcp",
        )
        .unwrap();
        for harness in ["claude-cloud", "openai-cloud", "cursor-cloud"] {
            let err = dispatch_cloud_turn(cfg, harness, None, None, "hi").unwrap_err();
            assert!(err.contains("requires HTTPS"), "{harness}: {err}");
        }

        // Verify run_id and session_id are extracted distinctly and empty completed fallback is gone.
        let both = json!({"run_id": "run_abc", "session_id": "sess_xyz", "status": "running"});
        assert_eq!(extract_cloud_run_id(&both).as_deref(), Some("run_abc"));
        assert_eq!(
            extract_cloud_session_id(&both, "cursor-cloud").as_deref(),
            Some("sess_xyz")
        );
        assert!(
            parse_sse_cloud_response("event: ping\ndata: {}\n\n", "claude-cloud", None).is_err()
        );
        assert!(extract_cloud_reply_text(&json!({"status": "completed"})).is_err());
    }

    #[test]
    fn poll_tolerates_transient_5xx_requires_status_and_classifies_429_401_503() {
        use std::io::BufRead as _;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let handle = std::thread::spawn(move || {
            for (idx, stream) in listener.incoming().take(3).enumerate() {
                let mut stream = stream.unwrap();
                let mut r = BufReader::new(stream.try_clone().unwrap());
                let mut line = String::new();
                let _ = r.read_line(&mut line);
                while {
                    let mut h = String::new();
                    r.read_line(&mut h).unwrap_or(0) > 0 && h != "\r\n"
                } {}
                let (code, body) = match idx {
                    0 | 1 => ("503 Service Unavailable", r#"{"error":"transient"}"#),
                    _ => (
                        "200 OK",
                        r#"{"id":"run_5xx","session_id":"sess_5xx","status":"completed","result":"recovered after 503"}"#,
                    ),
                };
                let _ = write!(
                    stream,
                    "HTTP/1.1 {code}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
            }
        });
        let prev_poll = std::env::var_os("ANYAGENTS_CLOUD_POLL_INTERVAL_MS");
        let _env = crate::server::tests::env_lock();
        unsafe {
            std::env::set_var("ANYAGENTS_CLOUD_POLL_INTERVAL_MS", "15");
        }
        let polled = poll_cloud_job_until_done(
            &poll_agent(),
            &base,
            "cursor-cloud",
            None,
            "run_5xx",
            &TurnCancel::default(),
        )
        .unwrap();
        assert_eq!(polled["result"], "recovered after 503");
        unsafe {
            match prev_poll {
                Some(v) => std::env::set_var("ANYAGENTS_CLOUD_POLL_INTERVAL_MS", v),
                None => std::env::remove_var("ANYAGENTS_CLOUD_POLL_INTERVAL_MS"),
            }
        }
        handle.join().unwrap();

        let rate = classify_http_error(
            429,
            Some("42"),
            r#"{"error":"rate_limit_exceeded"}"#,
            "claude-cloud",
            "/v1/messages",
        );
        assert_eq!(rate.code, "rate_limit");
        assert!(rate.quota_reset_at_ms.is_some());
        let mut out = Vec::new();
        emit_structured_cloud_error(&mut out, "sess_1", &rate, None);
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains("\"type\":\"rate_limit\"") && text.contains("\"quota_reset_at_ms\""));

        let unauth = classify_http_error(401, None, "bad key", "claude-cloud", "/v1/messages");
        assert_eq!(unauth.code, "authentication_failed");
        assert_eq!(unauth.status, Some(401));
    }

    #[test]
    fn incremental_sse_stream_enforces_idle_timeout_and_allows_non_blocking_steer_plus_interrupt() {
        let _env = crate::server::tests::env_lock();
        let prev_idle = std::env::var_os("ANYAGENTS_CLOUD_IDLE_TIMEOUT_SECS");
        unsafe {
            std::env::set_var("ANYAGENTS_CLOUD_IDLE_TIMEOUT_SECS", "1");
        }
        assert_eq!(cloud_idle_timeout(), Duration::from_secs(1));

        struct StalledSseReader;
        impl Read for StalledSseReader {
            fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
                std::thread::sleep(Duration::from_millis(1_300));
                Ok(0)
            }
        }
        let err = read_sse_cloud_stream(
            BufReader::new(StalledSseReader),
            "claude-cloud",
            Some("sess_1"),
            Duration::from_millis(120),
            &TurnCancel::default(),
        )
        .unwrap_err();
        assert!(err.message.contains("idle timeout"), "{}", err.message);

        unsafe {
            match prev_idle {
                Some(v) => std::env::set_var("ANYAGENTS_CLOUD_IDLE_TIMEOUT_SECS", v),
                None => std::env::remove_var("ANYAGENTS_CLOUD_IDLE_TIMEOUT_SECS"),
            }
        }

        // Verify non-blocking stdin reads queued mid-turn steers and processes interrupt concurrently.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let base = format!("http://{addr}");
        let stop_srv = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop_srv_clone = Arc::clone(&stop_srv);
        listener.set_nonblocking(true).unwrap();
        let srv = std::thread::spawn(move || {
            while !stop_srv_clone.load(Ordering::SeqCst) {
                let Ok((mut stream, _)) = listener.accept() else {
                    std::thread::sleep(Duration::from_millis(5));
                    continue;
                };
                let _ = stream.set_nonblocking(false);
                let mut r = BufReader::new(stream.try_clone().unwrap());
                let mut req_line = String::new();
                let _ = r.read_line(&mut req_line);
                let mut len = 0usize;
                loop {
                    let mut h = String::new();
                    if r.read_line(&mut h).unwrap_or(0) == 0 || h == "\r\n" {
                        break;
                    }
                    if let Some(v) = h.to_ascii_lowercase().strip_prefix("content-length:") {
                        len = v.trim().parse().unwrap_or(0);
                    }
                }
                let mut body = vec![0u8; len];
                let _ = r.read_exact(&mut body);
                let body_s = if req_line.contains("/cancel") {
                    r#"{"status":"cancelled"}"#
                } else {
                    r#"{"id":"run_steer_1","session_id":"sess_steer_1","status":"running"}"#
                };
                let _ = write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body_s}",
                    body_s.len()
                );
            }
        });
        let dir = tempfile::tempdir().unwrap();
        let cfg = dir.path();
        std::fs::write(
            cfg.join(crate::remote::TOKEN_FILE),
            format!("aars_{}", "f".repeat(64)),
        )
        .unwrap();
        std::fs::write(
            cfg.join(crate::remote::MCP_URL_FILE),
            "https://mcp.example.test/mcp",
        )
        .unwrap();
        std::fs::write(
            cfg.join("cloud.json"),
            json!({"base_url": base, "api_key": "test-key"}).to_string(),
        )
        .unwrap();

        struct SteerThenInterruptReader {
            step: u8,
        }
        impl Read for SteerThenInterruptReader {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                let msg = match self.step {
                    0 => {
                        self.step = 1;
                        b"{\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"first turn\"}]}}\n" as &[u8]
                    }
                    1 => {
                        self.step = 2;
                        b"{\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"mid-turn steer\"}]}}\n" as &[u8]
                    }
                    2 => {
                        self.step = 3;
                        std::thread::sleep(Duration::from_millis(80));
                        b"{\"type\":\"control_request\",\"request_id\":\"req_int\",\"request\":{\"subtype\":\"interrupt\"}}\n" as &[u8]
                    }
                    _ => return Ok(0),
                };
                let n = msg.len().min(buf.len());
                buf[..n].copy_from_slice(&msg[..n]);
                Ok(n)
            }
        }
        #[derive(Clone, Default)]
        struct VecWriter(Arc<Mutex<Vec<u8>>>);
        impl Write for VecWriter {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let out_buf = VecWriter::default();
        run_with_io(
            CloudHarnessArgs {
                config: Some(cfg.to_path_buf()),
                harness: Some("cursor-cloud".into()),
                model: None,
                resume: None,
            },
            BufReader::new(SteerThenInterruptReader { step: 0 }),
            out_buf.clone(),
        )
        .unwrap();
        stop_srv.store(true, Ordering::SeqCst);
        srv.join().unwrap();
        let out_str = String::from_utf8(out_buf.0.lock().unwrap().clone()).unwrap();
        assert!(out_str.contains("\"request_id\":\"req_int\""), "{out_str}");
        assert!(out_str.contains("\"is_error\":true"), "{out_str}");
    }

    #[test]
    fn build_cloud_system_prompt_deduplicates_memory_and_skills_blocks_and_supports_coord_memory() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = dir.path().join("config");
        std::fs::create_dir_all(cfg.join("memory")).unwrap();
        std::fs::create_dir_all(cfg.join(".agents/skills/demo-skill")).unwrap();
        std::fs::write(cfg.join("memory/MEMORY.md"), "duplicate memory sentinel").unwrap();
        std::fs::write(
            cfg.join(".agents/skills/demo-skill/SKILL.md"),
            "---\nname: demo-skill\ndescription: demo\n---\nbody\n",
        )
        .unwrap();
        std::fs::write(
            cfg.join("SYSTEM_PROMPT.md"),
            "Base prompt\n<memory>\nAlready mounted memory\n</memory>\n<available_skills>\n- `demo-skill`: demo\n</available_skills>\n",
        )
        .unwrap();

        let rendered = build_cloud_system_prompt(&cfg).unwrap();
        assert_eq!(
            rendered.matches("<available_skills>").count(),
            1,
            "{rendered}"
        );
        assert!(
            !rendered.contains("duplicate memory sentinel"),
            "{rendered}"
        );
        assert!(!rendered.contains("<learned_memory>"), "{rendered}");
    }
}
