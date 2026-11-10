//! Outbound Cloud Harness Adapter (`spiron-cloud`, `claude-cloud`,
//! `openai-cloud`, `cursor-cloud`).
//!
//! Speaks the `stream-json` wire protocol on stdin/stdout so `ClaudeHarness`
//! and `anyagents-supervisor` supervise cloud agent runs with full turn
//! lifecycle, inbox delivery, and session resumption while dispatching each
//! turn over HTTP with the agent's Streamable HTTP `/mcp` capability attached.

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use anyagents_core::Error;
use anyagents_harness::normalize_harness_name;
use clap::Args;
use serde_json::{Value, json};

#[derive(Args, Clone, Debug)]
pub struct CloudHarnessArgs {
    /// Agent configuration directory containing `mcp-capability` and credentials.
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
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    run_with_io(args, BufReader::new(stdin.lock()), stdout)
}

pub fn run_with_io<R: BufRead, W: Write + Send + 'static>(
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

    let session_id = Arc::new(Mutex::new(
        args.resume
            .filter(|s| !s.trim().is_empty())
            .or_else(|| read_persisted_session_id(&config, &harness)),
    ));
    let writer = Arc::new(Mutex::new(writer));

    type ActiveTurn = (
        Arc<AtomicBool>,
        Arc<Mutex<Option<String>>>,
        std::thread::JoinHandle<()>,
    );
    let mut active_turn: Option<ActiveTurn> = None;

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
        if event_type == "control_request" || event_type == "cancel" {
            let is_interrupt = event_type == "cancel"
                || event
                    .pointer("/request/subtype")
                    .and_then(Value::as_str)
                    .is_some_and(|s| s == "interrupt" || s == "cancel");
            if is_interrupt && let Some((cancel_flag, active_job_id, handle)) = active_turn.take() {
                cancel_flag.store(true, Ordering::SeqCst);
                if let Some(job_id) = active_job_id.lock().unwrap().clone() {
                    let base_url = resolve_cloud_base_url(&config, &harness);
                    let auth_token = resolve_cloud_auth_token(&config, &harness);
                    let _ = cancel_cloud_turn(&base_url, &harness, auth_token.as_deref(), &job_id);
                }
                let _ = handle.join();
            }
            continue;
        }
        if event_type != "user" {
            continue;
        }

        if let Some((_, _, prev_handle)) = active_turn.take() {
            let _ = prev_handle.join();
        }

        // Echo the user message back immediately so `ClaudeHarness` records the
        // message marker as echoed (`note_echo` / `note_markers`).
        {
            let mut out = writer.lock().unwrap();
            writeln!(out, "{trimmed}")?;
            out.flush()?;
        }

        let prompt_text = extract_user_prompt(&event);
        let cancel_flag = Arc::new(AtomicBool::new(false));
        let active_job_id = Arc::new(Mutex::new(None));

        let cfg_clone = config.clone();
        let harness_clone = harness.clone();
        let model_clone = model.clone();
        let sid_shared = Arc::clone(&session_id);
        let writer_shared = Arc::clone(&writer);
        let cancel_clone = Arc::clone(&cancel_flag);
        let job_clone = Arc::clone(&active_job_id);

        let handle = std::thread::spawn(move || {
            let current_sid = sid_shared.lock().unwrap().clone();
            match dispatch_cloud_turn_with_cancel(
                &cfg_clone,
                &harness_clone,
                model_clone.as_deref(),
                current_sid.as_deref(),
                &prompt_text,
                &cancel_clone,
                &job_clone,
            ) {
                Ok(outcome) => {
                    *sid_shared.lock().unwrap() = Some(outcome.session_id.clone());
                    persist_session_id(&cfg_clone, &harness_clone, &outcome.session_id);
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
                Err(err_msg) => {
                    let fallback_sid = sid_shared
                        .lock()
                        .unwrap()
                        .clone()
                        .unwrap_or_else(|| format!("{harness_clone}-session"));
                    let result_event = json!({
                        "type": "result",
                        "subtype": "error",
                        "is_error": true,
                        "session_id": fallback_sid,
                        "result": err_msg,
                    });
                    let mut out = writer_shared.lock().unwrap();
                    let _ = writeln!(out, "{result_event}");
                    let _ = out.flush();
                }
            }
        });

        active_turn = Some((cancel_flag, active_job_id, handle));
    }

    if let Some((_, _, handle)) = active_turn.take() {
        let _ = handle.join();
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
    if !session_id.trim().is_empty() {
        let _ = std::fs::write(
            config.join(session_filename_for(harness)),
            format!("{}\n", session_id.trim()),
        );
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

pub fn resolve_capability_token(config: &Path) -> Result<String, String> {
    if let Ok(raw) = std::fs::read_to_string(config.join("mcp-capability")) {
        let trimmed = raw.trim();
        if !trimmed.is_empty() {
            return Ok(trimmed.to_string());
        }
    }
    if let Ok(env_cap) = std::env::var("ANYAGENTS_AGENT_CAPABILITY") {
        let trimmed = env_cap.trim();
        if !trimmed.is_empty() {
            return Ok(trimmed.to_string());
        }
    }
    Err(format!("missing `mcp-capability` in {}", config.display()))
}

pub fn resolve_public_mcp_url(config: &Path) -> String {
    if let Ok(url) = std::env::var("ANYAGENTS_PUBLIC_MCP_URL") {
        let trimmed = url.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }
    if let Ok(file_url) = std::fs::read_to_string(config.join(".anyagents-mcp-url")) {
        let trimmed = file_url.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }
    if let Some(cloud_cfg) = read_cloud_json(config)
        && let Some(url) = cloud_cfg.get("mcp_url").and_then(Value::as_str)
        && !url.trim().is_empty()
    {
        return url.trim().to_string();
    }
    "http://127.0.0.1:8788/v1/webhooks/mcp".to_string()
}

pub fn mcp_url_with_profile(base_url: &str, profile: &str) -> String {
    let trimmed = base_url.trim();
    if trimmed.contains("profile=") {
        return trimmed.to_string();
    }
    if trimmed.contains('?') {
        format!("{trimmed}&profile={profile}")
    } else {
        format!("{trimmed}?profile={profile}")
    }
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
            .or_else(|| std::env::var("SPIRON_CLOUD_URL").ok())
            .filter(|s| !s.trim().is_empty())
            .or(json_url)
            .unwrap_or_else(|| "https://sylphx.com".to_string())
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
    if let Some(token) = extract_token_from_json_file(&config.join("cloud.json")) {
        return Some(token);
    }
    match normalize_harness_name(harness) {
        "spiron-cloud" => {
            for env_key in [
                "ANYAGENTS_SPIRON_API_KEY",
                "SPIRON_API_KEY",
                "SYLPHX_API_KEY",
            ] {
                if let Ok(v) = std::env::var(env_key)
                    && !v.trim().is_empty()
                {
                    return Some(v.trim().to_string());
                }
            }
            for rel in [
                ".spiron/auth.json",
                ".config/sylphx/auth.json",
                "credentials.json",
                "auth.json",
            ] {
                if let Some(tok) = extract_token_from_json_file(&config.join(rel)) {
                    return Some(tok);
                }
            }
            None
        }
        "claude-cloud" => {
            for env_key in [
                "ANYAGENTS_ANTHROPIC_API_KEY",
                "ANTHROPIC_API_KEY",
                "CLAUDE_API_KEY",
            ] {
                if let Ok(v) = std::env::var(env_key)
                    && !v.trim().is_empty()
                {
                    return Some(v.trim().to_string());
                }
            }
            for rel in [
                ".credentials.json",
                ".claude/.credentials.json",
                "auth.json",
            ] {
                if let Some(tok) = extract_token_from_json_file(&config.join(rel)) {
                    return Some(tok);
                }
            }
            None
        }
        "openai-cloud" => {
            for env_key in ["ANYAGENTS_OPENAI_API_KEY", "OPENAI_API_KEY"] {
                if let Ok(v) = std::env::var(env_key)
                    && !v.trim().is_empty()
                {
                    return Some(v.trim().to_string());
                }
            }
            for rel in ["auth.json", ".codex/auth.json", "credentials.json"] {
                if let Some(tok) = extract_token_from_json_file(&config.join(rel)) {
                    return Some(tok);
                }
            }
            None
        }
        "cursor-cloud" => {
            for env_key in ["ANYAGENTS_CURSOR_API_KEY", "CURSOR_API_KEY"] {
                if let Ok(v) = std::env::var(env_key)
                    && !v.trim().is_empty()
                {
                    return Some(v.trim().to_string());
                }
            }
            for rel in ["cursor-auth.json", "auth.json", "credentials.json"] {
                if let Some(tok) = extract_token_from_json_file(&config.join(rel)) {
                    return Some(tok);
                }
            }
            None
        }
        _ => None,
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
    let cancel_flag = AtomicBool::new(false);
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

pub fn dispatch_cloud_turn_with_cancel(
    config: &Path,
    harness: &str,
    model: Option<&str>,
    session_id: Option<&str>,
    prompt_text: &str,
    cancel_flag: &AtomicBool,
    active_job_id: &Mutex<Option<String>>,
) -> Result<CloudTurnOutcome, String> {
    if cancel_flag.load(Ordering::SeqCst) {
        return Err("cancelled by supervisor interrupt".to_string());
    }

    let capability = resolve_capability_token(config)?;
    let mcp_base_url = resolve_public_mcp_url(config);
    let base_url = resolve_cloud_base_url(config, harness);
    let auth_token = resolve_cloud_auth_token(config, harness);
    let canonical = normalize_harness_name(harness);

    let (endpoint, payload) = match canonical {
        "spiron-cloud" => {
            let mcp_url = mcp_url_with_profile(&mcp_base_url, "coordination");
            let body = json!({
                "prompt": prompt_text,
                "model": model.unwrap_or("claude-sonnet-4.6"),
                "session_id": session_id,
                "mcp_servers": [
                    {
                        "name": "anyagents",
                        "url": mcp_url,
                        "bearer_token": capability,
                    }
                ],
                "disabled_tools": [],
            });
            (format!("{base_url}/api/agent-runs"), body)
        }
        "claude-cloud" => {
            let mcp_url = mcp_url_with_profile(&mcp_base_url, "remote");
            let body = json!({
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
            (format!("{base_url}/v1/messages"), body)
        }
        "openai-cloud" => {
            let mcp_url = mcp_url_with_profile(&mcp_base_url, "remote");
            let body = json!({
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
            (format!("{base_url}/v1/responses"), body)
        }
        "cursor-cloud" => {
            let mcp_url = mcp_url_with_profile(&mcp_base_url, "remote");
            let body = json!({
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
            (format!("{base_url}/v0/agents"), body)
        }
        other => {
            return Err(format!("unsupported cloud harness `{other}`"));
        }
    };

    let agent: ureq::Agent = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(300)))
        .http_status_as_error(false)
        .build()
        .into();

    let mut req = agent
        .post(&endpoint)
        .header("content-type", "application/json");
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

    let mut resp = req
        .send_json(&payload)
        .map_err(|e| format!("cloud harness `{canonical}` request to {endpoint} failed: {e}"))?;
    let status = resp.status().as_u16();
    let content_type = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_ascii_lowercase();
    let raw_body = resp
        .body_mut()
        .read_to_string()
        .map_err(|e| format!("failed reading response body from {endpoint}: {e}"))?;

    if !(200..300).contains(&status) {
        return Err(format!(
            "cloud harness `{canonical}` returned HTTP {status}: {}",
            raw_body.trim()
        ));
    }

    let trimmed_body = raw_body.trim_start();
    if content_type.contains("text/event-stream")
        || trimmed_body.starts_with("event:")
        || trimmed_body.starts_with("data:")
    {
        return parse_sse_cloud_response(&raw_body, canonical, session_id);
    }

    let mut parsed: Value = serde_json::from_str(&raw_body).map_err(|e| {
        format!("cloud harness `{canonical}` returned invalid JSON from {endpoint}: {e}")
    })?;

    if let Some(initial_status) = parsed.get("status").and_then(Value::as_str)
        && matches!(
            initial_status.trim().to_ascii_lowercase().as_str(),
            "queued" | "running" | "in_progress" | "pending"
        )
        && let Some(job_id) = extract_cloud_job_id(&parsed)
    {
        *active_job_id.lock().unwrap() = Some(job_id.clone());
        parsed = poll_cloud_job_until_done(
            &agent,
            &base_url,
            canonical,
            auth_token.as_deref(),
            &job_id,
            cancel_flag,
        )?;
    }

    let resolved_session_id = extract_cloud_job_id(&parsed)
        .or_else(|| session_id.map(str::to_string))
        .unwrap_or_else(|| format!("{canonical}-session-1"));

    let reply_text = extract_cloud_reply_text(&parsed);
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

fn extract_cloud_job_id(parsed: &Value) -> Option<String> {
    parsed
        .get("session_id")
        .or_else(|| parsed.get("run_id"))
        .or_else(|| parsed.get("response_id"))
        .or_else(|| parsed.get("id"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

pub fn cloud_job_status_url(base_url: &str, harness: &str, job_id: &str) -> String {
    let base = base_url.trim_end_matches('/');
    match normalize_harness_name(harness) {
        "spiron-cloud" => format!("{base}/api/agent-runs/{job_id}"),
        "claude-cloud" => format!("{base}/v1/messages/{job_id}"),
        "openai-cloud" => format!("{base}/v1/responses/{job_id}"),
        "cursor-cloud" => format!("{base}/v0/agents/{job_id}"),
        _ => format!("{base}/api/agent-runs/{job_id}"),
    }
}

pub fn cancel_cloud_turn(
    base_url: &str,
    harness: &str,
    auth_token: Option<&str>,
    job_id: &str,
) -> Result<(), String> {
    let status_url = cloud_job_status_url(base_url, harness, job_id);
    let cancel_url = format!("{status_url}/cancel");
    let canonical = normalize_harness_name(harness);
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
    let mut resp = req
        .send_json(json!({ "action": "cancel" }))
        .map_err(|e| format!("cancel request to {cancel_url} failed: {e}"))?;
    let status = resp.status().as_u16();
    if !(200..300).contains(&status) {
        let body = resp.body_mut().read_to_string().unwrap_or_default();
        return Err(format!("cancel returned HTTP {status}: {}", body.trim()));
    }
    Ok(())
}

fn poll_cloud_job_until_done(
    agent: &ureq::Agent,
    base_url: &str,
    canonical: &str,
    auth_token: Option<&str>,
    job_id: &str,
    cancel_flag: &AtomicBool,
) -> Result<Value, String> {
    let status_url = cloud_job_status_url(base_url, canonical, job_id);
    let poll_ms = std::env::var("ANYAGENTS_CLOUD_POLL_INTERVAL_MS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(50)
        .clamp(10, 5_000);

    for _ in 0..6_000 {
        if cancel_flag.load(Ordering::SeqCst) {
            let _ = cancel_cloud_turn(base_url, canonical, auth_token, job_id);
            return Err("cancelled by supervisor interrupt".to_string());
        }
        std::thread::sleep(Duration::from_millis(poll_ms));
        if cancel_flag.load(Ordering::SeqCst) {
            let _ = cancel_cloud_turn(base_url, canonical, auth_token, job_id);
            return Err("cancelled by supervisor interrupt".to_string());
        }

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
        let mut resp = req
            .call()
            .map_err(|e| format!("cloud job poll to {status_url} failed: {e}"))?;
        let status_code = resp.status().as_u16();
        let body = resp
            .body_mut()
            .read_to_string()
            .map_err(|e| format!("failed reading poll body from {status_url}: {e}"))?;
        if !(200..300).contains(&status_code) {
            return Err(format!(
                "cloud job poll `{canonical}` returned HTTP {status_code}: {}",
                body.trim()
            ));
        }
        let poll_val: Value = serde_json::from_str(&body)
            .map_err(|e| format!("invalid JSON from cloud job poll {status_url}: {e}"))?;
        let st = poll_val
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("completed")
            .trim()
            .to_ascii_lowercase();
        match st.as_str() {
            "queued" | "running" | "in_progress" | "pending" => continue,
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
                return Err(format!("cloud job {job_id} {st}: {msg}"));
            }
            _ => return Ok(poll_val),
        }
    }
    Err(format!(
        "cloud job {job_id} timed out waiting for completion"
    ))
}

pub fn parse_sse_cloud_response(
    raw_sse: &str,
    canonical: &str,
    fallback_session_id: Option<&str>,
) -> Result<CloudTurnOutcome, String> {
    let mut current_event = String::new();
    let mut resolved_sid: Option<String> = None;
    let mut delta_chunks = String::new();
    let mut full_reply: Option<String> = None;
    let mut input_tokens = 0u64;
    let mut output_tokens = 0u64;

    for raw_line in raw_sse.lines() {
        let line = raw_line.trim();
        if line.is_empty() {
            current_event.clear();
            continue;
        }
        if let Some(ev) = line.strip_prefix("event:") {
            current_event = ev.trim().to_string();
            continue;
        }
        let Some(data_str) = line.strip_prefix("data:") else {
            continue;
        };
        let data_trimmed = data_str.trim();
        if data_trimmed.is_empty() || data_trimmed == "[DONE]" {
            continue;
        }
        let Ok(val) = serde_json::from_str::<Value>(data_trimmed) else {
            continue;
        };

        // Extract session/message/response ID
        if let Some(sid) = extract_cloud_job_id(&val).or_else(|| {
            val.pointer("/message/id")
                .or_else(|| val.pointer("/response/id"))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        }) {
            resolved_sid = Some(sid);
        }

        let ev_type = val
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or(current_event.as_str());

        match ev_type {
            "content_block_delta" => {
                if let Some(t) = val.pointer("/delta/text").and_then(Value::as_str) {
                    delta_chunks.push_str(t);
                }
            }
            "response.output_text.delta" => {
                if let Some(t) = val.get("delta").and_then(Value::as_str) {
                    delta_chunks.push_str(t);
                }
            }
            _ => {
                if let Some(t) = val.pointer("/delta/text").and_then(Value::as_str) {
                    delta_chunks.push_str(t);
                } else if let Some(t) = val.get("delta").and_then(Value::as_str) {
                    delta_chunks.push_str(t);
                } else if let Some(t) = val.get("text").and_then(Value::as_str) {
                    delta_chunks.push_str(t);
                }
                for key in ["result", "output_text", "reply"] {
                    if let Some(r) = val.get(key).and_then(Value::as_str)
                        && !r.trim().is_empty()
                    {
                        full_reply = Some(r.to_string());
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
                input_tokens = tok;
            }
        }
        for ptr in [
            "/usage/output_tokens",
            "/message/usage/output_tokens",
            "/response/usage/output_tokens",
        ] {
            if let Some(tok) = val.pointer(ptr).and_then(Value::as_u64) {
                output_tokens = tok;
            }
        }
    }

    let reply_text = if !delta_chunks.is_empty() {
        delta_chunks
    } else if let Some(r) = full_reply {
        r
    } else {
        "completed".to_string()
    };

    let session_id = resolved_sid
        .or_else(|| fallback_session_id.map(str::to_string))
        .unwrap_or_else(|| format!("{canonical}-session-1"));

    Ok(CloudTurnOutcome {
        session_id,
        reply_text,
        input_tokens,
        output_tokens,
    })
}

fn extract_cloud_reply_text(parsed: &Value) -> String {
    for key in ["result", "output_text", "reply", "text", "response"] {
        if let Some(s) = parsed.get(key).and_then(Value::as_str)
            && !s.trim().is_empty()
        {
            return s.to_string();
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
            return texts.join("\n");
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
            return texts.join("\n");
        }
    }
    if let Some(s) = parsed.pointer("/message/text").and_then(Value::as_str)
        && !s.trim().is_empty()
    {
        return s.to_string();
    }
    "completed".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Router,
        http::{HeaderMap, StatusCode},
        routing::post,
    };
    use std::sync::{Arc, Mutex};

    #[tokio::test]
    async fn cloud_harness_dispatches_spiron_claude_openai_and_cursor_turns_with_mcp_capability() {
        #[derive(Default)]
        struct RecordedCalls {
            spiron: Mutex<Vec<Value>>,
            claude: Mutex<Vec<(String, Value)>>,
            openai: Mutex<Vec<Value>>,
            cursor: Mutex<Vec<Value>>,
        }

        let recorded = Arc::new(RecordedCalls::default());
        let app = Router::new()
            .route(
                "/api/agent-runs",
                post({
                    let rec = Arc::clone(&recorded);
                    move |axum::Json(payload): axum::Json<Value>| {
                        let rec = Arc::clone(&rec);
                        async move {
                            rec.spiron.lock().unwrap().push(payload);
                            (
                                StatusCode::OK,
                                axum::Json(json!({
                                    "session_id": "spiron-sess-99",
                                    "result": "spiron cloud turn ok",
                                    "usage": { "input_tokens": 11, "output_tokens": 22 }
                                })),
                            )
                        }
                    }
                }),
            )
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
        std::fs::write(cfg.join("mcp-capability"), "cap-secret-xyz\n").unwrap();
        std::fs::write(
            cfg.join(".anyagents-mcp-url"),
            "https://mcp.example.test/mcp\n",
        )
        .unwrap();
        std::fs::write(
            cfg.join("cloud.json"),
            serde_json::to_vec(&json!({
                "base_url": format!("http://{addr}"),
                "api_key": "upstream-api-key"
            }))
            .unwrap(),
        )
        .unwrap();

        // 1. spiron-cloud
        let cfg_clone = cfg.clone();
        let spiron_out = tokio::task::spawn_blocking(move || {
            dispatch_cloud_turn(
                &cfg_clone,
                "spiron-cloud",
                Some("claude-sonnet-4.6"),
                None,
                "ping spiron",
            )
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(spiron_out.session_id, "spiron-sess-99");
        assert_eq!(spiron_out.reply_text, "spiron cloud turn ok");
        {
            let calls = recorded.spiron.lock().unwrap();
            assert_eq!(calls.len(), 1);
            assert_eq!(calls[0]["prompt"], "ping spiron");
            assert_eq!(
                calls[0]["mcp_servers"][0]["url"],
                "https://mcp.example.test/mcp?profile=coordination"
            );
            assert_eq!(calls[0]["mcp_servers"][0]["bearer_token"], "cap-secret-xyz");
        }

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
                "cap-secret-xyz"
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
                "Bearer cap-secret-xyz"
            );
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
            assert_eq!(calls[0]["mcp_servers"][0]["bearer_token"], "cap-secret-xyz");
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
            // 3. Spiron async job polling (`queued` -> `running` -> `completed`) & cancel (`POST /api/agent-runs/{id}/cancel`)
            .route(
                "/api/agent-runs",
                post(|axum::Json(payload): axum::Json<Value>| async move {
                    let prompt = payload.get("prompt").and_then(Value::as_str).unwrap_or("");
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
                "/api/agent-runs/run_poll_44",
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
                "/api/agent-runs/run_cancel_55",
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
                "/api/agent-runs/run_cancel_55/cancel",
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
        std::fs::write(cfg.join("mcp-capability"), "cap-sse-tok\n").unwrap();
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
            dispatch_cloud_turn(&cfg_clone, "spiron-cloud", None, None, "poll test").unwrap()
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
                    harness: Some("spiron-cloud".into()),
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
            "expected POST /api/agent-runs/run_cancel_55/cancel to be invoked, got {recorded_cancels:?}"
        );
        let stdout_str = String::from_utf8(out_buf.0.lock().unwrap().clone()).unwrap();
        assert!(
            stdout_str.contains("\"is_error\":true") && stdout_str.contains("cancelled"),
            "expected error result event on interrupt, got: {stdout_str}"
        );

        server_handle.abort();
    }
}
