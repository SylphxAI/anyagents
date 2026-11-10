//! Outbound Cloud Harness Adapter (`spiron-cloud`, `claude-cloud`,
//! `openai-cloud`, `cursor-cloud`).
//!
//! Speaks the `stream-json` wire protocol on stdin/stdout so `ClaudeHarness`
//! and `anyagents-supervisor` supervise cloud agent runs with full turn
//! lifecycle, inbox delivery, and session resumption while dispatching each
//! turn over HTTP with the agent's Streamable HTTP `/mcp` capability attached.

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

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

    let mut session_id = args
        .resume
        .filter(|s| !s.trim().is_empty())
        .or_else(|| read_persisted_session_id(&config, &harness));

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
        // message marker as echoed (`note_echo` / `note_markers`).
        writeln!(stdout, "{trimmed}")?;
        stdout.flush()?;

        let prompt_text = extract_user_prompt(&event);
        match dispatch_cloud_turn(
            &config,
            &harness,
            model.as_deref(),
            session_id.as_deref(),
            &prompt_text,
        ) {
            Ok(outcome) => {
                session_id = Some(outcome.session_id.clone());
                persist_session_id(&config, &harness, &outcome.session_id);
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
                writeln!(stdout, "{result_event}")?;
                stdout.flush()?;
            }
            Err(err_msg) => {
                let fallback_sid = session_id
                    .clone()
                    .unwrap_or_else(|| format!("{harness}-session"));
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

pub fn dispatch_cloud_turn(
    config: &Path,
    harness: &str,
    model: Option<&str>,
    session_id: Option<&str>,
    prompt_text: &str,
) -> Result<CloudTurnOutcome, String> {
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
        .timeout_global(Some(std::time::Duration::from_secs(300)))
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

    let parsed: Value = serde_json::from_str(&raw_body).map_err(|e| {
        format!("cloud harness `{canonical}` returned invalid JSON from {endpoint}: {e}")
    })?;

    let resolved_session_id = parsed
        .get("session_id")
        .or_else(|| parsed.get("response_id"))
        .or_else(|| parsed.get("id"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
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
}
