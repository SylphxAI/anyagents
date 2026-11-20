//! Stdio JSON-RPC to HTTPS Streamable MCP proxy (`anyagents-guest mcp-proxy`).

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use clap::Args;
use serde_json::{Value, json};

/// Maximum inline textual JSON-RPC frame size before offloading into the
/// sandbox-accessible tmp directory (`$ANYAGENTS_AGENT_DATA_DIR/tmp`).
const PROXY_TEXT_SPILL_BYTES: usize = 56 * 1024;
const MAX_CONCURRENT_RPC: usize = 32;

#[derive(Args, Clone, Debug)]
pub struct McpProxyArgs {
    /// Target coordinator MCP URL (typically `https://<host>/mcp`).
    #[arg(long)]
    pub url: Option<String>,
    /// Path to file containing the current bearer capability token (hot-reloaded per call).
    #[arg(long)]
    pub token_file: Option<PathBuf>,
    /// Static bearer token fallback when `--token-file` is absent.
    #[arg(long)]
    pub token: Option<String>,
    /// Request timeout in seconds.
    #[arg(long, default_value_t = 120)]
    pub timeout_secs: u64,
    /// Optional sandbox directory for offloading oversized textual MCP responses.
    #[arg(long)]
    pub spill_dir: Option<PathBuf>,
}

/// Resolve the sandbox-accessible offload directory (`<agent-data>/tmp`),
/// guaranteeing returned artifact paths are directly readable inside the sandbox.
pub(crate) fn resolve_proxy_spill_dir(explicit: Option<&Path>) -> PathBuf {
    if let Some(dir) = explicit.filter(|p| !p.as_os_str().is_empty()) {
        return dir.to_path_buf();
    }
    for key in ["ANYAGENTS_AGENT_DATA_DIR", "ANYAGENTS_SANDBOX_DATA_DIR"] {
        if let Ok(raw) = std::env::var(key) {
            let trimmed = raw.trim();
            if !trimmed.is_empty() {
                return PathBuf::from(trimmed).join("tmp");
            }
        }
    }
    if let Ok(raw) = std::env::var("ANYAGENTS_GUEST_STATE_DIR") {
        let trimmed = raw.trim().trim_end_matches('/');
        if !trimmed.is_empty() {
            let root = trimmed
                .strip_suffix("/state")
                .filter(|p| !p.is_empty())
                .unwrap_or(trimmed);
            return PathBuf::from(root).join("tmp");
        }
    }
    PathBuf::from(crate::DEFAULT_DATA_MOUNT_DIR).join("tmp")
}

/// Run the stdio <-> HTTPS MCP proxy loop.
pub fn run_mcp_proxy(mut args: McpProxyArgs) -> Result<(), String> {
    if args.url.is_none() {
        args.url = std::env::var("ANYAGENTS_MCP_URL").ok();
    }
    if args.token_file.is_none() {
        args.token_file = std::env::var_os("ANYAGENTS_MCP_TOKEN_FILE").map(PathBuf::from);
    }
    if args.token.is_none() {
        args.token = std::env::var("ANYAGENTS_MCP_TOKEN").ok();
    }
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    let mut reader = BufReader::new(stdin);
    let mut writer = std::io::BufWriter::new(stdout);
    run_proxy_io(&args, &mut reader, &mut writer)
}

#[derive(Default)]
struct ProxyState {
    session_id: Option<String>,
    use_base64_payload: bool,
    negotiated: bool,
}

struct InFlightGate {
    state: Mutex<usize>,
    cvar: Condvar,
    max: usize,
}

impl InFlightGate {
    fn new(max: usize) -> Self {
        Self {
            state: Mutex::new(0),
            cvar: Condvar::new(),
            max: max.max(1),
        }
    }

    fn acquire(&self) -> InFlightPermit<'_> {
        let mut count = self.state.lock().unwrap_or_else(|e| e.into_inner());
        while *count >= self.max {
            count = self.cvar.wait(count).unwrap_or_else(|e| e.into_inner());
        }
        *count += 1;
        InFlightPermit { gate: self }
    }
}

struct InFlightPermit<'a> {
    gate: &'a InFlightGate,
}

impl Drop for InFlightPermit<'_> {
    fn drop(&mut self) {
        let mut count = self.gate.state.lock().unwrap_or_else(|e| e.into_inner());
        *count = count.saturating_sub(1);
        self.gate.cvar.notify_one();
    }
}

fn write_response_lines<W: Write>(writer: &Mutex<&mut W>, lines: &[String]) -> Result<(), String> {
    if lines.is_empty() {
        return Ok(());
    }
    let mut guard = writer.lock().unwrap_or_else(|e| e.into_inner());
    for resp in lines {
        writeln!(*guard, "{resp}").map_err(|e| e.to_string())?;
    }
    guard.flush().map_err(|e| e.to_string())
}

#[allow(clippy::too_many_arguments)]
fn dispatch_single_rpc<W: Write>(
    http: &ureq::Agent,
    url: &str,
    token: Option<&str>,
    state: &Mutex<ProxyState>,
    writer: &Mutex<&mut W>,
    spill_dir: &Path,
    spill_seq: &AtomicU64,
    req_id: Option<Value>,
    body: &str,
) -> Result<(), String> {
    match forward_rpc(http, url, token, state, body) {
        Ok(responses) => {
            let processed: Vec<String> = responses
                .iter()
                .map(|resp| maybe_offload_response(resp, spill_dir, spill_seq))
                .collect();
            write_response_lines(writer, &processed)
        }
        Err(err_msg) => {
            if let Some(id) = req_id {
                let err_resp = json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "error": { "code": -32000, "message": err_msg }
                });
                write_response_lines(writer, &[err_resp.to_string()])
            } else {
                eprintln!("anyagents-guest mcp-proxy notification failed: {err_msg}");
                Ok(())
            }
        }
    }
}

pub(crate) fn run_proxy_io<R: BufRead, W: Write + Send>(
    args: &McpProxyArgs,
    reader: &mut R,
    writer: &mut W,
) -> Result<(), String> {
    let url = args
        .url
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "mcp-proxy requires --url or ANYAGENTS_MCP_URL".to_string())?;
    let http = ureq::Agent::config_builder()
        .timeout_connect(Some(Duration::from_secs(15)))
        .timeout_global(Some(Duration::from_secs(args.timeout_secs.max(5))))
        .http_status_as_error(false)
        .build()
        .new_agent();

    let spill_dir = resolve_proxy_spill_dir(args.spill_dir.as_deref());
    let spill_seq = AtomicU64::new(0);
    let state = Mutex::new(ProxyState::default());
    let writer = Mutex::new(writer);
    let gate = InFlightGate::new(MAX_CONCURRENT_RPC);
    let first_write_err: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));

    let read_result: Result<(), String> = std::thread::scope(|scope| {
        let mut line = String::new();
        loop {
            if let Some(err) = first_write_err
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone()
            {
                return Err(err);
            }
            line.clear();
            let n = reader
                .read_line(&mut line)
                .map_err(|err| format!("mcp-proxy stdin read error: {err}"))?;
            if n == 0 {
                break;
            }
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let req_val: Value = match serde_json::from_str(trimmed) {
                Ok(v) => v,
                Err(err) => {
                    let err_resp = json!({
                        "jsonrpc": "2.0",
                        "id": Value::Null,
                        "error": { "code": -32700, "message": format!("parse error: {err}") }
                    });
                    write_response_lines(&writer, &[err_resp.to_string()])?;
                    continue;
                }
            };
            let req_id = req_val.get("id").cloned();
            let method = req_val
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let token = resolve_token(args);
            let body = trimmed.to_string();

            // Run the very first request (typically `initialize`) synchronously so
            // `mcp-session-id` and base64 framing negotiation complete before concurrent
            // tool calls fan out. All subsequent requests run concurrently with zero HoL.
            let should_run_sync = method == "initialize"
                || !state.lock().unwrap_or_else(|e| e.into_inner()).negotiated;
            if should_run_sync {
                dispatch_single_rpc(
                    &http,
                    url,
                    token.as_deref(),
                    &state,
                    &writer,
                    &spill_dir,
                    &spill_seq,
                    req_id,
                    &body,
                )?;
                state.lock().unwrap_or_else(|e| e.into_inner()).negotiated = true;
                continue;
            }

            let permit = gate.acquire();
            let http_ref = &http;
            let state_ref = &state;
            let writer_ref = &writer;
            let spill_dir_ref = &spill_dir;
            let spill_seq_ref = &spill_seq;
            let err_slot = Arc::clone(&first_write_err);
            scope.spawn(move || {
                let _permit = permit;
                if let Err(err) = dispatch_single_rpc(
                    http_ref,
                    url,
                    token.as_deref(),
                    state_ref,
                    writer_ref,
                    spill_dir_ref,
                    spill_seq_ref,
                    req_id,
                    &body,
                ) {
                    let mut slot = err_slot.lock().unwrap_or_else(|e| e.into_inner());
                    if slot.is_none() {
                        *slot = Some(err);
                    }
                }
            });
        }
        Ok(())
    });

    read_result?;
    if let Some(err) = first_write_err
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .take()
    {
        return Err(err);
    }
    Ok(())
}

fn resolve_token(args: &McpProxyArgs) -> Option<String> {
    if let Some(path) = &args.token_file
        && let Ok(raw) = std::fs::read_to_string(path)
    {
        let trimmed = raw.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }
    args.token
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

const PAYLOAD_ENCODING_HEADER: &str = "x-anyagents-payload-encoding";

fn send_rpc_once(
    http: &ureq::Agent,
    url: &str,
    token: Option<&str>,
    session_id: Option<&str>,
    body: &str,
    encode_base64: bool,
) -> Result<(u16, Option<String>, bool, String, String), String> {
    let mut req = http
        .post(url)
        .header("accept", "application/json, text/event-stream");
    if let Some(tok) = token {
        req = req.header("authorization", &format!("Bearer {tok}"));
    }
    if let Some(sid) = session_id {
        req = req.header("mcp-session-id", sid);
    }
    let resp = if encode_base64 {
        let encoded = crate::turn::encode_base64_standard(body.as_bytes());
        req.header("content-type", "application/octet-stream")
            .header(PAYLOAD_ENCODING_HEADER, "base64")
            .send(encoded.as_bytes())
    } else {
        req.header("content-type", "application/json")
            .send(body.as_bytes())
    }
    .map_err(|err| format!("MCP POST `{url}` failed: {err}"))?;
    let status = resp.status().as_u16();
    let next_sid = resp
        .headers()
        .get("mcp-session-id")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let advertises_b64 = resp
        .headers()
        .get(PAYLOAD_ENCODING_HEADER)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.trim().eq_ignore_ascii_case("base64"));
    let content_type = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_ascii_lowercase();
    let raw_body = resp
        .into_body()
        .read_to_string()
        .map_err(|err| format!("MCP response read error: {err}"))?;
    Ok((status, next_sid, advertises_b64, content_type, raw_body))
}

fn forward_rpc(
    http: &ureq::Agent,
    url: &str,
    token: Option<&str>,
    state: &Mutex<ProxyState>,
    body: &str,
) -> Result<Vec<String>, String> {
    let (session_snapshot, attempt_b64) = {
        let guard = state.lock().unwrap_or_else(|e| e.into_inner());
        (guard.session_id.clone(), guard.use_base64_payload)
    };
    let (mut status, mut next_sid, mut advertises_b64, mut content_type, mut raw_body) =
        send_rpc_once(
            http,
            url,
            token,
            session_snapshot.as_deref(),
            body,
            attempt_b64,
        )?;

    // If an intermediate WAF blocks plaintext JSON containing shell/SQL fragments with
    // HTTP 403, retry once with opaque base64 framing (`application/octet-stream`).
    let mut next_b64 = None;
    if status == 403 && !attempt_b64 {
        if let Ok(retry) = send_rpc_once(http, url, token, session_snapshot.as_deref(), body, true)
            && retry.0 != 415
            && retry.0 != 400
        {
            (status, next_sid, advertises_b64, content_type, raw_body) = retry;
            if (200..300).contains(&status) {
                next_b64 = Some(true);
            }
        }
    } else if attempt_b64 && status == 415 {
        // Older coordinators without base64 framing support return 415; fall back cleanly.
        next_b64 = Some(false);
        (status, next_sid, advertises_b64, content_type, raw_body) =
            send_rpc_once(http, url, token, session_snapshot.as_deref(), body, false)?;
    }

    if advertises_b64 || next_b64.is_some() || next_sid.is_some() {
        let mut guard = state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(b) = next_b64 {
            guard.use_base64_payload = b;
        }
        if advertises_b64 {
            guard.use_base64_payload = true;
        }
        if let Some(sid) = next_sid {
            guard.session_id = Some(sid);
        }
    }

    if !(200..300).contains(&status) {
        return Err(format!(
            "MCP server returned HTTP {status}: {}",
            raw_body.trim()
        ));
    }
    if raw_body.trim().is_empty() || status == 202 || status == 204 {
        return Ok(Vec::new());
    }
    if content_type.contains("text/event-stream") || raw_body.lstrip_sse_hint() {
        return Ok(extract_sse_data_lines(&raw_body));
    }
    // Re-serialize compactly on one line so downstream NDJSON readers never choke on pretty-printed JSON.
    let mut out = Vec::new();
    match serde_json::from_str::<Value>(&raw_body) {
        Ok(val) => out.push(serde_json::to_string(&val).map_err(|e| e.to_string())?),
        Err(_) => {
            for l in raw_body.lines().map(str::trim).filter(|l| !l.is_empty()) {
                out.push(l.to_string());
            }
        }
    }
    Ok(out)
}

trait SseHint {
    fn lstrip_sse_hint(&self) -> bool;
}

impl SseHint for str {
    fn lstrip_sse_hint(&self) -> bool {
        let trimmed = self.trim_start();
        trimmed.starts_with("data:") || trimmed.starts_with("event:")
    }
}

fn extract_sse_data_lines(body: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut buf = String::new();
    let flush_buf = |buf: &mut String, out: &mut Vec<String>| {
        let trimmed = buf.trim();
        if !trimmed.is_empty() {
            if let Ok(val) = serde_json::from_str::<Value>(trimmed)
                && let Ok(compact) = serde_json::to_string(&val)
            {
                out.push(compact);
            } else {
                out.push(trimmed.to_string());
            }
        }
        buf.clear();
    };
    for line in body.lines() {
        let clean = line.strip_suffix('\r').unwrap_or(line);
        if clean.is_empty() {
            flush_buf(&mut buf, &mut out);
        } else if let Some(data) = clean.strip_prefix("data:") {
            if !buf.is_empty() {
                buf.push('\n');
            }
            buf.push_str(data.trim_start());
        }
    }
    flush_buf(&mut buf, &mut out);
    out
}

fn contains_media(value: &Value) -> bool {
    match value {
        Value::Object(map) => {
            map.get("type")
                .and_then(Value::as_str)
                .is_some_and(|t| matches!(t, "image" | "audio" | "resource" | "resource_link"))
                || map.contains_key("visionImage")
                || map.get("image_attached").and_then(Value::as_bool) == Some(true)
                || (map.contains_key("mimeType") && map.contains_key("data"))
                || map.values().any(contains_media)
        }
        Value::Array(items) => items.iter().any(contains_media),
        Value::String(text) => serde_json::from_str::<Value>(text)
            .ok()
            .filter(|v| v.is_object() || v.is_array())
            .is_some_and(|v| contains_media(&v)),
        _ => false,
    }
}

fn maybe_offload_response(resp: &str, spill_dir: &Path, spill_seq: &AtomicU64) -> String {
    let bytes = resp.as_bytes();
    if bytes.len() < PROXY_TEXT_SPILL_BYTES {
        return resp.to_string();
    }
    let Ok(original) = serde_json::from_slice::<Value>(bytes) else {
        return resp.to_string();
    };
    let textual = !contains_media(&original)
        && (original.get("error").is_some()
            || original.get("result").is_some_and(|result| {
                result
                    .get("content")
                    .and_then(Value::as_array)
                    .is_none_or(|blocks| blocks.iter().all(|block| block["type"] == "text"))
            }));
    if !textual {
        return resp.to_string();
    }
    let seq = spill_seq.fetch_add(1, Ordering::Relaxed) + 1;
    let Ok(path) = write_spill_file(bytes, spill_dir, seq) else {
        return resp.to_string();
    };
    let sha256 = anyagents_toolchain::sha256_file(&path).unwrap_or_default();
    let artifact = json!({
        "path": path,
        "bytes": bytes.len(),
        "sha256": sha256,
        "format": "json",
        "mode": "0600",
        "read": "Read the JSON file at `path` directly and verify `bytes` and `sha256` before parsing."
    });
    let Ok(text) = serde_json::to_string(&json!({"offloaded": true, "artifact": artifact})) else {
        return resp.to_string();
    };
    let mut replacement = json!({"jsonrpc": "2.0", "id": original.get("id")});
    if original.get("error").is_some() {
        replacement["error"] = json!({
            "code": original["error"]["code"],
            "message": "Oversized JSON-RPC error preserved in artifact",
            "data": artifact
        });
    } else {
        replacement["result"] = json!({
            "content": [{"type": "text", "text": text}],
            "isError": original["result"]["isError"].as_bool().unwrap_or(false)
        });
    }
    match serde_json::to_string(&replacement) {
        Ok(compact) if compact.len() < PROXY_TEXT_SPILL_BYTES => compact,
        _ => resp.to_string(),
    }
}

fn write_spill_file(bytes: &[u8], dir: &Path, seq: u64) -> std::io::Result<PathBuf> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt as _;
        builder.mode(0o700);
    }
    builder.create(dir)?;
    let canon = std::fs::canonicalize(dir)?;
    let path = canon.join(format!(
        "anyagents-mcp-frame-{}-{seq}.json",
        std::process::id()
    ));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let mut file = options.open(&path)?;
    file.write_all(bytes)?;
    file.flush()?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Cursor, Read as _};
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};

    #[test]
    fn mcp_proxy_forwards_json_sse_202_and_hot_reloads_bearer_token() {
        let seen_auth: Arc<Mutex<Vec<Option<String>>>> = Arc::new(Mutex::new(Vec::new()));
        let seen_ref = Arc::clone(&seen_auth);

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let seen = Arc::clone(&seen_ref);
                std::thread::spawn(move || {
                    let mut reader = BufReader_::new(stream);
                    let mut line = String::new();
                    let _ = reader.read_line(&mut line);
                    let mut content_len = 0usize;
                    let mut auth = None;
                    loop {
                        let mut h = String::new();
                        if reader.read_line(&mut h).is_err()
                            || h == "\r\n"
                            || h == "\n"
                            || h.is_empty()
                        {
                            break;
                        }
                        if let Some((k, v)) = h.split_once(':') {
                            if k.trim().eq_ignore_ascii_case("content-length") {
                                content_len = v.trim().parse().unwrap_or(0);
                            } else if k.trim().eq_ignore_ascii_case("authorization") {
                                auth = Some(v.trim().to_string());
                            }
                        }
                    }
                    let mut body = vec![0u8; content_len];
                    if content_len > 0 {
                        let _ = reader.read_exact(&mut body);
                    }
                    seen.lock().unwrap().push(auth);
                    let req: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
                    let method = req.get("method").and_then(Value::as_str).unwrap_or("");
                    let mut stream = reader.into_inner();
                    if method == "notifications/initialized" {
                        let _ = stream.write_all(b"HTTP/1.1 202 Accepted\r\nConnection: close\r\nContent-Length: 0\r\nMcp-Session-Id: sess-1\r\n\r\n");
                    } else if method == "tools/call" {
                        let sse = b"event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{\"ok\":true}}\n\n";
                        let hdr = format!(
                            "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\n\r\n",
                            sse.len()
                        );
                        let _ = stream.write_all(hdr.as_bytes());
                        let _ = stream.write_all(sse);
                    } else {
                        let resp = serde_json::to_vec(&json!({
                            "jsonrpc": "2.0",
                            "id": req["id"],
                            "result": { "protocolVersion": "2025-03-26" }
                        }))
                        .unwrap();
                        let hdr = format!(
                            "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Type: application/json\r\nMcp-Session-Id: sess-1\r\nContent-Length: {}\r\n\r\n",
                            resp.len()
                        );
                        let _ = stream.write_all(hdr.as_bytes());
                        let _ = stream.write_all(&resp);
                    }
                });
            }
        });

        use std::io::BufReader as BufReader_;

        let tmp = tempfile::tempdir().unwrap();
        let tok_file = tmp.path().join("cap");
        std::fs::write(&tok_file, "cap-1\n").unwrap();

        // First two requests use cap-1, then rotate token file, then verify cap-2 works without restart.
        let args = McpProxyArgs {
            url: Some(format!("http://{addr}/mcp?profile=full")),
            token_file: Some(tok_file.clone()),
            token: None,
            timeout_secs: 10,
            spill_dir: Some(tmp.path().join("tmp")),
        };
        let input1 = concat!(
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\"}\n",
            "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n",
        );
        let mut out1 = Vec::new();
        run_proxy_io(&args, &mut Cursor::new(input1.as_bytes()), &mut out1).unwrap();
        let s1 = String::from_utf8(out1).unwrap();
        assert_eq!(s1.lines().count(), 1);
        assert!(s1.contains("2025-03-26"));

        std::fs::write(&tok_file, "cap-2\n").unwrap();
        let input2 = "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/call\"}\n";
        let mut out2 = Vec::new();
        run_proxy_io(&args, &mut Cursor::new(input2.as_bytes()), &mut out2).unwrap();
        let s2 = String::from_utf8(out2).unwrap();
        assert!(s2.contains("\"ok\":true"));

        let auths = seen_auth.lock().unwrap();
        assert_eq!(
            auths.as_slice(),
            &[
                Some("Bearer cap-1".to_string()),
                Some("Bearer cap-1".to_string()),
                Some("Bearer cap-2".to_string()),
            ]
        );
    }

    #[test]
    fn mcp_proxy_retries_waf_403_with_base64_and_negotiates_encoding() {
        let seen_encodings: Arc<Mutex<Vec<Option<String>>>> = Arc::new(Mutex::new(Vec::new()));
        let seen_ref = Arc::clone(&seen_encodings);

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let seen = Arc::clone(&seen_ref);
                std::thread::spawn(move || {
                    use std::io::BufReader as BufReader_;
                    let mut reader = BufReader_::new(stream);
                    let mut line = String::new();
                    let _ = reader.read_line(&mut line);
                    let mut content_len = 0usize;
                    let mut encoding = None;
                    loop {
                        let mut h = String::new();
                        if reader.read_line(&mut h).is_err()
                            || h == "\r\n"
                            || h == "\n"
                            || h.is_empty()
                        {
                            break;
                        }
                        if let Some((k, v)) = h.split_once(':') {
                            if k.trim().eq_ignore_ascii_case("content-length") {
                                content_len = v.trim().parse().unwrap_or(0);
                            } else if k.trim().eq_ignore_ascii_case(PAYLOAD_ENCODING_HEADER) {
                                encoding = Some(v.trim().to_string());
                            }
                        }
                    }
                    let mut body = vec![0u8; content_len];
                    if content_len > 0 {
                        let _ = reader.read_exact(&mut body);
                    }
                    seen.lock().unwrap().push(encoding.clone());
                    let mut stream = reader.into_inner();

                    // Simulate Cloudflare WAF rejecting plaintext JSON containing shell pipes.
                    if encoding.is_none() && String::from_utf8_lossy(&body).contains("| head") {
                        let blocked = b"<html>403 Forbidden (WAF)</html>";
                        let hdr = format!(
                            "HTTP/1.1 403 Forbidden\r\nConnection: close\r\nContent-Type: text/html\r\nContent-Length: {}\r\n\r\n",
                            blocked.len()
                        );
                        let _ = stream.write_all(hdr.as_bytes());
                        let _ = stream.write_all(blocked);
                        return;
                    }

                    let resp = serde_json::to_vec(&json!({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "result": { "ok": true, "encoded": encoding.is_some() }
                    }))
                    .unwrap();
                    let hdr = format!(
                        "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Type: application/json\r\n{PAYLOAD_ENCODING_HEADER}: base64\r\nContent-Length: {}\r\n\r\n",
                        resp.len()
                    );
                    let _ = stream.write_all(hdr.as_bytes());
                    let _ = stream.write_all(&resp);
                });
            }
        });

        let tmp_waf = tempfile::tempdir().unwrap();
        let args = McpProxyArgs {
            url: Some(format!("http://{addr}/mcp?profile=full")),
            token_file: None,
            token: Some("cap-waf".into()),
            timeout_secs: 10,
            spill_dir: Some(tmp_waf.path().join("tmp")),
        };
        let input = concat!(
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"cmd\":\"ls | head -n 5\"}}\n",
            "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/call\",\"params\":{\"cmd\":\"cat /etc/hosts | grep local\"}}\n",
        );
        let mut out = Vec::new();
        run_proxy_io(&args, &mut Cursor::new(input.as_bytes()), &mut out).unwrap();
        let lines: Vec<&str> = std::str::from_utf8(&out).unwrap().lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("\"encoded\":true"));
        assert!(lines[1].contains("\"encoded\":true"));

        let encs = seen_encodings.lock().unwrap();
        // 1st call: plaintext blocked 403 -> retried with base64 -> sticky for 2nd call!
        assert_eq!(
            encs.as_slice(),
            &[None, Some("base64".to_string()), Some("base64".to_string())]
        );
    }

    #[test]
    fn mcp_proxy_offloads_oversized_response_into_sandbox_tmp_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let spill_dir = tmp.path().join("agent-data/tmp");
        let huge_text = "x".repeat(120 * 1024);
        let original = json!({
            "jsonrpc": "2.0",
            "id": 99,
            "result": {
                "content": [{"type": "text", "text": huge_text}],
                "isError": false
            }
        });
        let raw = serde_json::to_string(&original).unwrap();
        assert!(raw.len() > 100 * 1024);

        let seq = AtomicU64::new(0);
        let out = maybe_offload_response(&raw, &spill_dir, &seq);
        assert!(out.len() <= PROXY_TEXT_SPILL_BYTES);

        let parsed: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(parsed["id"], 99);
        let summary: Value =
            serde_json::from_str(parsed["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
        assert_eq!(summary["offloaded"], true);
        let artifact_path = PathBuf::from(summary["artifact"]["path"].as_str().unwrap());
        assert!(
            artifact_path.starts_with(std::fs::canonicalize(&spill_dir).unwrap()),
            "artifact must be stored inside sandbox tmp dir: {}",
            artifact_path.display()
        );
        let on_disk = std::fs::read_to_string(&artifact_path).unwrap();
        assert_eq!(on_disk, raw);
        assert_eq!(
            summary["artifact"]["bytes"].as_u64().unwrap() as usize,
            raw.len()
        );
        assert_eq!(
            summary["artifact"]["sha256"].as_str().unwrap(),
            anyagents_toolchain::sha256_file(&artifact_path).unwrap()
        );
    }

    #[test]
    fn concurrent_mcp_calls_do_not_head_of_line_block() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                std::thread::spawn(move || {
                    use std::io::BufReader as BufReader_;
                    let mut reader = BufReader_::new(stream);
                    let mut line = String::new();
                    let _ = reader.read_line(&mut line);
                    let mut content_len = 0usize;
                    loop {
                        let mut h = String::new();
                        if reader.read_line(&mut h).is_err()
                            || h == "\r\n"
                            || h == "\n"
                            || h.is_empty()
                        {
                            break;
                        }
                        if let Some((k, v)) = h.split_once(':')
                            && k.trim().eq_ignore_ascii_case("content-length")
                        {
                            content_len = v.trim().parse().unwrap_or(0);
                        }
                    }
                    let mut body = vec![0u8; content_len];
                    if content_len > 0 {
                        let _ = reader.read_exact(&mut body);
                    }
                    let req: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
                    let tool = req
                        .pointer("/params/name")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    if tool == "slow_tool" {
                        std::thread::sleep(Duration::from_millis(350));
                    }
                    let resp = serde_json::to_vec(&json!({
                        "jsonrpc": "2.0",
                        "id": req["id"],
                        "result": { "tool": tool }
                    }))
                    .unwrap();
                    let mut stream = reader.into_inner();
                    let hdr = format!(
                        "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Type: application/json\r\nMcp-Session-Id: sess-conc\r\nContent-Length: {}\r\n\r\n",
                        resp.len()
                    );
                    let _ = stream.write_all(hdr.as_bytes());
                    let _ = stream.write_all(&resp);
                });
            }
        });

        let tmp = tempfile::tempdir().unwrap();
        let args = McpProxyArgs {
            url: Some(format!("http://{addr}/mcp")),
            token_file: None,
            token: Some("cap-conc".into()),
            timeout_secs: 10,
            spill_dir: Some(tmp.path().join("tmp")),
        };
        let input = concat!(
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\"}\n",
            "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/call\",\"params\":{\"name\":\"slow_tool\"}}\n",
            "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"tools/call\",\"params\":{\"name\":\"fast_tool\"}}\n",
        );
        let mut out = Vec::new();
        run_proxy_io(&args, &mut Cursor::new(input.as_bytes()), &mut out).unwrap();
        let parsed: Vec<Value> = std::str::from_utf8(&out)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(parsed.len(), 3);
        assert_eq!(parsed[0]["id"], 1);
        // `fast_tool` (id=3) must complete and flush BEFORE `slow_tool` (id=2).
        assert_eq!(parsed[1]["id"], 3);
        assert_eq!(parsed[2]["id"], 2);
    }
}
