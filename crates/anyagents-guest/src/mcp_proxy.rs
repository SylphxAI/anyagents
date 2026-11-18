//! Stdio JSON-RPC to HTTPS Streamable MCP proxy (`anyagents-guest mcp-proxy`).

use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::time::Duration;

use clap::Args;
use serde_json::{Value, json};

#[derive(Args, Clone, Debug)]
pub struct McpProxyArgs {
    /// Target coordinator MCP URL (typically `https://<host>/mcp?profile=full`).
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
    let mut writer = std::io::BufWriter::new(stdout.lock());
    run_proxy_io(&args, &mut reader, &mut writer)
}

pub(crate) fn run_proxy_io<R: BufRead, W: Write>(
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

    let mut session_id: Option<String> = None;
    let mut line = String::new();
    loop {
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
                writeln!(writer, "{err_resp}").map_err(|e| e.to_string())?;
                writer.flush().map_err(|e| e.to_string())?;
                continue;
            }
        };
        let req_id = req_val.get("id").cloned();
        let token = resolve_token(args);

        match forward_rpc(&http, url, token.as_deref(), &mut session_id, trimmed) {
            Ok(responses) => {
                for resp in responses {
                    writeln!(writer, "{resp}").map_err(|e| e.to_string())?;
                }
                writer.flush().map_err(|e| e.to_string())?;
            }
            Err(err_msg) => {
                if let Some(id) = req_id {
                    let err_resp = json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "error": { "code": -32000, "message": err_msg }
                    });
                    writeln!(writer, "{err_resp}").map_err(|e| e.to_string())?;
                    writer.flush().map_err(|e| e.to_string())?;
                } else {
                    eprintln!("anyagents-guest mcp-proxy notification failed: {err_msg}");
                }
            }
        }
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

fn forward_rpc(
    http: &ureq::Agent,
    url: &str,
    token: Option<&str>,
    session_id: &mut Option<String>,
    body: &str,
) -> Result<Vec<String>, String> {
    let mut req = http
        .post(url)
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream");
    if let Some(tok) = token {
        req = req.header("authorization", &format!("Bearer {tok}"));
    }
    if let Some(sid) = session_id.as_deref() {
        req = req.header("mcp-session-id", sid);
    }
    let resp = req
        .send(body.as_bytes())
        .map_err(|err| format!("MCP POST `{url}` failed: {err}"))?;
    let status = resp.status().as_u16();
    if let Some(sid) = resp
        .headers()
        .get("mcp-session-id")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        *session_id = Some(sid.to_string());
    }
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

    if !(200..300).contains(&status) {
        return Err(format!("MCP server returned HTTP {status}: {}", raw_body.trim()));
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
                        if reader.read_line(&mut h).is_err() || h == "\r\n" || h == "\n" || h.is_empty() {
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
}
