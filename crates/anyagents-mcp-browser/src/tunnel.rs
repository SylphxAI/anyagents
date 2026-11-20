//! Outbound WebSocket reverse-tunnel client for serverless & NAT-traversed
//! `browser_takeover`.
//!
//! Connects outbound to `<ANYAGENTS_API_URL>/v1/takeover/{session_id}/tunnel`
//! (over `wss://` with `rustls` or `ws://` for local development, honoring
//! standard egress proxy env vars when set) using the agent's single-authority
//! coordinator credential (`x-anyagents-agent` or `x-anyagents-operator`).
//!
//! Features:
//! - Proxies HTTP requests (including streaming SSE `/events`) to the local
//!   loopback `agent-workstation` takeover server (`127.0.0.1:<port>`).
//! - Proxies bidirectional binary/text WebSocket sub-streams (`/ws`, preserving
//!   `Sec-WebSocket-Protocol: binary` for noVNC RFB).
//! - Sends `Ping` keepalives every 25s (< 60s) to clear CDN/proxy idle cutoffs.
//! - Transparently auto-reconnects with exponential backoff (`500ms -> 5s`)
//!   reusing the exact same `session_id` and `takeover_token` after proxy
//!   timeouts or rolling coordinator restarts.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use base64::Engine as _;
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http_body_util::{BodyExt as _, Full};
use hyper::http::{HeaderName, HeaderValue, Method};
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, Semaphore, mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use url::Url;

use crate::profile_sync::CoordinatorAuth;

/// Keepalive Ping interval on the outbound reverse-tunnel WebSocket (< 60s).
pub const DEFAULT_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(25);
/// Initial reconnect backoff on unexpected tunnel disconnect.
pub const DEFAULT_INITIAL_BACKOFF: Duration = Duration::from_millis(500);
/// Maximum reconnect backoff cap.
pub const DEFAULT_MAX_BACKOFF: Duration = Duration::from_secs(5);
/// Default timeout for initial tunnel handshake + `Registered` acknowledgement.
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

const MAX_HTTP_RESP_BYTES: usize = 16 * 1024 * 1024;
const MAX_TUNNEL_FRAME_BYTES: usize = 24 * 1024 * 1024;
const HTTP_CHUNK_SIZE: usize = 64 * 1024;
const MAX_CONCURRENT_HTTP: usize = 64;
const MAX_CONCURRENT_WS: usize = 16;
const TUNNEL_OUTBOUND_QUEUE: usize = 256;
const LOCAL_WS_INBOUND_QUEUE: usize = 256;

const BIN_TAG_WS_DATA: u8 = 0x01;
const BIN_TAG_HTTP_CHUNK: u8 = 0x02;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ControlFrame {
    Register {
        token: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        agent_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        local_origin: Option<String>,
    },
    Registered {
        public_base_url: String,
        public_url: String,
        session_id: String,
    },
    Unregister,
    Error {
        code: String,
        message: String,
    },
    HttpRequest {
        id: u64,
        method: String,
        path: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        query: Option<String>,
        #[serde(default)]
        headers: Vec<(String, String)>,
        #[serde(default, skip_serializing_if = "String::is_empty")]
        body_b64: String,
    },
    HttpResponseHead {
        id: u64,
        status: u16,
        #[serde(default)]
        headers: Vec<(String, String)>,
        #[serde(default)]
        done: bool,
    },
    HttpResponseEnd {
        id: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    HttpCancel {
        id: u64,
    },
    WsOpen {
        id: u64,
        path: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        query: Option<String>,
        #[serde(default)]
        protocols: Vec<String>,
        #[serde(default)]
        headers: Vec<(String, String)>,
    },
    WsOpened {
        id: u64,
        ok: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        protocol: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    WsText {
        id: u64,
        text: String,
    },
    WsClose {
        id: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        code: Option<u16>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
}

enum TunnelOutbound {
    Text(String),
    Binary(Bytes),
}

impl TunnelOutbound {
    fn control(frame: &ControlFrame) -> Result<Self, serde_json::Error> {
        serde_json::to_string(frame).map(Self::Text)
    }
}

fn encode_binary_frame(tag: u8, id: u64, payload: &[u8]) -> Bytes {
    let mut buf = Vec::with_capacity(9 + payload.len());
    buf.push(tag);
    buf.extend_from_slice(&id.to_be_bytes());
    buf.extend_from_slice(payload);
    Bytes::from(buf)
}

fn decode_binary_frame(raw: &[u8]) -> Option<(u8, u64, Bytes)> {
    if raw.len() < 9 {
        return None;
    }
    let tag = raw[0];
    let id = u64::from_be_bytes(raw[1..9].try_into().ok()?);
    Some((tag, id, Bytes::copy_from_slice(&raw[9..])))
}

/// Configuration for an outbound takeover reverse-tunnel client.
#[derive(Clone)]
pub struct TunnelConfig {
    pub api_url: String,
    pub auth: CoordinatorAuth,
    pub local_addr: SocketAddr,
    pub takeover_token: String,
    pub session_id: Option<String>,
    pub agent_id: Option<String>,
    pub connect_timeout: Duration,
    pub keepalive_interval: Duration,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl TunnelConfig {
    pub fn new(
        api_url: String,
        auth: CoordinatorAuth,
        local_addr: SocketAddr,
        takeover_token: String,
    ) -> Self {
        Self {
            api_url,
            auth,
            local_addr,
            takeover_token,
            session_id: None,
            agent_id: None,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            keepalive_interval: DEFAULT_KEEPALIVE_INTERVAL,
            initial_backoff: DEFAULT_INITIAL_BACKOFF,
            max_backoff: DEFAULT_MAX_BACKOFF,
        }
    }
}

impl std::fmt::Debug for TunnelConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TunnelConfig")
            .field("api_url", &self.api_url)
            .field("auth", &self.auth)
            .field("local_addr", &self.local_addr)
            .field("takeover_token", &"[redacted]")
            .field("session_id", &self.session_id)
            .finish()
    }
}

/// Running background handle for an outbound takeover reverse-tunnel.
/// Dropping the handle signals clean shutdown (`Unregister` + close).
pub struct TakeoverTunnelHandle {
    session_id: String,
    public_base_url: String,
    public_url: String,
    stop_tx: watch::Sender<bool>,
    connected: Arc<AtomicBool>,
    reconnect_count: Arc<AtomicU64>,
    task: Option<JoinHandle<()>>,
}

impl std::fmt::Debug for TakeoverTunnelHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TakeoverTunnelHandle")
            .field("session_id", &self.session_id)
            .field("public_base_url", &self.public_base_url)
            .field("connected", &self.is_connected())
            .field("reconnect_count", &self.reconnect_count())
            .finish()
    }
}

impl TakeoverTunnelHandle {
    /// Connect to the coordinator, register this takeover session, and spawn the
    /// resilient background tunnel + auto-reconnect supervisor task.
    pub async fn start(config: TunnelConfig) -> Result<Self, String> {
        let session_id = match config.session_id.as_deref().map(str::trim) {
            Some(id) if is_valid_session_id(id) => id.to_string(),
            Some(bad) => return Err(format!("invalid takeover session id `{bad}`")),
            None => generate_session_id(),
        };
        let ws_url = build_tunnel_ws_url(&config.api_url, &session_id)?;
        let (ws, reg) = tokio::time::timeout(
            config.connect_timeout,
            connect_and_register(
                &ws_url,
                &config.auth,
                &config.takeover_token,
                config.agent_id.as_deref(),
                config.local_addr,
            ),
        )
        .await
        .map_err(|_| {
            format!(
                "timed out connecting takeover reverse-tunnel after {}s",
                config.connect_timeout.as_secs()
            )
        })??;

        let (stop_tx, stop_rx) = watch::channel(false);
        let connected = Arc::new(AtomicBool::new(true));
        let reconnect_count = Arc::new(AtomicU64::new(0));
        let public_base_url = reg.public_base_url.clone();
        let public_url = reg.public_url.clone();

        let task = tokio::spawn(run_tunnel_supervisor(SupervisorCtx {
            ws_url,
            config,
            stop_rx,
            connected: Arc::clone(&connected),
            reconnect_count: Arc::clone(&reconnect_count),
            initial_ws: ws,
        }));

        Ok(Self {
            session_id,
            public_base_url,
            public_url,
            stop_tx,
            connected,
            reconnect_count,
            task: Some(task),
        })
    }

    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    pub fn public_base_url(&self) -> &str {
        &self.public_base_url
    }

    pub fn public_url(&self) -> &str {
        &self.public_url
    }

    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }

    pub fn reconnect_count(&self) -> u64 {
        self.reconnect_count.load(Ordering::Acquire)
    }

    /// Gracefully send `Unregister`, close the tunnel WebSocket, and wait briefly
    /// for the supervisor task to exit.
    pub async fn stop(mut self) {
        let _ = self.stop_tx.send(true);
        if let Some(task) = self.task.take() {
            let _ = tokio::time::timeout(Duration::from_secs(3), task).await;
        }
    }
}

impl Drop for TakeoverTunnelHandle {
    fn drop(&mut self) {
        let _ = self.stop_tx.send(true);
    }
}

pub fn is_valid_session_id(seg: &str) -> bool {
    let bytes = seg.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 64
        && bytes[0].is_ascii_alphanumeric()
        && bytes[1..]
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
}

fn generate_session_id() -> String {
    let mut bytes = [0u8; 16];
    let filled_from_os = {
        #[cfg(unix)]
        {
            use std::io::Read as _;
            std::fs::File::open("/dev/urandom")
                .and_then(|mut f| f.read_exact(&mut bytes))
                .is_ok()
        }
        #[cfg(not(unix))]
        {
            false
        }
    };
    if !filled_from_os {
        use std::collections::hash_map::RandomState;
        use std::hash::{BuildHasher as _, Hasher as _};
        let mut h1 = RandomState::new().build_hasher();
        h1.write_u32(std::process::id());
        let a = h1.finish().to_be_bytes();
        let mut h2 = RandomState::new().build_hasher();
        h2.write_u64(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0),
        );
        let b = h2.finish().to_be_bytes();
        bytes[..8].copy_from_slice(&a);
        bytes[8..].copy_from_slice(&b);
    }
    let mut hex = String::with_capacity(3 + 32);
    hex.push_str("tk_");
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut hex, "{byte:02x}");
    }
    hex
}

pub fn build_tunnel_ws_url(api_url: &str, session_id: &str) -> Result<Url, String> {
    let mut parsed = Url::parse(api_url.trim())
        .map_err(|e| format!("invalid coordinator API URL `{api_url}`: {e}"))?;
    let ws_scheme = match parsed.scheme() {
        "http" | "ws" => "ws",
        "https" | "wss" => "wss",
        other => return Err(format!("unsupported coordinator URL scheme `{other}`")),
    };
    parsed
        .set_scheme(ws_scheme)
        .map_err(|()| "failed setting websocket URL scheme".to_string())?;
    let base_path = parsed.path().trim_end_matches('/');
    parsed.set_path(&format!("{base_path}/v1/takeover/{session_id}/tunnel"));
    parsed.set_query(None);
    parsed.set_fragment(None);
    Ok(parsed)
}

enum TunnelIo {
    Plain(TcpStream),
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl AsyncRead for TunnelIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match &mut *self {
            Self::Plain(s) => Pin::new(s).poll_read(cx, buf),
            Self::Tls(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for TunnelIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match &mut *self {
            Self::Plain(s) => Pin::new(s).poll_write(cx, buf),
            Self::Tls(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match &mut *self {
            Self::Plain(s) => Pin::new(s).poll_flush(cx),
            Self::Tls(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match &mut *self {
            Self::Plain(s) => Pin::new(s).poll_shutdown(cx),
            Self::Tls(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

fn should_bypass_egress_proxy(host: &str) -> bool {
    let h = host.trim_matches(['[', ']']).to_ascii_lowercase();
    if matches!(h.as_str(), "localhost" | "127.0.0.1" | "::1") {
        return true;
    }
    let no_proxy = std::env::var("NO_PROXY")
        .ok()
        .or_else(|| std::env::var("no_proxy").ok())
        .unwrap_or_default();
    for raw in no_proxy.split(',') {
        let rule = raw.trim().to_ascii_lowercase();
        if rule.is_empty() {
            continue;
        }
        if rule == "*" {
            return true;
        }
        let rule_host = rule.split(':').next().unwrap_or(&rule);
        if let Some(suffix) = rule_host.strip_prefix('.') {
            if h == suffix || h.ends_with(rule_host) {
                return true;
            }
        } else if h == rule_host || h.ends_with(&format!(".{rule_host}")) {
            return true;
        }
    }
    false
}

fn resolve_egress_proxy(is_tls: bool, target_host: &str) -> Option<Url> {
    if should_bypass_egress_proxy(target_host) {
        return None;
    }
    let candidates: &[&str] = if is_tls {
        &["HTTPS_PROXY", "https_proxy", "ALL_PROXY", "all_proxy"]
    } else {
        &["HTTP_PROXY", "http_proxy", "ALL_PROXY", "all_proxy"]
    };
    for key in candidates {
        if let Ok(raw) = std::env::var(key) {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                continue;
            }
            let with_scheme = if trimmed.contains("://") {
                trimmed.to_string()
            } else {
                format!("http://{trimmed}")
            };
            if let Ok(url) = Url::parse(&with_scheme)
                && matches!(url.scheme(), "http" | "https")
                && url.host_str().is_some()
            {
                return Some(url);
            }
        }
    }
    None
}

async fn connect_via_http_connect_proxy(
    proxy: &Url,
    target_host: &str,
    target_port: u16,
) -> Result<TcpStream, String> {
    let proxy_host = proxy.host_str().ok_or("proxy URL missing host")?;
    let proxy_port = proxy
        .port_or_known_default()
        .ok_or("proxy URL missing port")?;
    let mut stream = TcpStream::connect((proxy_host, proxy_port))
        .await
        .map_err(|e| format!("failed connecting to egress proxy {proxy_host}:{proxy_port}: {e}"))?;

    let authority = if target_host.contains(':') {
        format!("[{target_host}]:{target_port}")
    } else {
        format!("{target_host}:{target_port}")
    };
    let mut req = format!("CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\n");
    if !proxy.username().is_empty() || proxy.password().is_some() {
        let raw_cred = format!("{}:{}", proxy.username(), proxy.password().unwrap_or(""));
        let b64 = base64::engine::general_purpose::STANDARD.encode(raw_cred.as_bytes());
        req.push_str(&format!("Proxy-Authorization: Basic {b64}\r\n"));
    }
    req.push_str("\r\n");
    stream
        .write_all(req.as_bytes())
        .await
        .map_err(|e| format!("egress proxy CONNECT write failed: {e}"))?;

    // Read response header byte-by-byte up to `\r\n\r\n` so we never over-read
    // into the tunneled TLS/WebSocket bytes.
    let mut head = Vec::with_capacity(256);
    let mut byte = [0u8; 1];
    loop {
        stream
            .read_exact(&mut byte)
            .await
            .map_err(|e| format!("egress proxy CONNECT read failed: {e}"))?;
        head.push(byte[0]);
        if head.ends_with(b"\r\n\r\n") {
            break;
        }
        if head.len() > 16 * 1024 {
            return Err("egress proxy CONNECT response headers too large".into());
        }
    }
    let status_line = String::from_utf8_lossy(&head)
        .lines()
        .next()
        .unwrap_or("")
        .to_string();
    let code = status_line.split_whitespace().nth(1).unwrap_or("");
    if code != "200" {
        return Err(format!("egress proxy CONNECT rejected: {status_line}"));
    }
    Ok(stream)
}

fn build_rustls_connector() -> Result<tokio_rustls::TlsConnector, String> {
    let roots = rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let cfg = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|e| format!("TLS protocol config error: {e}"))?
    .with_root_certificates(roots)
    .with_no_client_auth();
    Ok(tokio_rustls::TlsConnector::from(Arc::new(cfg)))
}

async fn dial_tunnel_ws(
    ws_url: &Url,
    auth: &CoordinatorAuth,
) -> Result<WebSocketStream<TunnelIo>, String> {
    let host = ws_url
        .host_str()
        .ok_or("tunnel URL missing host")?
        .to_string();
    let is_tls = ws_url.scheme() == "wss";
    let port = ws_url
        .port_or_known_default()
        .ok_or("tunnel URL missing port")?;

    let tcp = if let Some(proxy) = resolve_egress_proxy(is_tls, &host) {
        connect_via_http_connect_proxy(&proxy, &host, port).await?
    } else {
        TcpStream::connect((host.as_str(), port))
            .await
            .map_err(|e| format!("failed connecting to coordinator {host}:{port}: {e}"))?
    };
    let _ = tcp.set_nodelay(true);

    let io = if is_tls {
        let connector = build_rustls_connector()?;
        let server_name =
            rustls_pki_types::ServerName::try_from(host.trim_matches(['[', ']']).to_string())
                .map_err(|e| format!("invalid TLS server name `{host}`: {e}"))?;
        let tls = connector
            .connect(server_name, tcp)
            .await
            .map_err(|e| format!("TLS handshake with coordinator failed: {e}"))?;
        TunnelIo::Tls(Box::new(tls))
    } else {
        TunnelIo::Plain(tcp)
    };

    let mut req = ws_url
        .as_str()
        .into_client_request()
        .map_err(|e| format!("invalid tunnel websocket request: {e}"))?;
    for (k, v) in auth.headers() {
        let name = HeaderName::from_bytes(k.as_bytes())
            .map_err(|e| format!("invalid auth header name: {e}"))?;
        let val = HeaderValue::from_str(&v).map_err(|e| format!("invalid auth header: {e}"))?;
        req.headers_mut().insert(name, val);
    }

    let mut ws_cfg = WebSocketConfig::default();
    ws_cfg.max_message_size = Some(MAX_TUNNEL_FRAME_BYTES);
    ws_cfg.max_frame_size = Some(MAX_TUNNEL_FRAME_BYTES);

    let (ws, _resp) = tokio_tungstenite::client_async_with_config(req, io, Some(ws_cfg))
        .await
        .map_err(|e| format!("coordinator takeover tunnel handshake failed: {e}"))?;
    Ok(ws)
}

struct RegisteredAck {
    public_base_url: String,
    public_url: String,
}

async fn connect_and_register(
    ws_url: &Url,
    auth: &CoordinatorAuth,
    takeover_token: &str,
    agent_id: Option<&str>,
    local_addr: SocketAddr,
) -> Result<(WebSocketStream<TunnelIo>, RegisteredAck), String> {
    let mut ws = dial_tunnel_ws(ws_url, auth).await?;
    let register = ControlFrame::Register {
        token: takeover_token.to_string(),
        agent_id: agent_id.map(str::to_string),
        local_origin: Some(format!("http://{local_addr}")),
    };
    let text = serde_json::to_string(&register)
        .map_err(|e| format!("failed serializing register frame: {e}"))?;
    ws.send(WsMessage::Text(text.into()))
        .await
        .map_err(|e| format!("failed sending register frame: {e}"))?;

    while let Some(msg) = ws.next().await {
        match msg {
            Ok(WsMessage::Text(txt)) => {
                let frame: ControlFrame = serde_json::from_str(&txt)
                    .map_err(|e| format!("invalid coordinator registration reply: {e}"))?;
                match frame {
                    ControlFrame::Registered {
                        public_base_url,
                        public_url,
                        ..
                    } => {
                        return Ok((
                            ws,
                            RegisteredAck {
                                public_base_url,
                                public_url,
                            },
                        ));
                    }
                    ControlFrame::Error { code, message } => {
                        return Err(format!("coordinator rejected tunnel ({code}): {message}"));
                    }
                    _ => return Err("unexpected control frame during tunnel registration".into()),
                }
            }
            Ok(WsMessage::Ping(p)) => {
                let _ = ws.send(WsMessage::Pong(p)).await;
            }
            Ok(WsMessage::Pong(_)) => {}
            Ok(WsMessage::Close(cf)) => {
                let reason = cf
                    .map(|c| format!("{} {}", c.code, c.reason))
                    .unwrap_or_else(|| "closed".into());
                return Err(format!(
                    "coordinator closed tunnel during register: {reason}"
                ));
            }
            Ok(_) => {}
            Err(e) => return Err(format!("tunnel register read failed: {e}")),
        }
    }
    Err("coordinator closed tunnel before sending Registered acknowledgement".into())
}

struct SupervisorCtx {
    ws_url: Url,
    config: TunnelConfig,
    stop_rx: watch::Receiver<bool>,
    connected: Arc<AtomicBool>,
    reconnect_count: Arc<AtomicU64>,
    initial_ws: WebSocketStream<TunnelIo>,
}

async fn wait_stopped(rx: &mut watch::Receiver<bool>) {
    let _ = rx.wait_for(|s| *s).await;
}

async fn run_tunnel_supervisor(mut ctx: SupervisorCtx) {
    let mut current_ws = Some(ctx.initial_ws);
    let mut backoff = ctx.config.initial_backoff;

    loop {
        if let Some(ws) = current_ws.take() {
            ctx.connected.store(true, Ordering::Release);
            backoff = ctx.config.initial_backoff;
            let stopped = pump_active_tunnel(ws, &ctx.config, &mut ctx.stop_rx).await;
            ctx.connected.store(false, Ordering::Release);
            if stopped || *ctx.stop_rx.borrow() {
                return;
            }
        }

        // Transparent auto-reconnect with exponential backoff (default 500ms -> 5s),
        // reusing the exact same session_id + takeover_token.
        tokio::select! {
            () = wait_stopped(&mut ctx.stop_rx) => return,
            _ = tokio::time::sleep(backoff) => {}
        }
        if *ctx.stop_rx.borrow() {
            return;
        }

        let attempt = tokio::time::timeout(
            ctx.config.connect_timeout,
            connect_and_register(
                &ctx.ws_url,
                &ctx.config.auth,
                &ctx.config.takeover_token,
                ctx.config.agent_id.as_deref(),
                ctx.config.local_addr,
            ),
        )
        .await;
        match attempt {
            Ok(Ok((ws, _ack))) => {
                ctx.reconnect_count.fetch_add(1, Ordering::AcqRel);
                current_ws = Some(ws);
            }
            Ok(Err(err)) if err.contains("(FORBIDDEN)") || err.contains("(UNAUTHORIZED)") => {
                eprintln!("anyagents-mcp-browser: takeover tunnel stopped permanently: {err}");
                return;
            }
            Ok(Err(err)) => {
                eprintln!(
                    "anyagents-mcp-browser: takeover tunnel reconnect failed ({err}); retrying in {}ms",
                    backoff.as_millis()
                );
                backoff = backoff.saturating_mul(2).min(ctx.config.max_backoff);
            }
            Err(_) => {
                eprintln!(
                    "anyagents-mcp-browser: takeover tunnel reconnect timed out; retrying in {}ms",
                    backoff.as_millis()
                );
                backoff = backoff.saturating_mul(2).min(ctx.config.max_backoff);
            }
        }
    }
}

enum LocalWsOutbound {
    Binary(Bytes),
    Text(String),
    Close(Option<tokio_tungstenite::tungstenite::protocol::CloseFrame>),
}

async fn pump_active_tunnel(
    ws: WebSocketStream<TunnelIo>,
    config: &TunnelConfig,
    stop_rx: &mut watch::Receiver<bool>,
) -> bool {
    let (mut ws_tx, mut ws_rx) = ws.split();
    let (out_tx, mut out_rx) = mpsc::channel::<TunnelOutbound>(TUNNEL_OUTBOUND_QUEUE);
    let http_cancels: Arc<Mutex<HashMap<u64, oneshot::Sender<()>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let ws_streams: Arc<Mutex<HashMap<u64, mpsc::Sender<LocalWsOutbound>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let http_sem = Arc::new(Semaphore::new(MAX_CONCURRENT_HTTP));
    let ws_sem = Arc::new(Semaphore::new(MAX_CONCURRENT_WS));

    let mut ping_interval = tokio::time::interval_at(
        tokio::time::Instant::now() + config.keepalive_interval,
        config.keepalive_interval,
    );
    ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let mut stopped_by_caller = false;
    loop {
        tokio::select! {
            () = wait_stopped(stop_rx) => {
                stopped_by_caller = true;
                if let Ok(txt) = serde_json::to_string(&ControlFrame::Unregister) {
                    let _ = ws_tx.send(WsMessage::Text(txt.into())).await;
                }
                let _ = ws_tx.send(WsMessage::Close(Some(
                    tokio_tungstenite::tungstenite::protocol::CloseFrame {
                        code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Normal,
                        reason: "takeover_stopped".into(),
                    },
                ))).await;
                let _ = ws_tx.flush().await;
                break;
            }
            _ = ping_interval.tick() => {
                if ws_tx.send(WsMessage::Ping(Bytes::from_static(b"keepalive"))).await.is_err() {
                    break;
                }
            }
            outbound = out_rx.recv() => {
                let Some(msg) = outbound else {
                    break;
                };
                let wire = match msg {
                    TunnelOutbound::Text(t) => WsMessage::Text(t.into()),
                    TunnelOutbound::Binary(b) => WsMessage::Binary(b),
                };
                if ws_tx.send(wire).await.is_err() {
                    break;
                }
            }
            incoming = ws_rx.next() => {
                match incoming {
                    Some(Ok(WsMessage::Text(txt))) => {
                        let Ok(frame) = serde_json::from_str::<ControlFrame>(&txt) else {
                            continue;
                        };
                        match frame {
                            ControlFrame::HttpRequest {
                                id,
                                method,
                                path,
                                query,
                                headers,
                                body_b64,
                            } => {
                                let Ok(permit) = Arc::clone(&http_sem).try_acquire_owned() else {
                                    send_http_error(&out_tx, id, 503, "too many concurrent requests").await;
                                    continue;
                                };
                                let (cancel_tx, cancel_rx) = oneshot::channel();
                                http_cancels.lock().await.insert(id, cancel_tx);
                                let cancels = Arc::clone(&http_cancels);
                                let out = out_tx.clone();
                                let local_addr = config.local_addr;
                                let token = config.takeover_token.clone();
                                tokio::spawn(async move {
                                    let _permit = permit;
                                    proxy_local_http(
                                        local_addr,
                                        &token,
                                        id,
                                        &method,
                                        &path,
                                        query.as_deref(),
                                        headers,
                                        &body_b64,
                                        out,
                                        cancel_rx,
                                    )
                                    .await;
                                    cancels.lock().await.remove(&id);
                                });
                            }
                            ControlFrame::HttpCancel { id } => {
                                if let Some(tx) = http_cancels.lock().await.remove(&id) {
                                    let _ = tx.send(());
                                }
                            }
                            ControlFrame::WsOpen {
                                id,
                                path,
                                query,
                                protocols,
                                headers,
                            } => {
                                let Ok(permit) = Arc::clone(&ws_sem).try_acquire_owned() else {
                                    if let Ok(msg) = TunnelOutbound::control(&ControlFrame::WsOpened {
                                        id,
                                        ok: false,
                                        protocol: None,
                                        error: Some("too many concurrent viewer websockets".into()),
                                    }) {
                                        let _ = out_tx.send(msg).await;
                                    }
                                    continue;
                                };
                                let (stream_tx, stream_rx) = mpsc::channel(LOCAL_WS_INBOUND_QUEUE);
                                ws_streams.lock().await.insert(id, stream_tx);
                                let streams = Arc::clone(&ws_streams);
                                let out = out_tx.clone();
                                let local_addr = config.local_addr;
                                let token = config.takeover_token.clone();
                                tokio::spawn(async move {
                                    let _permit = permit;
                                    proxy_local_ws(
                                        local_addr,
                                        &token,
                                        id,
                                        &path,
                                        query.as_deref(),
                                        protocols,
                                        headers,
                                        out,
                                        stream_rx,
                                    )
                                    .await;
                                    streams.lock().await.remove(&id);
                                });
                            }
                            ControlFrame::WsText { id, text } => {
                                let tx = ws_streams.lock().await.get(&id).cloned();
                                if let Some(tx) = tx {
                                    let _ = tx.send(LocalWsOutbound::Text(text)).await;
                                }
                            }
                            ControlFrame::WsClose { id, code, reason } => {
                                let tx = ws_streams.lock().await.remove(&id);
                                if let Some(tx) = tx {
                                    let cf = code.map(|c| {
                                        tokio_tungstenite::tungstenite::protocol::CloseFrame {
                                            code: c.into(),
                                            reason: reason.unwrap_or_default().into(),
                                        }
                                    });
                                    let _ = tx.send(LocalWsOutbound::Close(cf)).await;
                                }
                            }
                            _ => {}
                        }
                    }
                    Some(Ok(WsMessage::Binary(raw))) => {
                        if let Some((BIN_TAG_WS_DATA, id, payload)) = decode_binary_frame(&raw) {
                            let tx = ws_streams.lock().await.get(&id).cloned();
                            if let Some(tx) = tx {
                                let _ = tx.send(LocalWsOutbound::Binary(payload)).await;
                            }
                        }
                    }
                    Some(Ok(WsMessage::Ping(p))) => {
                        if ws_tx.send(WsMessage::Pong(p)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(WsMessage::Pong(_))) => {}
                    Some(Ok(WsMessage::Close(_))) | Some(Err(_)) | None => break,
                    Some(Ok(WsMessage::Frame(_))) => {}
                }
            }
        }
    }

    // Abort all active local HTTP streams & close all local WS sub-streams from this tunnel generation.
    for (_, tx) in http_cancels.lock().await.drain() {
        let _ = tx.send(());
    }
    for (_, tx) in ws_streams.lock().await.drain() {
        let _ = tx.try_send(LocalWsOutbound::Close(None));
    }

    stopped_by_caller
}

fn build_local_path_and_query(path: &str, query: Option<&str>, token: &str) -> String {
    let clean_path = if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    };
    let has_token =
        query.is_some_and(|q| url::form_urlencoded::parse(q.as_bytes()).any(|(k, _)| k == "token"));
    let encoded_token: String = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("token", token)
        .finish();
    match query {
        Some(q) if !q.is_empty() && has_token => format!("{clean_path}?{q}"),
        Some(q) if !q.is_empty() => format!("{clean_path}?{q}&{encoded_token}"),
        _ => format!("{clean_path}?{encoded_token}"),
    }
}

async fn send_http_error(
    out_tx: &mpsc::Sender<TunnelOutbound>,
    id: u64,
    status: u16,
    message: &str,
) {
    let head = ControlFrame::HttpResponseHead {
        id,
        status,
        headers: vec![("content-type".into(), "text/plain; charset=utf-8".into())],
        done: false,
    };
    if let Ok(msg) = TunnelOutbound::control(&head)
        && out_tx.send(msg).await.is_ok()
    {
        let chunk = encode_binary_frame(BIN_TAG_HTTP_CHUNK, id, message.as_bytes());
        let _ = out_tx.send(TunnelOutbound::Binary(chunk)).await;
        if let Ok(end) = TunnelOutbound::control(&ControlFrame::HttpResponseEnd { id, error: None })
        {
            let _ = out_tx.send(end).await;
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn proxy_local_http(
    local_addr: SocketAddr,
    token: &str,
    id: u64,
    method: &str,
    path: &str,
    query: Option<&str>,
    headers: Vec<(String, String)>,
    body_b64: &str,
    out_tx: mpsc::Sender<TunnelOutbound>,
    mut cancel_rx: oneshot::Receiver<()>,
) {
    let body_bytes = if body_b64.is_empty() {
        Bytes::new()
    } else {
        match base64::engine::general_purpose::STANDARD.decode(body_b64) {
            Ok(b) => Bytes::from(b),
            Err(e) => {
                send_http_error(&out_tx, id, 400, &format!("invalid request body: {e}")).await;
                return;
            }
        }
    };
    let Ok(http_method) = Method::from_bytes(method.as_bytes()) else {
        send_http_error(&out_tx, id, 400, "invalid HTTP method").await;
        return;
    };

    let stream = match TcpStream::connect(local_addr).await {
        Ok(s) => s,
        Err(e) => {
            send_http_error(
                &out_tx,
                id,
                502,
                &format!("local takeover unreachable: {e}"),
            )
            .await;
            return;
        }
    };
    let (mut sender, conn) = match hyper::client::conn::http1::handshake(TokioIo::new(stream)).await
    {
        Ok(pair) => pair,
        Err(e) => {
            send_http_error(
                &out_tx,
                id,
                502,
                &format!("local HTTP handshake failed: {e}"),
            )
            .await;
            return;
        }
    };
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let uri = build_local_path_and_query(path, query, token);
    let mut req_builder = hyper::Request::builder()
        .method(http_method)
        .uri(&uri)
        .header("host", local_addr.to_string())
        .header("authorization", format!("Bearer {token}"));
    for (k, v) in headers {
        let lower = k.to_ascii_lowercase();
        if matches!(
            lower.as_str(),
            "host" | "authorization" | "connection" | "transfer-encoding" | "upgrade"
        ) {
            continue;
        }
        if let (Ok(name), Ok(val)) = (
            HeaderName::from_bytes(lower.as_bytes()),
            HeaderValue::from_str(&v),
        ) {
            req_builder = req_builder.header(name, val);
        }
    }
    let req = match req_builder.body(Full::new(body_bytes)) {
        Ok(r) => r,
        Err(e) => {
            send_http_error(&out_tx, id, 500, &format!("failed building local req: {e}")).await;
            return;
        }
    };

    let resp = tokio::select! {
        _ = &mut cancel_rx => return,
        res = sender.send_request(req) => match res {
            Ok(r) => r,
            Err(e) => {
                send_http_error(&out_tx, id, 502, &format!("local takeover request failed: {e}")).await;
                return;
            }
        }
    };

    let status = resp.status().as_u16();
    let mut is_sse = false;
    let mut resp_headers = Vec::new();
    for (k, v) in resp.headers() {
        let lower = k.as_str().to_ascii_lowercase();
        if matches!(
            lower.as_str(),
            "connection" | "keep-alive" | "transfer-encoding" | "upgrade" | "te" | "trailer"
        ) {
            continue;
        }
        if let Ok(s) = v.to_str() {
            if lower == "content-type" && s.to_ascii_lowercase().starts_with("text/event-stream") {
                is_sse = true;
            }
            resp_headers.push((lower, s.to_string()));
        }
    }

    let head = ControlFrame::HttpResponseHead {
        id,
        status,
        headers: resp_headers,
        done: false,
    };
    let Ok(head_msg) = TunnelOutbound::control(&head) else {
        return;
    };
    if out_tx.send(head_msg).await.is_err() {
        return;
    }

    let mut body = resp.into_body();
    let mut total_bytes = 0usize;
    let mut stream_error: Option<String> = None;

    loop {
        let next_frame = tokio::select! {
            _ = &mut cancel_rx => return,
            frame = body.frame() => frame,
        };
        match next_frame {
            Some(Ok(frame)) => {
                if let Ok(data) = frame.into_data() {
                    if data.is_empty() {
                        continue;
                    }
                    if !is_sse {
                        total_bytes = total_bytes.saturating_add(data.len());
                        if total_bytes > MAX_HTTP_RESP_BYTES {
                            stream_error = Some("upstream response exceeded 16 MiB limit".into());
                            break;
                        }
                    }
                    for slice in data.chunks(HTTP_CHUNK_SIZE) {
                        let bin = encode_binary_frame(BIN_TAG_HTTP_CHUNK, id, slice);
                        if out_tx.send(TunnelOutbound::Binary(bin)).await.is_err() {
                            return;
                        }
                    }
                }
            }
            Some(Err(err)) => {
                stream_error = Some(format!("local takeover body read failed: {err}"));
                break;
            }
            None => break,
        }
    }

    if let Ok(end) = TunnelOutbound::control(&ControlFrame::HttpResponseEnd {
        id,
        error: stream_error,
    }) {
        let _ = out_tx.send(end).await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn proxy_local_ws(
    local_addr: SocketAddr,
    token: &str,
    id: u64,
    path: &str,
    query: Option<&str>,
    protocols: Vec<String>,
    headers: Vec<(String, String)>,
    out_tx: mpsc::Sender<TunnelOutbound>,
    mut inbound_rx: mpsc::Receiver<LocalWsOutbound>,
) {
    let path_and_query = build_local_path_and_query(path, query, token);
    let url = format!("ws://{local_addr}{path_and_query}");
    let mut req = match url.into_client_request() {
        Ok(r) => r,
        Err(e) => {
            if let Ok(m) = TunnelOutbound::control(&ControlFrame::WsOpened {
                id,
                ok: false,
                protocol: None,
                error: Some(format!("invalid local ws url: {e}")),
            }) {
                let _ = out_tx.send(m).await;
            }
            return;
        }
    };
    if let Ok(auth_val) = HeaderValue::from_str(&format!("Bearer {token}")) {
        req.headers_mut().insert("authorization", auth_val);
    }
    if !protocols.is_empty()
        && let Ok(proto_val) = HeaderValue::from_str(&protocols.join(", "))
    {
        req.headers_mut()
            .insert("sec-websocket-protocol", proto_val);
    }
    for (k, v) in headers {
        let lower = k.to_ascii_lowercase();
        if matches!(
            lower.as_str(),
            "host"
                | "authorization"
                | "connection"
                | "upgrade"
                | "sec-websocket-key"
                | "sec-websocket-version"
                | "sec-websocket-protocol"
                | "sec-websocket-extensions"
        ) {
            continue;
        }
        if let (Ok(name), Ok(val)) = (
            HeaderName::from_bytes(lower.as_bytes()),
            HeaderValue::from_str(&v),
        ) {
            req.headers_mut().insert(name, val);
        }
    }

    let mut ws_cfg = WebSocketConfig::default();
    ws_cfg.max_message_size = Some(MAX_TUNNEL_FRAME_BYTES);
    ws_cfg.max_frame_size = Some(MAX_HTTP_RESP_BYTES);

    let (local_ws, resp) =
        match tokio_tungstenite::connect_async_with_config(req, Some(ws_cfg), false).await {
            Ok(pair) => pair,
            Err(e) => {
                if let Ok(m) = TunnelOutbound::control(&ControlFrame::WsOpened {
                    id,
                    ok: false,
                    protocol: None,
                    error: Some(format!("local takeover websocket dial failed: {e}")),
                }) {
                    let _ = out_tx.send(m).await;
                }
                return;
            }
        };

    let negotiated = resp
        .headers()
        .get("sec-websocket-protocol")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    if let Ok(opened) = TunnelOutbound::control(&ControlFrame::WsOpened {
        id,
        ok: true,
        protocol: negotiated,
        error: None,
    }) && out_tx.send(opened).await.is_err()
    {
        return;
    }

    let (mut local_tx, mut local_rx) = local_ws.split();
    loop {
        tokio::select! {
            from_viewer = inbound_rx.recv() => {
                match from_viewer {
                    Some(LocalWsOutbound::Binary(b)) => {
                        if local_tx.send(WsMessage::Binary(b)).await.is_err() {
                            break;
                        }
                    }
                    Some(LocalWsOutbound::Text(t)) => {
                        if local_tx.send(WsMessage::Text(t.into())).await.is_err() {
                            break;
                        }
                    }
                    Some(LocalWsOutbound::Close(cf)) => {
                        let _ = local_tx.send(WsMessage::Close(cf)).await;
                        break;
                    }
                    None => {
                        let _ = local_tx.send(WsMessage::Close(None)).await;
                        break;
                    }
                }
            }
            from_local = local_rx.next() => {
                match from_local {
                    Some(Ok(WsMessage::Binary(b))) => {
                        let frame = encode_binary_frame(BIN_TAG_WS_DATA, id, &b);
                        if out_tx.send(TunnelOutbound::Binary(frame)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(WsMessage::Text(t))) => {
                        let Ok(msg) = TunnelOutbound::control(&ControlFrame::WsText {
                            id,
                            text: t.to_string(),
                        }) else {
                            break;
                        };
                        if out_tx.send(msg).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(WsMessage::Ping(p))) => {
                        let _ = local_tx.send(WsMessage::Pong(p)).await;
                    }
                    Some(Ok(WsMessage::Pong(_))) => {}
                    Some(Ok(WsMessage::Close(cf))) => {
                        let (code, reason) = cf
                            .map(|c| (Some(u16::from(c.code)), Some(c.reason.to_string())))
                            .unwrap_or((None, None));
                        if let Ok(msg) = TunnelOutbound::control(&ControlFrame::WsClose {
                            id,
                            code,
                            reason,
                        }) {
                            let _ = out_tx.send(msg).await;
                        }
                        break;
                    }
                    Some(Err(_)) | None => {
                        if let Ok(msg) = TunnelOutbound::control(&ControlFrame::WsClose {
                            id,
                            code: None,
                            reason: None,
                        }) {
                            let _ = out_tx.send(msg).await;
                        }
                        break;
                    }
                    Some(Ok(WsMessage::Frame(_))) => {}
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::extract::ws::{Message as AxumWsMsg, WebSocket, WebSocketUpgrade};
    use axum::extract::{Path as AxumPath, Query, State};
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use axum::routing::get;

    #[test]
    fn url_builder_and_query_token_helpers() {
        let u = build_tunnel_ws_url("https://coord.example.com/base/", "tk_abc_123").unwrap();
        assert_eq!(
            u.as_str(),
            "wss://coord.example.com/base/v1/takeover/tk_abc_123/tunnel"
        );
        let u2 = build_tunnel_ws_url("http://127.0.0.1:8789", "tk_local").unwrap();
        assert_eq!(
            u2.as_str(),
            "ws://127.0.0.1:8789/v1/takeover/tk_local/tunnel"
        );
        assert!(build_tunnel_ws_url("ftp://example.com", "tk_1").is_err());

        assert_eq!(
            build_local_path_and_query("/app.js", None, "tok123"),
            "/app.js?token=tok123"
        );
        assert_eq!(
            build_local_path_and_query("/state", Some("a=1"), "tok123"),
            "/state?a=1&token=tok123"
        );
        assert_eq!(
            build_local_path_and_query("/", Some("token=tok123"), "tok123"),
            "/?token=tok123"
        );
        assert!(is_valid_session_id(&generate_session_id()));
    }

    type HttpReplySlot = Arc<Mutex<Option<oneshot::Sender<(u16, String)>>>>;
    type WsReplySlot = Arc<Mutex<Option<oneshot::Sender<(Option<String>, Vec<u8>)>>>>;

    #[derive(Clone)]
    struct MockCoordState {
        expected_agent_token: String,
        expected_takeover_token: String,
        registrations: Arc<AtomicU64>,
        pings_seen: Arc<AtomicU64>,
        unregistered: Arc<AtomicBool>,
        drop_first_conn: Arc<AtomicBool>,
        active_tunnel: Arc<Mutex<Option<mpsc::Sender<AxumWsMsg>>>>,
        http_reply: HttpReplySlot,
        ws_reply: WsReplySlot,
    }

    async fn mock_coord_tunnel_route(
        State(st): State<MockCoordState>,
        AxumPath(session_id): AxumPath<String>,
        headers: axum::http::HeaderMap,
        ws: WebSocketUpgrade,
    ) -> impl IntoResponse {
        let auth = headers
            .get("x-anyagents-agent")
            .and_then(|v| v.to_str().ok());
        if auth != Some(st.expected_agent_token.as_str()) {
            return StatusCode::UNAUTHORIZED.into_response();
        }
        let host = headers
            .get("host")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("127.0.0.1")
            .to_string();
        ws.on_upgrade(move |socket| run_mock_coord_socket(socket, st, session_id, host))
            .into_response()
    }

    async fn run_mock_coord_socket(
        socket: WebSocket,
        st: MockCoordState,
        session_id: String,
        host: String,
    ) {
        let (mut tx, mut rx) = socket.split();
        let first = rx.next().await.unwrap().unwrap();
        let AxumWsMsg::Text(txt) = first else {
            panic!("expected Register text frame");
        };
        let ControlFrame::Register { token, .. } = serde_json::from_str(&txt).unwrap() else {
            panic!("expected Register frame");
        };
        assert_eq!(token, st.expected_takeover_token);
        let reg_num = st.registrations.fetch_add(1, Ordering::SeqCst) + 1;
        let base = format!("http://{host}/takeover/{session_id}/");
        let ack = ControlFrame::Registered {
            public_base_url: base.clone(),
            public_url: format!("{base}?token={token}"),
            session_id: session_id.clone(),
        };
        tx.send(AxumWsMsg::Text(serde_json::to_string(&ack).unwrap().into()))
            .await
            .unwrap();

        let (out_tx, mut out_rx) = mpsc::channel::<AxumWsMsg>(64);
        *st.active_tunnel.lock().await = Some(out_tx);

        // Simulate forced proxy/coordinator drop on connection #1 when requested.
        if reg_num == 1 && st.drop_first_conn.swap(false, Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(60)).await;
            let _ = tx.close().await;
            return;
        }

        let mut http_status = 0u16;
        let mut http_buf = Vec::new();
        let mut ws_proto = None;

        loop {
            tokio::select! {
                out = out_rx.recv() => {
                    let Some(m) = out else { break; };
                    if tx.send(m).await.is_err() {
                        break;
                    }
                }
                msg = rx.next() => {
                    match msg {
                        Some(Ok(AxumWsMsg::Text(t))) => {
                            let Ok(frame) = serde_json::from_str::<ControlFrame>(&t) else {
                                continue;
                            };
                            match frame {
                                ControlFrame::Unregister => {
                                    st.unregistered.store(true, Ordering::SeqCst);
                                    break;
                                }
                                ControlFrame::HttpResponseHead { status, .. } => {
                                    http_status = status;
                                    http_buf.clear();
                                }
                                ControlFrame::HttpResponseEnd { .. } => {
                                    if let Some(ch) = st.http_reply.lock().await.take() {
                                        let _ = ch.send((http_status, String::from_utf8_lossy(&http_buf).into_owned()));
                                    }
                                }
                                ControlFrame::WsOpened { ok, protocol, .. } => {
                                    assert!(ok);
                                    ws_proto = protocol;
                                }
                                _ => {}
                            }
                        }
                        Some(Ok(AxumWsMsg::Binary(raw))) => {
                            if let Some((tag, _id, payload)) = decode_binary_frame(&raw) {
                                if tag == BIN_TAG_HTTP_CHUNK {
                                    http_buf.extend_from_slice(&payload);
                                } else if tag == BIN_TAG_WS_DATA
                                    && let Some(ch) = st.ws_reply.lock().await.take()
                                {
                                    let _ = ch.send((ws_proto.clone(), payload.to_vec()));
                                }
                            }
                        }
                        Some(Ok(AxumWsMsg::Ping(_))) => {
                            st.pings_seen.fetch_add(1, Ordering::SeqCst);
                        }
                        Some(Ok(AxumWsMsg::Pong(_))) => {}
                        _ => break,
                    }
                }
            }
        }
    }

    /// Start a local mock `agent-workstation` server verifying token auth, HTML, and binary RFB WS.
    async fn start_mock_local_takeover(expected_token: String) -> (SocketAddr, JoinHandle<()>) {
        let tok_http = expected_token.clone();
        let tok_ws = expected_token;
        let app = Router::new()
            .route(
                "/",
                get(
                    move |Query(q): Query<HashMap<String, String>>,
                          headers: axum::http::HeaderMap| {
                        let expected = tok_http.clone();
                        async move {
                            let ok = q.get("token").map(String::as_str) == Some(expected.as_str())
                                || headers.get("authorization").and_then(|v| v.to_str().ok())
                                    == Some(&format!("Bearer {expected}"));
                            if !ok {
                                return (StatusCode::UNAUTHORIZED, "missing token").into_response();
                            }
                            (StatusCode::OK, "<html>novnc-local</html>").into_response()
                        }
                    },
                ),
            )
            .route(
                "/ws",
                get(
                    move |Query(q): Query<HashMap<String, String>>, ws: WebSocketUpgrade| {
                        let expected = tok_ws.clone();
                        async move {
                            if q.get("token").map(String::as_str) != Some(expected.as_str()) {
                                return StatusCode::UNAUTHORIZED.into_response();
                            }
                            ws.protocols(["binary"]).on_upgrade(|mut sock| async move {
                                if let Some(Ok(AxumWsMsg::Binary(b))) = sock.recv().await {
                                    let mut out = b"rfb-ok:".to_vec();
                                    out.extend_from_slice(&b);
                                    let _ = sock.send(AxumWsMsg::Binary(Bytes::from(out))).await;
                                }
                            })
                        }
                    },
                ),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (addr, task)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn guest_tunnel_auto_reconnects_preserves_session_proxies_http_ws_and_stops_cleanly() {
        let takeover_token = "local_tok_abc123".to_string();
        let (local_addr, local_task) = start_mock_local_takeover(takeover_token.clone()).await;

        let coord_state = MockCoordState {
            expected_agent_token: "aars_test_agent_token".into(),
            expected_takeover_token: takeover_token.clone(),
            registrations: Arc::new(AtomicU64::new(0)),
            pings_seen: Arc::new(AtomicU64::new(0)),
            unregistered: Arc::new(AtomicBool::new(false)),
            drop_first_conn: Arc::new(AtomicBool::new(true)),
            active_tunnel: Arc::new(Mutex::new(None)),
            http_reply: Arc::new(Mutex::new(None)),
            ws_reply: Arc::new(Mutex::new(None)),
        };
        let coord_app = Router::new()
            .route(
                "/v1/takeover/{session_id}/tunnel",
                get(mock_coord_tunnel_route),
            )
            .with_state(coord_state.clone());
        let coord_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let coord_addr = coord_listener.local_addr().unwrap();
        let coord_task = tokio::spawn(async move {
            let _ = axum::serve(coord_listener, coord_app).await;
        });

        let mut cfg = TunnelConfig::new(
            format!("http://{coord_addr}"),
            CoordinatorAuth::Agent("aars_test_agent_token".into()),
            local_addr,
            takeover_token.clone(),
        );
        cfg.session_id = Some("tk_guest_reconnect_01".into());
        cfg.keepalive_interval = Duration::from_millis(60);
        cfg.initial_backoff = Duration::from_millis(50);
        cfg.max_backoff = Duration::from_millis(200);

        let handle = TakeoverTunnelHandle::start(cfg).await.unwrap();
        assert_eq!(handle.session_id(), "tk_guest_reconnect_01");
        assert_eq!(
            handle.public_url(),
            format!("http://{coord_addr}/takeover/tk_guest_reconnect_01/?token={takeover_token}")
        );

        // Wait for forced drop #1 + automatic transparent reconnect (#2) with SAME session_id + token!
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while coord_state.registrations.load(Ordering::SeqCst) < 2 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "guest failed to transparently auto-reconnect"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(handle.reconnect_count() >= 1);

        // Verify guest sends keepalive Pings on active reconnected tunnel.
        while coord_state.pings_seen.load(Ordering::SeqCst) == 0 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "guest failed to send keepalive Ping"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // Send proxied HTTP request through reconnected tunnel to local mock takeover server.
        let (http_tx, http_rx) = oneshot::channel();
        *coord_state.http_reply.lock().await = Some(http_tx);
        let tunnel_tx = coord_state.active_tunnel.lock().await.clone().unwrap();
        let http_req = ControlFrame::HttpRequest {
            id: 1,
            method: "GET".into(),
            path: "/".into(),
            query: None,
            headers: vec![],
            body_b64: String::new(),
        };
        tunnel_tx
            .send(AxumWsMsg::Text(
                serde_json::to_string(&http_req).unwrap().into(),
            ))
            .await
            .unwrap();
        let (status, body) = tokio::time::timeout(Duration::from_secs(3), http_rx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status, 200);
        assert_eq!(body, "<html>novnc-local</html>");

        // Send proxied WS open + binary RFB frame through reconnected tunnel.
        let (ws_tx, ws_rx) = oneshot::channel();
        *coord_state.ws_reply.lock().await = Some(ws_tx);
        let ws_open = ControlFrame::WsOpen {
            id: 7,
            path: "/ws".into(),
            query: None,
            protocols: vec!["binary".into()],
            headers: vec![],
        };
        tunnel_tx
            .send(AxumWsMsg::Text(
                serde_json::to_string(&ws_open).unwrap().into(),
            ))
            .await
            .unwrap();
        // Give local WS handshake a moment, then send binary RFB frame.
        tokio::time::sleep(Duration::from_millis(50)).await;
        tunnel_tx
            .send(AxumWsMsg::Binary(encode_binary_frame(
                BIN_TAG_WS_DATA,
                7,
                b"RFB-PING",
            )))
            .await
            .unwrap();
        let (proto, payload) = tokio::time::timeout(Duration::from_secs(3), ws_rx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(proto.as_deref(), Some("binary"));
        assert_eq!(payload, b"rfb-ok:RFB-PING");

        // Stopping handle sends Unregister cleanly.
        handle.stop().await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(coord_state.unregistered.load(Ordering::SeqCst));

        coord_task.abort();
        local_task.abort();
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn guest_tunnel_bridges_real_agent_workstation_takeover_html_state_take_release_and_rfb()
    {
        use agent_workstation::{ControlGate, Takeover, TakeoverConfig};
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        use tokio::net::UnixListener;

        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("vnc.sock");
        let vnc_listener = UnixListener::bind(&sock).unwrap();
        let gate = ControlGate::new();
        let takeover_token = "real_aw_tok_987654321".to_string();
        let takeover = Takeover::start(
            TakeoverConfig {
                token: Some(takeover_token.clone()),
                ..Default::default()
            },
            sock,
            gate.clone(),
        )
        .await
        .unwrap();
        let local_addr = takeover.local_addr().unwrap();

        let coord_state = MockCoordState {
            expected_agent_token: "aars_real_aw_agent".into(),
            expected_takeover_token: takeover_token.clone(),
            registrations: Arc::new(AtomicU64::new(0)),
            pings_seen: Arc::new(AtomicU64::new(0)),
            unregistered: Arc::new(AtomicBool::new(false)),
            drop_first_conn: Arc::new(AtomicBool::new(false)),
            active_tunnel: Arc::new(Mutex::new(None)),
            http_reply: Arc::new(Mutex::new(None)),
            ws_reply: Arc::new(Mutex::new(None)),
        };
        let coord_app = Router::new()
            .route(
                "/v1/takeover/{session_id}/tunnel",
                get(mock_coord_tunnel_route),
            )
            .with_state(coord_state.clone());
        let coord_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let coord_addr = coord_listener.local_addr().unwrap();
        let coord_task = tokio::spawn(async move {
            let _ = axum::serve(coord_listener, coord_app).await;
        });

        let mut cfg = TunnelConfig::new(
            format!("http://{coord_addr}"),
            CoordinatorAuth::Agent("aars_real_aw_agent".into()),
            local_addr,
            takeover_token.clone(),
        );
        cfg.session_id = Some("tk_real_aw_01".into());
        let handle = TakeoverTunnelHandle::start(cfg).await.unwrap();
        takeover
            .set_public_url(Url::parse(handle.public_base_url()).unwrap())
            .unwrap();
        assert_eq!(
            takeover.url().as_str(),
            format!("http://{coord_addr}/takeover/tk_real_aw_01/?token={takeover_token}")
        );

        let tunnel_tx = coord_state.active_tunnel.lock().await.clone().unwrap();

        // 1. Fetch real noVNC index.html over tunnel.
        let (http_tx, http_rx) = oneshot::channel();
        *coord_state.http_reply.lock().await = Some(http_tx);
        let req = ControlFrame::HttpRequest {
            id: 1,
            method: "GET".into(),
            path: "/".into(),
            query: None,
            headers: vec![],
            body_b64: String::new(),
        };
        tunnel_tx
            .send(AxumWsMsg::Text(serde_json::to_string(&req).unwrap().into()))
            .await
            .unwrap();
        let (st, html) = tokio::time::timeout(Duration::from_secs(3), http_rx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(st, 200);
        assert!(html.contains("<title>Desktop takeover</title>"), "{html}");

        // 2. POST /take hands control to human, and GET /state reflects holder=human.
        let (http_tx, http_rx) = oneshot::channel();
        *coord_state.http_reply.lock().await = Some(http_tx);
        let take_req = ControlFrame::HttpRequest {
            id: 2,
            method: "POST".into(),
            path: "/take".into(),
            query: None,
            headers: vec![],
            body_b64: String::new(),
        };
        tunnel_tx
            .send(AxumWsMsg::Text(
                serde_json::to_string(&take_req).unwrap().into(),
            ))
            .await
            .unwrap();
        let (st, take_json) = tokio::time::timeout(Duration::from_secs(3), http_rx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(st, 200);
        assert!(take_json.contains(r#""holder":"human""#), "{take_json}");

        // 3. Open real `/ws` RFB bridge over tunnel and confirm RFB 003.008 banner + input relay.
        let (ws_tx, ws_rx) = oneshot::channel();
        *coord_state.ws_reply.lock().await = Some(ws_tx);
        let ws_open = ControlFrame::WsOpen {
            id: 3,
            path: "/ws".into(),
            query: None,
            protocols: vec!["binary".into()],
            headers: vec![],
        };
        tunnel_tx
            .send(AxumWsMsg::Text(
                serde_json::to_string(&ws_open).unwrap().into(),
            ))
            .await
            .unwrap();
        let (mut vnc_stream, _) =
            tokio::time::timeout(Duration::from_secs(3), vnc_listener.accept())
                .await
                .unwrap()
                .unwrap();
        vnc_stream.write_all(b"RFB 003.008\n").await.unwrap();
        let (proto, banner) = tokio::time::timeout(Duration::from_secs(3), ws_rx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(proto.as_deref(), Some("binary"));
        assert_eq!(banner, b"RFB 003.008\n");

        // Send RFB client hello through tunnel and verify VNC Unix socket receives it.
        let hello = b"RFB 003.008\n\x01\x01";
        tunnel_tx
            .send(AxumWsMsg::Binary(encode_binary_frame(
                BIN_TAG_WS_DATA,
                3,
                hello,
            )))
            .await
            .unwrap();
        let mut received = vec![0u8; hello.len()];
        tokio::time::timeout(Duration::from_secs(3), vnc_stream.read_exact(&mut received))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(received, hello);

        // 4. POST /release returns control to agent cleanly.
        let (http_tx, http_rx) = oneshot::channel();
        *coord_state.http_reply.lock().await = Some(http_tx);
        let rel_req = ControlFrame::HttpRequest {
            id: 4,
            method: "POST".into(),
            path: "/release".into(),
            query: None,
            headers: vec![("content-type".into(), "application/json".into())],
            body_b64: base64::engine::general_purpose::STANDARD
                .encode(br#"{"note":"captcha solved"}"#),
        };
        tunnel_tx
            .send(AxumWsMsg::Text(
                serde_json::to_string(&rel_req).unwrap().into(),
            ))
            .await
            .unwrap();
        let (st, rel_json) = tokio::time::timeout(Duration::from_secs(3), http_rx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(st, 200);
        assert!(rel_json.contains(r#""holder":"agent""#), "{rel_json}");

        handle.stop().await;
        takeover.shutdown().await;
        coord_task.abort();
    }
}
