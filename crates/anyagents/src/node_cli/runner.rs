//! Outbound TLS 1.3 WebSocket reverse-tunnel runner, local kill-switch control
//! socket, and process-group execution for `anyagents node run`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyagents_core::Error;
use base64::Engine as _;
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{TcpStream, UnixListener, UnixStream};
use tokio::sync::{Mutex, mpsc, watch};
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;
use tokio_tungstenite::tungstenite::Message as WsMsg;
use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;
use url::Url;

use crate::node_cli::egress::EgressManager;
use crate::node_cli::files::FilesJail;
use crate::node_cli::ssrf_guard::EgressPolicy;
use crate::node_cli::{StoredNodeConfig, control_socket_path, load_node_config_at};
use crate::node_protocol::{
    KEEPALIVE_INTERVAL, MAX_EXEC_OUTPUT_BYTES, MAX_TUNNEL_FRAME_BYTES, NodeClientFrame,
    NodeServerFrame, OutputStream,
};

const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
const OUTPUT_CHUNK_BYTES: usize = 4096;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct LocalControlRequest {
    pub(crate) action: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct LocalControlResponse {
    pub(crate) ok: bool,
    pub(crate) node_id: String,
    pub(crate) connected: bool,
    pub(crate) paused: bool,
    pub(crate) revoked: bool,
    pub(crate) active_execs: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) error: Option<String>,
}

struct ActiveChild {
    pid: Option<u32>,
    cancel_tx: watch::Sender<Option<String>>,
}

pub(crate) struct RunnerState {
    pub(crate) node_id: String,
    pub(crate) connected: AtomicBool,
    pub(crate) paused: AtomicBool,
    pub(crate) revoked: AtomicBool,
    pub(crate) stop: AtomicBool,
    pub(crate) shutdown_tx: watch::Sender<bool>,
    active_children: Mutex<HashMap<String, ActiveChild>>,
    system_info: SystemInfo,
    pub(crate) egress: Option<EgressManager>,
    pub(crate) files_jail: FilesJail,
}

#[derive(Clone)]
struct SystemInfo {
    hostname: String,
    os: String,
    arch: String,
    capabilities: Vec<String>,
}

impl RunnerState {
    fn new(
        node_id: String,
        capabilities: Vec<String>,
        egress_policy: Option<EgressPolicy>,
        files_jail: FilesJail,
    ) -> (Arc<Self>, watch::Receiver<bool>) {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let egress = egress_policy.map(|p| EgressManager::new(&p));
        let state = Arc::new(Self {
            node_id,
            connected: AtomicBool::new(false),
            paused: AtomicBool::new(false),
            revoked: AtomicBool::new(false),
            stop: AtomicBool::new(false),
            shutdown_tx,
            active_children: Mutex::new(HashMap::new()),
            system_info: SystemInfo {
                hostname: crate::node_cli::detect_hostname(),
                os: std::env::consts::OS.to_string(),
                arch: std::env::consts::ARCH.to_string(),
                capabilities,
            },
            egress,
            files_jail,
        });
        (state, shutdown_rx)
    }

    pub(crate) async fn active_count(&self) -> usize {
        self.active_children.lock().await.len()
    }

    /// Instant kill-switch: marks node paused and sends SIGKILL to every active
    /// spawned child process group so no background/grandchild process survives.
    /// Also tears down all active egress streams.
    pub(crate) async fn trigger_pause_kill_switch(&self, reason: &str) {
        self.paused.store(true, Ordering::SeqCst);
        self.kill_all_active(reason).await;
        if let Some(ref egress) = self.egress {
            egress.close_all().await;
        }
    }

    pub(crate) async fn trigger_revoke_kill_switch(&self, reason: &str) {
        self.paused.store(true, Ordering::SeqCst);
        self.revoked.store(true, Ordering::SeqCst);
        self.stop.store(true, Ordering::SeqCst);
        self.kill_all_active(reason).await;
        if let Some(ref egress) = self.egress {
            egress.close_all().await;
        }
        let _ = self.shutdown_tx.send(true);
    }

    pub(crate) async fn kill_all_active(&self, reason: &str) {
        let active = self.active_children.lock().await;
        for entry in active.values() {
            if let Some(pid) = entry.pid {
                kill_process_group(pid);
            }
            let _ = entry.cancel_tx.send(Some(reason.to_string()));
        }
    }

    async fn cancel_one(&self, exec_id: &str, reason: &str) {
        let active = self.active_children.lock().await;
        if let Some(entry) = active.get(exec_id) {
            if let Some(pid) = entry.pid {
                kill_process_group(pid);
            }
            let _ = entry.cancel_tx.send(Some(reason.to_string()));
        }
    }
}

#[cfg(unix)]
pub(crate) fn kill_process_group(pid: u32) {
    if let Ok(raw_pid) = libc::pid_t::try_from(pid)
        && raw_pid > 0
    {
        // Kill the entire process group (-pgid == -pid because process_group(0) was set).
        unsafe {
            let _ = libc::kill(-raw_pid, libc::SIGKILL);
            let _ = libc::kill(raw_pid, libc::SIGKILL);
        }
    }
}

#[cfg(not(unix))]
pub(crate) fn kill_process_group(_pid: u32) {}

/// Send a synchronous command to a locally running `anyagents node run` daemon,
/// returning `None` if no local control socket is listening.
pub(crate) fn query_local_control(
    config_path: &Path,
    action: &str,
) -> Option<LocalControlResponse> {
    let sock = control_socket_path(config_path);
    if !sock.exists() {
        return None;
    }
    let mut stream = std::os::unix::net::UnixStream::connect(&sock).ok()?;
    let _ = stream.set_read_timeout(Some(Duration::from_secs(3)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(3)));
    let payload = serde_json::to_string(&LocalControlRequest {
        action: action.to_string(),
    })
    .ok()?;
    use std::io::{Read as _, Write as _};
    stream.write_all(payload.as_bytes()).ok()?;
    stream.write_all(b"\n").ok()?;
    stream.flush().ok()?;
    let mut raw = String::new();
    stream.read_to_string(&mut raw).ok()?;
    serde_json::from_str(raw.trim()).ok()
}

pub(crate) fn run_daemon(config_path: &Path) -> Result<(), Error> {
    let cfg = load_node_config_at(config_path)?.ok_or_else(|| {
        Error::Message(format!(
            "no node pairing config at {}; run `anyagents node pair --url <coordinator_url>` first",
            config_path.display()
        ))
    })?;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|err| Error::Message(format!("failed to build node runtime: {err}")))?;
    rt.block_on(run_daemon_async(config_path.to_path_buf(), cfg))
}

pub(crate) async fn run_daemon_async(
    config_path: PathBuf,
    cfg: StoredNodeConfig,
) -> Result<(), Error> {
    let caps = if cfg.capabilities.is_empty() {
        vec!["exec".to_string()]
    } else {
        cfg.capabilities.clone()
    };
    let egress_policy = if caps.iter().any(|c| c == "exit") {
        Some(EgressPolicy {
            allow_private_cidrs: cfg.allow_private_cidrs.clone(),
            allow_domains: cfg.allow_domains.clone(),
            deny_domains: cfg.deny_domains.clone(),
        })
    } else {
        None
    };
    let files_jail = if caps.iter().any(|c| c == "files") {
        FilesJail::new(
            cfg.allow_dirs
                .iter()
                .map(|d| std::path::PathBuf::from(d.trim()))
                .filter(|p| !p.as_os_str().is_empty())
                .collect(),
        )
    } else {
        FilesJail::default()
    };
    let (state, shutdown_rx) =
        RunnerState::new(cfg.node_id.clone(), caps, egress_policy, files_jail);
    let sock_path = control_socket_path(&config_path);
    let _sock_guard = bind_control_socket(&sock_path, Arc::clone(&state)).await?;

    let state_sig = Arc::clone(&state);
    let signal_task = tokio::spawn(async move {
        wait_for_shutdown_signal().await;
        state_sig.stop.store(true, Ordering::SeqCst);
        state_sig
            .kill_all_active("node daemon received termination signal")
            .await;
        let _ = state_sig.shutdown_tx.send(true);
    });

    let res = run_tunnel_loop(&cfg, &state, shutdown_rx).await;
    signal_task.abort();
    state.kill_all_active("node daemon shutting down").await;
    res
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() {
    let mut sigterm =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).ok();
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = async {
            if let Some(ref mut s) = sigterm {
                s.recv().await;
            } else {
                std::future::pending::<()>().await;
            }
        } => {}
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

struct ControlSocketGuard {
    path: PathBuf,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for ControlSocketGuard {
    fn drop(&mut self) {
        self.task.abort();
        let _ = std::fs::remove_file(&self.path);
    }
}

async fn bind_control_socket(
    path: &Path,
    state: Arc<RunnerState>,
) -> Result<ControlSocketGuard, Error> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    if path.exists() {
        let _ = std::fs::remove_file(path);
    }
    let listener = UnixListener::bind(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    let task = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let st = Arc::clone(&state);
            tokio::spawn(async move {
                handle_local_control_conn(stream, st).await;
            });
        }
    });
    Ok(ControlSocketGuard {
        path: path.to_path_buf(),
        task,
    })
}

async fn handle_local_control_conn(stream: UnixStream, state: Arc<RunnerState>) {
    let (reader, mut writer) = stream.into_split();
    let mut line = String::new();
    let mut buf = BufReader::new(reader);
    if buf.read_line(&mut line).await.is_err() {
        return;
    }
    let req: LocalControlRequest = match serde_json::from_str(line.trim()) {
        Ok(r) => r,
        Err(err) => {
            let resp = LocalControlResponse {
                ok: false,
                node_id: state.node_id.clone(),
                connected: state.connected.load(Ordering::SeqCst),
                paused: state.paused.load(Ordering::SeqCst),
                revoked: state.revoked.load(Ordering::SeqCst),
                active_execs: state.active_count().await,
                error: Some(err.to_string()),
            };
            let _ = writer
                .write_all(serde_json::to_string(&resp).unwrap_or_default().as_bytes())
                .await;
            return;
        }
    };
    match req.action.as_str() {
        "status" => {}
        "pause" => {
            state
                .trigger_pause_kill_switch("paused via local kill-switch")
                .await;
        }
        "resume" => {
            if !state.revoked.load(Ordering::SeqCst) {
                state.paused.store(false, Ordering::SeqCst);
            }
        }
        "revoke" => {
            state
                .trigger_revoke_kill_switch("revoked via local control")
                .await;
        }
        other => {
            let resp = LocalControlResponse {
                ok: false,
                node_id: state.node_id.clone(),
                connected: state.connected.load(Ordering::SeqCst),
                paused: state.paused.load(Ordering::SeqCst),
                revoked: state.revoked.load(Ordering::SeqCst),
                active_execs: state.active_count().await,
                error: Some(format!("unknown control action `{other}`")),
            };
            let _ = writer
                .write_all(serde_json::to_string(&resp).unwrap_or_default().as_bytes())
                .await;
            return;
        }
    }
    let resp = LocalControlResponse {
        ok: true,
        node_id: state.node_id.clone(),
        connected: state.connected.load(Ordering::SeqCst),
        paused: state.paused.load(Ordering::SeqCst),
        revoked: state.revoked.load(Ordering::SeqCst),
        active_execs: state.active_count().await,
        error: None,
    };
    let _ = writer
        .write_all(serde_json::to_string(&resp).unwrap_or_default().as_bytes())
        .await;
    let _ = writer.shutdown().await;
}

pub(crate) fn build_ws_connect_url(coordinator_url: &str) -> Result<Url, Error> {
    let mut parsed = Url::parse(coordinator_url.trim().trim_end_matches('/'))
        .map_err(|err| Error::Message(format!("invalid coordinator URL: {err}")))?;
    match parsed.scheme() {
        "https" => {
            parsed
                .set_scheme("wss")
                .map_err(|_| Error::Message("failed to convert https to wss".into()))?;
        }
        "http" => {
            parsed
                .set_scheme("ws")
                .map_err(|_| Error::Message("failed to convert http to ws".into()))?;
        }
        "wss" | "ws" => {}
        other => {
            return Err(Error::Message(format!(
                "unsupported coordinator URL scheme `{other}`"
            )));
        }
    }
    parsed.set_path("/v1/nodes/connect");
    parsed.set_query(None);
    parsed.set_fragment(None);
    Ok(parsed)
}

enum TunnelStream {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl AsyncRead for TunnelStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            Self::Tls(s) => std::pin::Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for TunnelStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(s) => std::pin::Pin::new(s).poll_write(cx, buf),
            Self::Tls(s) => std::pin::Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => std::pin::Pin::new(s).poll_flush(cx),
            Self::Tls(s) => std::pin::Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            Self::Tls(s) => std::pin::Pin::new(s).poll_shutdown(cx),
        }
    }
}

async fn connect_ws(
    ws_url: &Url,
    node_token: &str,
) -> Result<tokio_tungstenite::WebSocketStream<TunnelStream>, ConnectFailure> {
    let host = ws_url
        .host_str()
        .ok_or_else(|| ConnectFailure::Fatal("missing coordinator host".into()))?;
    let port = ws_url
        .port_or_known_default()
        .ok_or_else(|| ConnectFailure::Fatal("missing coordinator port".into()))?;
    let tcp = TcpStream::connect((host, port))
        .await
        .map_err(|err| ConnectFailure::Transient(format!("TCP connect failed: {err}")))?;
    let stream = if ws_url.scheme() == "wss" {
        let mut roots = rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let tls_cfg = rustls::ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(|err| ConnectFailure::Fatal(format!("TLS 1.3 config failed: {err}")))?
            .with_root_certificates(roots)
            .with_no_client_auth();
        let connector = TlsConnector::from(Arc::new(tls_cfg));
        let server_name = rustls_pki_types::ServerName::try_from(host.to_string())
            .map_err(|err| ConnectFailure::Fatal(format!("invalid TLS server name: {err}")))?;
        let tls = connector
            .connect(server_name, tcp)
            .await
            .map_err(|err| ConnectFailure::Transient(format!("TLS 1.3 handshake failed: {err}")))?;
        TunnelStream::Tls(Box::new(tls))
    } else {
        TunnelStream::Plain(tcp)
    };

    let mut req = ws_url
        .as_str()
        .into_client_request()
        .map_err(|err| ConnectFailure::Fatal(format!("invalid WS request: {err}")))?;
    req.headers_mut().insert(
        "authorization",
        format!("Bearer {node_token}")
            .parse()
            .map_err(|_| ConnectFailure::Fatal("invalid node token header".into()))?,
    );
    let mut ws_cfg = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default();
    ws_cfg.max_message_size = Some(MAX_TUNNEL_FRAME_BYTES);
    ws_cfg.max_frame_size = Some(MAX_TUNNEL_FRAME_BYTES);

    match tokio_tungstenite::client_async_with_config(req, stream, Some(ws_cfg)).await {
        Ok((ws, _resp)) => Ok(ws),
        Err(tokio_tungstenite::tungstenite::Error::Http(resp))
            if matches!(resp.status().as_u16(), 401 | 403) =>
        {
            Err(ConnectFailure::Fatal(format!(
                "node authentication rejected (HTTP {}) — token may be revoked",
                resp.status().as_u16()
            )))
        }
        Err(err) => Err(ConnectFailure::Transient(format!(
            "WebSocket handshake failed: {err}"
        ))),
    }
}

enum ConnectFailure {
    Transient(String),
    Fatal(String),
}

async fn run_tunnel_loop(
    cfg: &StoredNodeConfig,
    state: &Arc<RunnerState>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<(), Error> {
    let ws_url = build_ws_connect_url(&cfg.coordinator_url)?;
    let mut backoff = INITIAL_BACKOFF;

    loop {
        if *shutdown_rx.borrow() || state.stop.load(Ordering::SeqCst) {
            return Ok(());
        }
        match connect_ws(&ws_url, &cfg.node_token).await {
            Ok(ws) => {
                backoff = INITIAL_BACKOFF;
                state.connected.store(true, Ordering::SeqCst);
                let session_res = run_ws_session(ws, state, &mut shutdown_rx).await;
                state.connected.store(false, Ordering::SeqCst);
                state.kill_all_active("node tunnel disconnected").await;
                if let Err(err) = session_res {
                    eprintln!("anyagents-node: session error: {err}");
                }
                if state.revoked.load(Ordering::SeqCst) {
                    return Err(Error::Message("node was revoked by coordinator".into()));
                }
                if *shutdown_rx.borrow() || state.stop.load(Ordering::SeqCst) {
                    return Ok(());
                }
            }
            Err(ConnectFailure::Fatal(msg)) => {
                return Err(Error::Message(msg));
            }
            Err(ConnectFailure::Transient(msg)) => {
                eprintln!(
                    "anyagents-node: connection failed ({msg}); reconnecting in {}s...",
                    backoff.as_secs()
                );
            }
        }

        tokio::select! {
            _ = tokio::time::sleep(backoff) => {}
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    return Ok(());
                }
            }
        }
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

async fn run_ws_session(
    ws: tokio_tungstenite::WebSocketStream<TunnelStream>,
    state: &Arc<RunnerState>,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> Result<(), String> {
    let (mut ws_tx, mut ws_rx) = ws.split();
    let (out_tx, mut out_rx) = mpsc::channel::<NodeClientFrame>(256);

    let hello = NodeClientFrame::Hello {
        hostname: state.system_info.hostname.clone(),
        os: state.system_info.os.clone(),
        arch: state.system_info.arch.clone(),
        capabilities: state.system_info.capabilities.clone(),
    };
    send_client_frame(&mut ws_tx, &hello).await?;

    let mut heartbeat_tick = tokio::time::interval(KEEPALIVE_INTERVAL);
    heartbeat_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    heartbeat_tick.tick().await;

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    let _ = ws_tx.send(WsMsg::Close(None)).await;
                    break;
                }
            }
            _ = heartbeat_tick.tick() => {
                let active_execs = state.active_count().await;
                let active_streams = if let Some(ref egress) = state.egress {
                    egress.active_count().await
                } else {
                    0
                };
                send_client_frame(&mut ws_tx, &NodeClientFrame::Heartbeat { active_execs, active_streams }).await?;
                ws_tx.send(WsMsg::Ping(Vec::new().into())).await.map_err(|e| e.to_string())?;
            }
            Some(frame) = out_rx.recv() => {
                send_client_frame(&mut ws_tx, &frame).await?;
            }
            incoming = ws_rx.next() => {
                let Some(msg) = incoming else { break; };
                match msg {
                    Ok(WsMsg::Text(txt)) => {
                        let Ok(server_frame) = serde_json::from_str::<NodeServerFrame>(&txt) else {
                            continue;
                        };
                        match server_frame {
                            NodeServerFrame::Welcome { status, .. } => {
                                state.paused.store(status == "paused", Ordering::SeqCst);
                            }
                            NodeServerFrame::Control { action, reason } => {
                                match action.as_str() {
                                    "pause" => {
                                        let why = reason.unwrap_or_else(|| "paused by coordinator".into());
                                        state.trigger_pause_kill_switch(&why).await;
                                    }
                                    "resume" => {
                                        if !state.revoked.load(Ordering::SeqCst) {
                                            state.paused.store(false, Ordering::SeqCst);
                                        }
                                    }
                                    "revoke" => {
                                        let why = reason.unwrap_or_else(|| "revoked by coordinator".into());
                                        state.trigger_revoke_kill_switch(&why).await;
                                        let _ = ws_tx.send(WsMsg::Close(None)).await;
                                        break;
                                    }
                                    _ => {}
                                }
                            }
                            NodeServerFrame::ExecStart { exec_id, command, args, cwd, timeout_secs } => {
                                if state.paused.load(Ordering::SeqCst) || state.revoked.load(Ordering::SeqCst) {
                                    let _ = out_tx.send(NodeClientFrame::ExecDone {
                                        exec_id,
                                        exit_code: None,
                                        timed_out: false,
                                        cancelled: true,
                                        error: Some("node is paused".into()),
                                        duration_ms: 0,
                                    }).await;
                                    continue;
                                }
                                let st = Arc::clone(state);
                                let tx = out_tx.clone();
                                tokio::spawn(async move {
                                    execute_child_task(st, tx, exec_id, command, args, cwd, timeout_secs).await;
                                });
                            }
                            NodeServerFrame::ExecCancel { exec_id, reason } => {
                                state.cancel_one(&exec_id, &reason).await;
                            }
                            // ── Multiplexed TCP egress streams (exit capability) ──
                            NodeServerFrame::StreamOpen { stream_id, host, port } => {
                                if state.paused.load(Ordering::SeqCst) || state.revoked.load(Ordering::SeqCst) {
                                    let _ = out_tx.send(NodeClientFrame::StreamError {
                                        stream_id, error: "node is paused".into(),
                                    }).await;
                                    continue;
                                }
                                if let Some(ref egress) = state.egress {
                                    egress.open_stream(stream_id, host, port, out_tx.clone()).await;
                                } else {
                                    let _ = out_tx.send(NodeClientFrame::StreamError {
                                        stream_id, error: "exit capability not enabled on this node".into(),
                                    }).await;
                                }
                            }
                            NodeServerFrame::StreamData { stream_id, data_b64 } => {
                                if let Some(ref egress) = state.egress {
                                    egress.write_to_stream(&stream_id, &data_b64).await;
                                }
                            }
                            NodeServerFrame::StreamClose { stream_id } => {
                                if let Some(ref egress) = state.egress {
                                    egress.close_stream(&stream_id).await;
                                }
                            }
                            // ── Scoped files capability ──
                            NodeServerFrame::FilesList { req_id, path } => {
                                if !state.files_jail.is_enabled() {
                                    let _ = out_tx.send(NodeClientFrame::FilesResult {
                                        req_id, entries: None, data_b64: None,
                                        error: Some("files capability not enabled on this node".into()),
                                    }).await;
                                    continue;
                                }
                                if state.paused.load(Ordering::SeqCst) || state.revoked.load(Ordering::SeqCst) {
                                    let _ = out_tx.send(NodeClientFrame::FilesResult {
                                        req_id, entries: None, data_b64: None,
                                        error: Some("node is paused".into()),
                                    }).await;
                                    continue;
                                }
                                let jail = state.files_jail.clone();
                                let tx = out_tx.clone();
                                tokio::task::spawn_blocking(move || {
                                    let result = jail.list(&path);
                                    let frame = match result {
                                        Ok(entries) => NodeClientFrame::FilesResult {
                                            req_id, entries: Some(entries), data_b64: None, error: None,
                                        },
                                        Err(err) => NodeClientFrame::FilesResult {
                                            req_id, entries: None, data_b64: None, error: Some(err),
                                        },
                                    };
                                    let _ = tx.blocking_send(frame);
                                });
                            }
                            NodeServerFrame::FilesRead { req_id, path } => {
                                if !state.files_jail.is_enabled() {
                                    let _ = out_tx.send(NodeClientFrame::FilesResult {
                                        req_id, entries: None, data_b64: None,
                                        error: Some("files capability not enabled on this node".into()),
                                    }).await;
                                    continue;
                                }
                                if state.paused.load(Ordering::SeqCst) || state.revoked.load(Ordering::SeqCst) {
                                    let _ = out_tx.send(NodeClientFrame::FilesResult {
                                        req_id, entries: None, data_b64: None,
                                        error: Some("node is paused".into()),
                                    }).await;
                                    continue;
                                }
                                let jail = state.files_jail.clone();
                                let tx = out_tx.clone();
                                tokio::task::spawn_blocking(move || {
                                    let result = jail.read(&path);
                                    let frame = match result {
                                        Ok(data_b64) => NodeClientFrame::FilesResult {
                                            req_id, entries: None, data_b64: Some(data_b64), error: None,
                                        },
                                        Err(err) => NodeClientFrame::FilesResult {
                                            req_id, entries: None, data_b64: None, error: Some(err),
                                        },
                                    };
                                    let _ = tx.blocking_send(frame);
                                });
                            }
                            NodeServerFrame::FilesWrite { req_id, path, data_b64 } => {
                                if !state.files_jail.is_enabled() {
                                    let _ = out_tx.send(NodeClientFrame::FilesResult {
                                        req_id, entries: None, data_b64: None,
                                        error: Some("files capability not enabled on this node".into()),
                                    }).await;
                                    continue;
                                }
                                if state.paused.load(Ordering::SeqCst) || state.revoked.load(Ordering::SeqCst) {
                                    let _ = out_tx.send(NodeClientFrame::FilesResult {
                                        req_id, entries: None, data_b64: None,
                                        error: Some("node is paused".into()),
                                    }).await;
                                    continue;
                                }
                                let jail = state.files_jail.clone();
                                let tx = out_tx.clone();
                                tokio::task::spawn_blocking(move || {
                                    let result = jail.write(&path, &data_b64);
                                    let frame = match result {
                                        Ok(()) => NodeClientFrame::FilesResult {
                                            req_id, entries: None, data_b64: None, error: None,
                                        },
                                        Err(err) => NodeClientFrame::FilesResult {
                                            req_id, entries: None, data_b64: None, error: Some(err),
                                        },
                                    };
                                    let _ = tx.blocking_send(frame);
                                });
                            }
                        }
                    }
                    Ok(WsMsg::Ping(payload)) => {
                        ws_tx.send(WsMsg::Pong(payload)).await.map_err(|e| e.to_string())?;
                    }
                    Ok(WsMsg::Pong(_)) => {}
                    Ok(WsMsg::Close(_)) => break,
                    Ok(WsMsg::Binary(_)) | Ok(WsMsg::Frame(_)) => {}
                    Err(err) => return Err(err.to_string()),
                }
            }
        }
    }
    Ok(())
}

async fn send_client_frame(
    ws_tx: &mut futures::stream::SplitSink<tokio_tungstenite::WebSocketStream<TunnelStream>, WsMsg>,
    frame: &NodeClientFrame,
) -> Result<(), String> {
    let json = serde_json::to_string(frame).map_err(|e| e.to_string())?;
    ws_tx
        .send(WsMsg::Text(json.into()))
        .await
        .map_err(|e| e.to_string())
}

async fn execute_child_task(
    state: Arc<RunnerState>,
    out_tx: mpsc::Sender<NodeClientFrame>,
    exec_id: String,
    command: String,
    args: Vec<String>,
    cwd: Option<String>,
    timeout_secs: u64,
) {
    let started = Instant::now();
    let (cancel_tx, mut cancel_rx) = watch::channel::<Option<String>>(None);

    let mut cmd = tokio::process::Command::new(&command);
    cmd.args(&args)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    if let Some(ref dir) = cwd
        && !dir.trim().is_empty()
    {
        cmd.current_dir(dir.trim());
    }
    #[cfg(unix)]
    {
        cmd.process_group(0);
    }

    let mut child = match cmd.spawn() {
        Ok(child) => child,
        Err(err) => {
            let _ = out_tx
                .send(NodeClientFrame::ExecDone {
                    exec_id,
                    exit_code: None,
                    timed_out: false,
                    cancelled: false,
                    error: Some(format!("failed to spawn `{command}`: {err}")),
                    duration_ms: started.elapsed().as_millis() as u64,
                })
                .await;
            return;
        }
    };

    let pid = child.id();
    state
        .active_children
        .lock()
        .await
        .insert(exec_id.clone(), ActiveChild { pid, cancel_tx });

    if state.paused.load(Ordering::SeqCst) || state.revoked.load(Ordering::SeqCst) {
        if let Some(pid) = pid {
            kill_process_group(pid);
        }
        let _ = child.wait().await;
        state.active_children.lock().await.remove(&exec_id);
        let _ = out_tx
            .send(NodeClientFrame::ExecDone {
                exec_id,
                exit_code: None,
                timed_out: false,
                cancelled: true,
                error: Some("node paused during spawn".into()),
                duration_ms: started.elapsed().as_millis() as u64,
            })
            .await;
        return;
    }

    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let stdout_task = stdout.map(|pipe| {
        tokio::spawn(pump_output(
            pipe,
            exec_id.clone(),
            OutputStream::Stdout,
            out_tx.clone(),
        ))
    });
    let stderr_task = stderr.map(|pipe| {
        tokio::spawn(pump_output(
            pipe,
            exec_id.clone(),
            OutputStream::Stderr,
            out_tx.clone(),
        ))
    });

    let mut timed_out = false;
    let mut cancelled = false;
    let mut cancel_reason: Option<String> = None;
    let mut exit_code: Option<i32> = None;
    let deadline = tokio::time::sleep(Duration::from_secs(timeout_secs.max(1)));
    tokio::pin!(deadline);

    tokio::select! {
        status_res = child.wait() => {
            if let Ok(status) = status_res {
                exit_code = status.code();
            }
        }
        _ = &mut deadline => {
            timed_out = true;
            if let Some(pid) = pid {
                kill_process_group(pid);
            }
            let _ = child.wait().await;
        }
        _ = cancel_rx.changed() => {
            let reason_opt = { cancel_rx.borrow().clone() };
            if let Some(reason) = reason_opt {
                cancelled = true;
                cancel_reason = Some(reason);
                if let Some(pid) = pid {
                    kill_process_group(pid);
                }
                let _ = child.wait().await;
            }
        }
    }

    if let Some(task) = stdout_task {
        let _ = task.await;
    }
    if let Some(task) = stderr_task {
        let _ = task.await;
    }
    state.active_children.lock().await.remove(&exec_id);

    let _ = out_tx
        .send(NodeClientFrame::ExecDone {
            exec_id,
            exit_code,
            timed_out,
            cancelled,
            error: cancel_reason.or_else(|| timed_out.then(|| "execution timed out".into())),
            duration_ms: started.elapsed().as_millis() as u64,
        })
        .await;
}

async fn pump_output<R: AsyncRead + Unpin>(
    mut pipe: R,
    exec_id: String,
    stream: OutputStream,
    out_tx: mpsc::Sender<NodeClientFrame>,
) {
    let mut sent = 0usize;
    let mut buf = vec![0u8; OUTPUT_CHUNK_BYTES];
    loop {
        match pipe.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                if sent >= MAX_EXEC_OUTPUT_BYTES {
                    continue;
                }
                let take = n.min(MAX_EXEC_OUTPUT_BYTES - sent);
                sent += take;
                let data_b64 = base64::engine::general_purpose::STANDARD.encode(&buf[..take]);
                if out_tx
                    .send(NodeClientFrame::ExecOutput {
                        exec_id: exec_id.clone(),
                        stream,
                        data_b64,
                    })
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }
    }
}
