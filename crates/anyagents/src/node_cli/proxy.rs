//! Local loopback proxy listener that speaks HTTP CONNECT and SOCKS5 (NO AUTH
//! on loopback only) and tunnels each client TCP stream through the Coordinator
//! egress WebSocket to an `exit`-capable node.

use std::collections::HashMap;
use std::sync::Arc;

use base64::Engine as _;
use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, mpsc};
use tokio_tungstenite::tungstenite::Message;

const STREAM_READ_BUF: usize = 16 * 1024;

struct ProxyRelay {
    ws_tx: mpsc::Sender<Message>,
    streams: Arc<Mutex<HashMap<String, mpsc::Sender<StreamEvent>>>>,
}

enum StreamEvent {
    Opened,
    Data(Vec<u8>),
    Error(String),
    Closed,
}

pub(crate) async fn run_proxy(
    coordinator_url: &str,
    node_token: &str,
    node_id: &str,
    listen_addr: &str,
) -> Result<(), crate::Error> {
    let ws_url = build_egress_ws_url(coordinator_url, node_id)?;
    let mut request =
        tokio_tungstenite::tungstenite::client::IntoClientRequest::into_client_request(
            ws_url.as_str(),
        )
        .map_err(|e| crate::Error::Message(format!("invalid WebSocket URL: {e}")))?;
    request.headers_mut().insert(
        "Authorization",
        format!("Bearer {node_token}")
            .parse()
            .map_err(|_| crate::Error::Message("invalid token header".into()))?,
    );

    let (ws, _resp) = tokio_tungstenite::connect_async(request)
        .await
        .map_err(|e| crate::Error::Message(format!("WebSocket connect failed: {e}")))?;

    let (ws_write, ws_read) = ws.split();

    let (ws_tx, mut ws_rx) = mpsc::channel::<Message>(256);
    let streams: Arc<Mutex<HashMap<String, mpsc::Sender<StreamEvent>>>> =
        Arc::new(Mutex::new(HashMap::new()));

    // WS write pump.
    let mut ws_write_handle = tokio::spawn(async move {
        let mut ws_write = ws_write;
        while let Some(msg) = ws_rx.recv().await {
            if ws_write.send(msg).await.is_err() {
                break;
            }
        }
    });

    // WS read pump: dispatch incoming frames to per-stream channels.
    let streams_read = Arc::clone(&streams);
    let mut ws_read_handle = tokio::spawn(async move {
        let mut ws_read = ws_read;
        while let Some(Ok(msg)) = ws_read.next().await {
            if let Message::Text(text) = msg
                && let Ok(frame) = serde_json::from_str::<Value>(&text)
            {
                let ftype = frame.get("type").and_then(Value::as_str).unwrap_or("");
                let sid = frame.get("stream_id").and_then(Value::as_str).unwrap_or("");
                if sid.is_empty() {
                    continue;
                }
                let lock = streams_read.lock().await;
                if let Some(tx) = lock.get(sid) {
                    match ftype {
                        "opened" => {
                            let _ = tx.send(StreamEvent::Opened).await;
                        }
                        "data" => {
                            if let Some(b64) = frame.get("data_b64").and_then(Value::as_str)
                                && let Ok(data) =
                                    base64::engine::general_purpose::STANDARD.decode(b64)
                            {
                                let _ = tx.send(StreamEvent::Data(data)).await;
                            }
                        }
                        "error" => {
                            let err = frame
                                .get("error")
                                .and_then(Value::as_str)
                                .unwrap_or("unknown error")
                                .to_string();
                            let _ = tx.send(StreamEvent::Error(err)).await;
                        }
                        "closed" => {
                            let _ = tx.send(StreamEvent::Closed).await;
                        }
                        _ => {}
                    }
                }
            }
        }
    });

    let relay = Arc::new(ProxyRelay { ws_tx, streams });

    let listener = TcpListener::bind(listen_addr)
        .await
        .map_err(|e| crate::Error::Message(format!("bind {listen_addr}: {e}")))?;

    eprintln!("proxy listening on {listen_addr} (HTTP CONNECT + SOCKS5)");

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                match accepted {
                    Ok((tcp, peer)) => {
                        // Only allow loopback.
                        if !peer.ip().is_loopback() {
                            drop(tcp);
                            continue;
                        }
                        let relay = Arc::clone(&relay);
                        tokio::spawn(async move {
                            if let Err(e) = handle_client(tcp, &relay).await {
                                eprintln!("proxy client error: {e}");
                            }
                        });
                    }
                    Err(e) => {
                        eprintln!("accept error: {e}");
                    }
                }
            }
            _ = &mut ws_read_handle => break,
            _ = &mut ws_write_handle => break,
        }
    }

    Ok(())
}

fn build_egress_ws_url(coordinator_url: &str, node_id: &str) -> Result<String, crate::Error> {
    let base = coordinator_url.trim_end_matches('/');
    let scheme = if base.starts_with("https://") {
        "wss"
    } else if base.starts_with("http://") {
        "ws"
    } else {
        return Err(crate::Error::Message(format!(
            "unsupported coordinator URL scheme: {base}"
        )));
    };
    let rest = base
        .strip_prefix("https://")
        .or_else(|| base.strip_prefix("http://"))
        .unwrap_or(base);
    let encoded_id: String = node_id
        .bytes()
        .flat_map(|b| {
            if b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.' || b == b'~' {
                vec![b as char]
            } else {
                format!("%{b:02X}").chars().collect()
            }
        })
        .collect();
    Ok(format!("{scheme}://{rest}/v1/nodes/{encoded_id}/egress"))
}

async fn handle_client(tcp: TcpStream, relay: &ProxyRelay) -> Result<(), String> {
    let mut peek = [0u8; 1];
    tcp.peek(&mut peek)
        .await
        .map_err(|e| format!("peek: {e}"))?;

    match peek[0] {
        // SOCKS5 starts with 0x05.
        0x05 => handle_socks5(tcp, relay).await,
        // HTTP CONNECT starts with ASCII (C for CONNECT).
        _ => handle_http_connect(tcp, relay).await,
    }
}

async fn handle_http_connect(mut tcp: TcpStream, relay: &ProxyRelay) -> Result<(), String> {
    let mut buf = Vec::with_capacity(4096);
    let mut tmp = [0u8; 4096];
    loop {
        let n = tcp.read(&mut tmp).await.map_err(|e| format!("read: {e}"))?;
        if n == 0 {
            return Err("client disconnected before CONNECT".into());
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
        if buf.len() > 8192 {
            return Err("request too large".into());
        }
    }

    let header = String::from_utf8_lossy(&buf);
    let first_line = header.lines().next().unwrap_or("");
    let parts: Vec<&str> = first_line.split_whitespace().collect();
    if parts.len() < 3 || parts[0] != "CONNECT" {
        tcp.write_all(b"HTTP/1.1 405 Method Not Allowed\r\n\r\n")
            .await
            .ok();
        return Err(format!("not a CONNECT request: {first_line}"));
    }

    let authority = parts[1];
    let (host, port) = parse_authority(authority)?;

    let stream_id = generate_stream_id();
    let (ev_tx, mut ev_rx) = mpsc::channel::<StreamEvent>(64);
    relay.streams.lock().await.insert(stream_id.clone(), ev_tx);

    relay
        .ws_tx
        .send(Message::Text(
            serde_json::to_string(&json!({
                "type": "open",
                "stream_id": stream_id,
                "host": host,
                "port": port,
            }))
            .unwrap_or_default()
            .into(),
        ))
        .await
        .map_err(|_| "ws send failed".to_string())?;

    // Wait for opened or error.
    match wait_for_opened(&mut ev_rx).await? {
        true => {
            tcp.write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                .await
                .map_err(|e| format!("write: {e}"))?;
        }
        false => {
            relay.streams.lock().await.remove(&stream_id);
            return Err("stream open failed".into());
        }
    }

    relay_bidirectional(tcp, &stream_id, relay, &mut ev_rx).await;
    relay.streams.lock().await.remove(&stream_id);
    Ok(())
}

async fn handle_socks5(mut tcp: TcpStream, relay: &ProxyRelay) -> Result<(), String> {
    // Read greeting.
    let mut header = [0u8; 2];
    tcp.read_exact(&mut header)
        .await
        .map_err(|e| format!("socks5 greeting: {e}"))?;
    if header[0] != 0x05 {
        return Err("not SOCKS5".into());
    }
    let nmethods = header[1] as usize;
    let mut methods = vec![0u8; nmethods];
    tcp.read_exact(&mut methods)
        .await
        .map_err(|e| format!("socks5 methods: {e}"))?;

    // Reply: NO AUTH (0x00) on loopback.
    tcp.write_all(&[0x05, 0x00])
        .await
        .map_err(|e| format!("socks5 auth reply: {e}"))?;

    // Read connect request.
    let mut req_header = [0u8; 4];
    tcp.read_exact(&mut req_header)
        .await
        .map_err(|e| format!("socks5 request: {e}"))?;
    if req_header[0] != 0x05 || req_header[1] != 0x01 {
        // Only CONNECT (0x01) is supported.
        tcp.write_all(&[0x05, 0x07, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
            .await
            .ok();
        return Err("only SOCKS5 CONNECT supported".into());
    }

    let (host, port) = match req_header[3] {
        0x01 => {
            // IPv4
            let mut addr = [0u8; 4];
            tcp.read_exact(&mut addr)
                .await
                .map_err(|e| format!("socks5 ipv4: {e}"))?;
            let mut port_buf = [0u8; 2];
            tcp.read_exact(&mut port_buf)
                .await
                .map_err(|e| format!("socks5 port: {e}"))?;
            let port = u16::from_be_bytes(port_buf);
            (
                format!("{}.{}.{}.{}", addr[0], addr[1], addr[2], addr[3]),
                port,
            )
        }
        0x03 => {
            // Domain name.
            let mut len = [0u8; 1];
            tcp.read_exact(&mut len)
                .await
                .map_err(|e| format!("socks5 domain len: {e}"))?;
            let mut domain = vec![0u8; len[0] as usize];
            tcp.read_exact(&mut domain)
                .await
                .map_err(|e| format!("socks5 domain: {e}"))?;
            let mut port_buf = [0u8; 2];
            tcp.read_exact(&mut port_buf)
                .await
                .map_err(|e| format!("socks5 port: {e}"))?;
            let port = u16::from_be_bytes(port_buf);
            (String::from_utf8_lossy(&domain).to_string(), port)
        }
        0x04 => {
            // IPv6
            let mut addr = [0u8; 16];
            tcp.read_exact(&mut addr)
                .await
                .map_err(|e| format!("socks5 ipv6: {e}"))?;
            let mut port_buf = [0u8; 2];
            tcp.read_exact(&mut port_buf)
                .await
                .map_err(|e| format!("socks5 port: {e}"))?;
            let port = u16::from_be_bytes(port_buf);
            let ip = std::net::Ipv6Addr::from(addr);
            (ip.to_string(), port)
        }
        atyp => {
            tcp.write_all(&[0x05, 0x08, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
                .await
                .ok();
            return Err(format!("unsupported SOCKS5 address type: {atyp}"));
        }
    };

    let stream_id = generate_stream_id();
    let (ev_tx, mut ev_rx) = mpsc::channel::<StreamEvent>(64);
    relay.streams.lock().await.insert(stream_id.clone(), ev_tx);

    relay
        .ws_tx
        .send(Message::Text(
            serde_json::to_string(&json!({
                "type": "open",
                "stream_id": stream_id,
                "host": host,
                "port": port,
            }))
            .unwrap_or_default()
            .into(),
        ))
        .await
        .map_err(|_| "ws send failed".to_string())?;

    match wait_for_opened(&mut ev_rx).await? {
        true => {
            // Success reply: 0x05 0x00 0x00 ATYP(0x01) BIND.ADDR(0.0.0.0) BIND.PORT(0)
            tcp.write_all(&[0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
                .await
                .map_err(|e| format!("socks5 reply: {e}"))?;
        }
        false => {
            tcp.write_all(&[0x05, 0x05, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
                .await
                .ok();
            relay.streams.lock().await.remove(&stream_id);
            return Err("stream open failed".into());
        }
    }

    relay_bidirectional(tcp, &stream_id, relay, &mut ev_rx).await;
    relay.streams.lock().await.remove(&stream_id);
    Ok(())
}

async fn wait_for_opened(ev_rx: &mut mpsc::Receiver<StreamEvent>) -> Result<bool, String> {
    match tokio::time::timeout(std::time::Duration::from_secs(30), ev_rx.recv()).await {
        Ok(Some(StreamEvent::Opened)) => Ok(true),
        Ok(Some(StreamEvent::Error(e))) => {
            eprintln!("stream error: {e}");
            Ok(false)
        }
        Ok(Some(StreamEvent::Closed)) => Ok(false),
        Ok(None) => Err("ws disconnected".into()),
        Ok(Some(StreamEvent::Data(_))) => Err("unexpected data before opened".into()),
        Err(_) => Err("stream open timed out".into()),
    }
}

async fn relay_bidirectional(
    tcp: TcpStream,
    stream_id: &str,
    relay: &ProxyRelay,
    ev_rx: &mut mpsc::Receiver<StreamEvent>,
) {
    let (mut tcp_rx, mut tcp_tx) = tcp.into_split();
    let ws_tx = relay.ws_tx.clone();
    let sid = stream_id.to_string();

    // TCP -> WS (client writes to remote via coordinator).
    let sid_read = sid.clone();
    let read_task = tokio::spawn(async move {
        let mut buf = vec![0u8; STREAM_READ_BUF];
        loop {
            match tcp_rx.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    let data_b64 = base64::engine::general_purpose::STANDARD.encode(&buf[..n]);
                    if ws_tx
                        .send(Message::Text(
                            serde_json::to_string(&json!({
                                "type": "data",
                                "stream_id": sid_read,
                                "data_b64": data_b64,
                            }))
                            .unwrap_or_default()
                            .into(),
                        ))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });

    // WS -> TCP (remote data to client).
    loop {
        match ev_rx.recv().await {
            Some(StreamEvent::Data(data)) => {
                if tcp_tx.write_all(&data).await.is_err() {
                    break;
                }
            }
            Some(StreamEvent::Closed) | Some(StreamEvent::Error(_)) | None => break,
            Some(StreamEvent::Opened) => {}
        }
    }

    read_task.abort();

    // Send close to coordinator.
    let _ = relay
        .ws_tx
        .send(Message::Text(
            serde_json::to_string(&json!({
                "type": "close",
                "stream_id": sid,
            }))
            .unwrap_or_default()
            .into(),
        ))
        .await;
}

fn parse_authority(authority: &str) -> Result<(String, u16), String> {
    // Handle [ipv6]:port format.
    if let Some(bracket_end) = authority.rfind(']') {
        let host = &authority[1..bracket_end];
        let port_str = authority
            .get(bracket_end + 2..)
            .ok_or("missing port after IPv6")?;
        let port: u16 = port_str.parse().map_err(|_| "invalid port")?;
        return Ok((host.to_string(), port));
    }
    let colon = authority.rfind(':').ok_or("missing port in authority")?;
    let host = &authority[..colon];
    let port: u16 = authority[colon + 1..]
        .parse()
        .map_err(|_| "invalid port".to_string())?;
    Ok((host.to_string(), port))
}

fn generate_stream_id() -> String {
    format!(
        "ps_{}",
        anyagents_core::MessageId::generate()
            .as_str()
            .trim_start_matches("msg_")
    )
}
