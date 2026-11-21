//! Node-side multiplexed TCP egress stream handler for the `exit` capability.
//!
//! When the coordinator dispatches a `StreamOpen { host, port }`, the node
//! resolves DNS, validates against the SSRF guard, and dials a TCP connection.
//! Data flows bidirectionally as `StreamData` frames with per-stream
//! backpressure, concurrency cap, and idle timeout.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use base64::Engine as _;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, Semaphore, mpsc};

use crate::node_cli::ssrf_guard::{EgressPolicy, SsrfDecision, SsrfGuard};
use crate::node_protocol::{
    MAX_STREAM_CONCURRENCY, NodeClientFrame, STREAM_IDLE_TIMEOUT, STREAM_WINDOW_BYTES,
};

const STREAM_READ_BUF: usize = 16 * 1024;

/// Per-stream tracking on the node side.
struct ActiveStream {
    /// Cancel signal: closed when the coordinator sends `StreamClose`.
    cancel_tx: mpsc::Sender<()>,
    /// Forward data from the coordinator into the TCP write half.
    write_tx: mpsc::Sender<Vec<u8>>,
}

/// Node-side egress stream manager.
pub(crate) struct EgressManager {
    guard: SsrfGuard,
    streams: Arc<Mutex<HashMap<String, ActiveStream>>>,
    semaphore: Arc<Semaphore>,
    total_bytes_tx: Arc<AtomicU64>,
    total_bytes_rx: Arc<AtomicU64>,
}

impl EgressManager {
    pub(crate) fn new(policy: &EgressPolicy) -> Self {
        Self {
            guard: SsrfGuard::from_policy(policy),
            streams: Arc::new(Mutex::new(HashMap::new())),
            semaphore: Arc::new(Semaphore::new(MAX_STREAM_CONCURRENCY)),
            total_bytes_tx: Arc::new(AtomicU64::new(0)),
            total_bytes_rx: Arc::new(AtomicU64::new(0)),
        }
    }

    pub(crate) async fn active_count(&self) -> usize {
        self.streams.lock().await.len()
    }

    /// Handle a `StreamOpen` from the coordinator.
    pub(crate) async fn open_stream(
        &self,
        stream_id: String,
        host: String,
        port: u16,
        out_tx: mpsc::Sender<NodeClientFrame>,
    ) {
        // Check concurrency.
        let permit = match self.semaphore.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                let _ = out_tx
                    .send(NodeClientFrame::StreamError {
                        stream_id,
                        error: "too many concurrent egress streams".into(),
                    })
                    .await;
                return;
            }
        };

        // Resolve DNS.
        let addrs = match tokio::net::lookup_host(format!("{host}:{port}")).await {
            Ok(addrs) => addrs.collect::<Vec<_>>(),
            Err(err) => {
                let _ = out_tx
                    .send(NodeClientFrame::StreamError {
                        stream_id,
                        error: format!("DNS resolution failed for {host}:{port}: {err}"),
                    })
                    .await;
                return;
            }
        };

        if addrs.is_empty() {
            let _ = out_tx
                .send(NodeClientFrame::StreamError {
                    stream_id,
                    error: format!("no addresses resolved for {host}:{port}"),
                })
                .await;
            return;
        }

        // SSRF guard: check ALL resolved addresses.
        match self.guard.check_host(&host, port, &addrs) {
            SsrfDecision::Allow => {}
            SsrfDecision::Deny(reason) => {
                let _ = out_tx
                    .send(NodeClientFrame::StreamError {
                        stream_id,
                        error: reason,
                    })
                    .await;
                return;
            }
        }

        // Connect to the first resolved address that works.
        let tcp = match connect_first(&addrs).await {
            Ok(tcp) => tcp,
            Err(err) => {
                let _ = out_tx
                    .send(NodeClientFrame::StreamError {
                        stream_id,
                        error: format!("TCP connect to {host}:{port} failed: {err}"),
                    })
                    .await;
                return;
            }
        };

        // Register the stream.
        let (cancel_tx, cancel_rx) = mpsc::channel::<()>(1);
        let (write_tx, write_rx) =
            mpsc::channel::<Vec<u8>>(STREAM_WINDOW_BYTES / STREAM_READ_BUF + 1);
        self.streams.lock().await.insert(
            stream_id.clone(),
            ActiveStream {
                cancel_tx,
                write_tx,
            },
        );

        // Notify coordinator that the stream is open.
        if out_tx
            .send(NodeClientFrame::StreamOpened {
                stream_id: stream_id.clone(),
            })
            .await
            .is_err()
        {
            self.streams.lock().await.remove(&stream_id);
            return;
        }

        // Spawn the bidirectional relay.
        let streams = Arc::clone(&self.streams);
        let bytes_tx = Arc::clone(&self.total_bytes_tx);
        let bytes_rx = Arc::clone(&self.total_bytes_rx);
        tokio::spawn(async move {
            let (stream_bytes_tx, stream_bytes_rx) =
                relay_stream(tcp, &stream_id, out_tx.clone(), cancel_rx, write_rx).await;
            bytes_tx.fetch_add(stream_bytes_tx, Ordering::Relaxed);
            bytes_rx.fetch_add(stream_bytes_rx, Ordering::Relaxed);
            streams.lock().await.remove(&stream_id);
            let _ = out_tx
                .send(NodeClientFrame::StreamClosed {
                    stream_id,
                    bytes_tx: stream_bytes_tx,
                    bytes_rx: stream_bytes_rx,
                })
                .await;
            drop(permit);
        });
    }

    /// Handle inbound `StreamData` from the coordinator (data TO the remote).
    pub(crate) async fn write_to_stream(&self, stream_id: &str, data_b64: &str) {
        if let Ok(data) = base64::engine::general_purpose::STANDARD.decode(data_b64.as_bytes())
            && let Some(stream) = self.streams.lock().await.get(stream_id)
        {
            let _ = stream.write_tx.send(data).await;
        }
    }

    /// Handle a `StreamClose` from the coordinator.
    pub(crate) async fn close_stream(&self, stream_id: &str) {
        if let Some(stream) = self.streams.lock().await.get(stream_id) {
            let _ = stream.cancel_tx.try_send(());
        }
    }

    /// Close all active streams (used during pause/revoke).
    pub(crate) async fn close_all(&self) {
        let streams: Vec<_> = self.streams.lock().await.drain().collect();
        for (_, stream) in streams {
            let _ = stream.cancel_tx.try_send(());
        }
    }
}

async fn connect_first(addrs: &[std::net::SocketAddr]) -> Result<TcpStream, String> {
    let mut last_err = String::new();
    for addr in addrs {
        match tokio::time::timeout(Duration::from_secs(10), TcpStream::connect(addr)).await {
            Ok(Ok(tcp)) => return Ok(tcp),
            Ok(Err(err)) => last_err = err.to_string(),
            Err(_) => last_err = "connection timed out".into(),
        }
    }
    Err(last_err)
}

/// Bidirectional relay between a TCP stream and WS frames.
/// Returns (bytes sent to remote, bytes received from remote).
async fn relay_stream(
    tcp: TcpStream,
    stream_id: &str,
    out_tx: mpsc::Sender<NodeClientFrame>,
    mut cancel_rx: mpsc::Receiver<()>,
    mut inbound_rx: mpsc::Receiver<Vec<u8>>,
) -> (u64, u64) {
    let (mut tcp_rx, mut tcp_tx) = tcp.into_split();
    let mut bytes_tx = 0u64;
    let mut bytes_rx = 0u64;

    // Relay: TCP read → WS frames (node→coordinator)
    let sid_read = stream_id.to_string();
    let out_read = out_tx.clone();
    let read_task = tokio::spawn(async move {
        let mut total = 0u64;
        let mut buf = vec![0u8; STREAM_READ_BUF];
        loop {
            match tokio::time::timeout(STREAM_IDLE_TIMEOUT, tcp_rx.read(&mut buf)).await {
                Ok(Ok(0)) | Err(_) => break,
                Ok(Ok(n)) => {
                    total += n as u64;
                    let data_b64 = base64::engine::general_purpose::STANDARD.encode(&buf[..n]);
                    if out_read
                        .send(NodeClientFrame::StreamData {
                            stream_id: sid_read.clone(),
                            data_b64,
                        })
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Ok(Err(_)) => break,
            }
        }
        total
    });

    // Relay: WS frames (coordinator→node) → TCP write
    let write_task = tokio::spawn(async move {
        let mut total = 0u64;
        while let Some(data) = inbound_rx.recv().await {
            total += data.len() as u64;
            if tcp_tx.write_all(&data).await.is_err() {
                break;
            }
        }
        total
    });

    tokio::select! {
        _ = cancel_rx.recv() => {}
        result = read_task => {
            if let Ok(n) = result {
                bytes_rx = n;
            }
        }
    }
    if let Ok(n) = write_task.await {
        bytes_tx = n;
    }
    (bytes_tx, bytes_rx)
}
