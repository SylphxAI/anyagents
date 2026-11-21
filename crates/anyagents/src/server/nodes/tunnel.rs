//! Outbound WebSocket reverse-tunnel registry and connection loop for edge nodes.

use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex, OnceLock, Weak};
use std::time::Duration;

use anyagents_core::{NodeId, NodeStatus, Store};
use axum::body::Bytes;
use axum::extract::ws::{CloseFrame, Message, Utf8Bytes, WebSocket};
use base64::Engine as _;
use futures::{SinkExt, StreamExt};
use tokio::sync::{Mutex, RwLock, mpsc};

use crate::node_protocol::{
    FilesEntry, KEEPALIVE_INTERVAL, MAX_EXEC_OUTPUT_BYTES, NodeClientFrame, NodeServerFrame,
    OutputStream,
};

#[derive(Debug)]
pub(super) enum ExecTunnelEvent {
    Output {
        stream: OutputStream,
        bytes: Vec<u8>,
    },
    Done {
        exit_code: Option<i32>,
        timed_out: bool,
        cancelled: bool,
        error: Option<String>,
        duration_ms: u64,
    },
    Interrupted {
        reason: String,
    },
}

/// Event from a files request dispatched through the tunnel.
#[derive(Debug)]
pub(super) enum FilesTunnelEvent {
    Result {
        entries: Option<Vec<FilesEntry>>,
        data_b64: Option<String>,
        error: Option<String>,
    },
    Interrupted {
        reason: String,
    },
}

#[derive(Debug)]
pub(super) struct ExecOutcome {
    pub(super) exit_code: Option<i32>,
    pub(super) timed_out: bool,
    pub(super) cancelled: bool,
    pub(super) error: Option<String>,
    pub(super) stdout: String,
    pub(super) stderr: String,
    pub(super) stdout_truncated: bool,
    pub(super) stderr_truncated: bool,
    pub(super) duration_ms: u64,
}

struct NodeConn {
    generation: u64,
    tx: mpsc::Sender<NodeServerFrame>,
    close_tx: mpsc::Sender<()>,
    execs: Arc<Mutex<HashMap<String, mpsc::Sender<ExecTunnelEvent>>>>,
    files_reqs: Arc<Mutex<HashMap<String, mpsc::Sender<FilesTunnelEvent>>>>,
}

/// Bounded in-memory registry of active outbound node WebSocket tunnels.
pub(crate) struct NodeRegistry {
    conns: RwLock<HashMap<String, Arc<NodeConn>>>,
    next_gen: std::sync::atomic::AtomicU64,
}

impl NodeRegistry {
    fn new() -> Self {
        Self {
            conns: RwLock::new(HashMap::new()),
            next_gen: std::sync::atomic::AtomicU64::new(1),
        }
    }

    pub(crate) async fn is_connected(&self, node_id: &NodeId) -> bool {
        self.conns.read().await.contains_key(node_id.as_str())
    }

    async fn attach(
        &self,
        node_id: &NodeId,
        tx: mpsc::Sender<NodeServerFrame>,
        close_tx: mpsc::Sender<()>,
    ) -> Arc<NodeConn> {
        let generation = self
            .next_gen
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let conn = Arc::new(NodeConn {
            generation,
            tx,
            close_tx,
            execs: Arc::new(Mutex::new(HashMap::new())),
            files_reqs: Arc::new(Mutex::new(HashMap::new())),
        });
        let previous = self
            .conns
            .write()
            .await
            .insert(node_id.as_str().to_string(), Arc::clone(&conn));
        if let Some(prev) = previous {
            fail_all_execs(&prev.execs, "node tunnel replaced by a newer connection").await;
            let _ = prev.close_tx.try_send(());
        }
        conn
    }

    async fn detach_if_current(&self, node_id: &NodeId, conn: &Arc<NodeConn>) -> bool {
        let mut map = self.conns.write().await;
        let is_current = map
            .get(node_id.as_str())
            .is_some_and(|cur| cur.generation == conn.generation);
        if is_current {
            map.remove(node_id.as_str());
            drop(map);
            fail_all_execs(&conn.execs, "node tunnel disconnected").await;
            fail_all_files(&conn.files_reqs, "node tunnel disconnected").await;
            true
        } else {
            false
        }
    }

    pub(super) async fn pause_node(&self, node_id: &NodeId, reason: &str) {
        let conn = self.conns.read().await.get(node_id.as_str()).cloned();
        if let Some(conn) = conn {
            let _ = conn
                .tx
                .send(NodeServerFrame::Control {
                    action: "pause".into(),
                    reason: Some(reason.to_string()),
                })
                .await;
            fail_all_execs(&conn.execs, reason).await;
            fail_all_files(&conn.files_reqs, reason).await;
        }
    }

    pub(super) async fn resume_node(&self, node_id: &NodeId) {
        let conn = self.conns.read().await.get(node_id.as_str()).cloned();
        if let Some(conn) = conn {
            let _ = conn
                .tx
                .send(NodeServerFrame::Control {
                    action: "resume".into(),
                    reason: None,
                })
                .await;
        }
    }

    pub(super) async fn revoke_node(&self, node_id: &NodeId, reason: &str) {
        let conn = self.conns.write().await.remove(node_id.as_str());
        if let Some(conn) = conn {
            let _ = conn
                .tx
                .send(NodeServerFrame::Control {
                    action: "revoke".into(),
                    reason: Some(reason.to_string()),
                })
                .await;
            fail_all_execs(&conn.execs, reason).await;
            fail_all_files(&conn.files_reqs, reason).await;
            let _ = conn.close_tx.try_send(());
        }
    }

    pub(super) async fn dispatch_exec(
        &self,
        node_id: &NodeId,
        exec_id: String,
        command: String,
        args: Vec<String>,
        cwd: Option<String>,
        timeout_secs: u64,
    ) -> Result<ExecOutcome, String> {
        let conn = self
            .conns
            .read()
            .await
            .get(node_id.as_str())
            .cloned()
            .ok_or_else(|| format!("node `{node_id}` has no active WebSocket tunnel"))?;

        let (ev_tx, mut ev_rx) = mpsc::channel::<ExecTunnelEvent>(128);
        conn.execs.lock().await.insert(exec_id.clone(), ev_tx);

        if conn
            .tx
            .send(NodeServerFrame::ExecStart {
                exec_id: exec_id.clone(),
                command,
                args,
                cwd,
                timeout_secs,
            })
            .await
            .is_err()
        {
            conn.execs.lock().await.remove(&exec_id);
            return Err(format!("node `{node_id}` tunnel closed before dispatch"));
        }

        let started = tokio::time::Instant::now();
        let deadline = started + Duration::from_secs(timeout_secs.saturating_add(2));
        let mut stdout_buf = Vec::new();
        let mut stderr_buf = Vec::new();
        let mut stdout_truncated = false;
        let mut stderr_truncated = false;

        loop {
            let now = tokio::time::Instant::now();
            if now >= deadline {
                let _ = conn
                    .tx
                    .send(NodeServerFrame::ExecCancel {
                        exec_id: exec_id.clone(),
                        reason: "coordinator exec timeout".into(),
                    })
                    .await;
                conn.execs.lock().await.remove(&exec_id);
                return Ok(ExecOutcome {
                    exit_code: None,
                    timed_out: true,
                    cancelled: false,
                    error: Some("execution timed out".into()),
                    stdout: String::from_utf8_lossy(&stdout_buf).into_owned(),
                    stderr: String::from_utf8_lossy(&stderr_buf).into_owned(),
                    stdout_truncated,
                    stderr_truncated,
                    duration_ms: started.elapsed().as_millis() as u64,
                });
            }

            let wait = tokio::time::timeout(deadline - now, ev_rx.recv()).await;
            match wait {
                Ok(Some(ExecTunnelEvent::Output { stream, bytes })) => match stream {
                    OutputStream::Stdout => {
                        append_bounded(&mut stdout_buf, &bytes, &mut stdout_truncated);
                    }
                    OutputStream::Stderr => {
                        append_bounded(&mut stderr_buf, &bytes, &mut stderr_truncated);
                    }
                },
                Ok(Some(ExecTunnelEvent::Done {
                    exit_code,
                    timed_out,
                    cancelled,
                    error,
                    duration_ms,
                })) => {
                    conn.execs.lock().await.remove(&exec_id);
                    return Ok(ExecOutcome {
                        exit_code,
                        timed_out,
                        cancelled,
                        error,
                        stdout: String::from_utf8_lossy(&stdout_buf).into_owned(),
                        stderr: String::from_utf8_lossy(&stderr_buf).into_owned(),
                        stdout_truncated,
                        stderr_truncated,
                        duration_ms,
                    });
                }
                Ok(Some(ExecTunnelEvent::Interrupted { reason })) => {
                    let _ = conn
                        .tx
                        .send(NodeServerFrame::ExecCancel {
                            exec_id: exec_id.clone(),
                            reason: reason.clone(),
                        })
                        .await;
                    conn.execs.lock().await.remove(&exec_id);
                    return Ok(ExecOutcome {
                        exit_code: None,
                        timed_out: false,
                        cancelled: true,
                        error: Some(reason),
                        stdout: String::from_utf8_lossy(&stdout_buf).into_owned(),
                        stderr: String::from_utf8_lossy(&stderr_buf).into_owned(),
                        stdout_truncated,
                        stderr_truncated,
                        duration_ms: started.elapsed().as_millis() as u64,
                    });
                }
                Ok(None) => {
                    conn.execs.lock().await.remove(&exec_id);
                    return Ok(ExecOutcome {
                        exit_code: None,
                        timed_out: false,
                        cancelled: true,
                        error: Some("node tunnel disconnected".into()),
                        stdout: String::from_utf8_lossy(&stdout_buf).into_owned(),
                        stderr: String::from_utf8_lossy(&stderr_buf).into_owned(),
                        stdout_truncated,
                        stderr_truncated,
                        duration_ms: started.elapsed().as_millis() as u64,
                    });
                }
                Err(_) => continue,
            }
        }
    }

    /// Dispatch a `files_list` request to a connected node and wait for its response.
    pub(super) async fn dispatch_files_list(
        &self,
        node_id: &NodeId,
        path: String,
    ) -> Result<Vec<FilesEntry>, String> {
        let (conn, req_id, mut ev_rx) = self.prepare_files_req(node_id).await?;
        if conn
            .tx
            .send(NodeServerFrame::FilesList {
                req_id: req_id.clone(),
                path,
            })
            .await
            .is_err()
        {
            conn.files_reqs.lock().await.remove(&req_id);
            return Err(format!("node `{node_id}` tunnel closed"));
        }
        match wait_files_event(&mut ev_rx, &conn, &req_id).await {
            Ok(FilesTunnelEvent::Result {
                entries: Some(entries),
                error: None,
                ..
            }) => Ok(entries),
            Ok(FilesTunnelEvent::Result {
                error: Some(err), ..
            }) => Err(err),
            Ok(FilesTunnelEvent::Interrupted { reason }) => Err(reason),
            _ => Err("unexpected files response".into()),
        }
    }

    /// Dispatch a `files_read` request to a connected node.
    pub(super) async fn dispatch_files_read(
        &self,
        node_id: &NodeId,
        path: String,
    ) -> Result<String, String> {
        let (conn, req_id, mut ev_rx) = self.prepare_files_req(node_id).await?;
        if conn
            .tx
            .send(NodeServerFrame::FilesRead {
                req_id: req_id.clone(),
                path,
            })
            .await
            .is_err()
        {
            conn.files_reqs.lock().await.remove(&req_id);
            return Err(format!("node `{node_id}` tunnel closed"));
        }
        match wait_files_event(&mut ev_rx, &conn, &req_id).await {
            Ok(FilesTunnelEvent::Result {
                data_b64: Some(data),
                error: None,
                ..
            }) => Ok(data),
            Ok(FilesTunnelEvent::Result {
                error: Some(err), ..
            }) => Err(err),
            Ok(FilesTunnelEvent::Interrupted { reason }) => Err(reason),
            _ => Err("unexpected files response".into()),
        }
    }

    /// Dispatch a `files_write` request to a connected node.
    pub(super) async fn dispatch_files_write(
        &self,
        node_id: &NodeId,
        path: String,
        data_b64: String,
    ) -> Result<(), String> {
        let (conn, req_id, mut ev_rx) = self.prepare_files_req(node_id).await?;
        if conn
            .tx
            .send(NodeServerFrame::FilesWrite {
                req_id: req_id.clone(),
                path,
                data_b64,
            })
            .await
            .is_err()
        {
            conn.files_reqs.lock().await.remove(&req_id);
            return Err(format!("node `{node_id}` tunnel closed"));
        }
        match wait_files_event(&mut ev_rx, &conn, &req_id).await {
            Ok(FilesTunnelEvent::Result { error: None, .. }) => Ok(()),
            Ok(FilesTunnelEvent::Result {
                error: Some(err), ..
            }) => Err(err),
            Ok(FilesTunnelEvent::Interrupted { reason }) => Err(reason),
            _ => Err("unexpected files response".into()),
        }
    }

    async fn prepare_files_req(
        &self,
        node_id: &NodeId,
    ) -> Result<(Arc<NodeConn>, String, mpsc::Receiver<FilesTunnelEvent>), String> {
        let conn = self
            .conns
            .read()
            .await
            .get(node_id.as_str())
            .cloned()
            .ok_or_else(|| format!("node `{node_id}` has no active WebSocket tunnel"))?;
        let req_id = format!(
            "nfr_{}",
            anyagents_core::MessageId::generate()
                .as_str()
                .trim_start_matches("msg_")
        );
        let (ev_tx, ev_rx) = mpsc::channel::<FilesTunnelEvent>(2);
        conn.files_reqs.lock().await.insert(req_id.clone(), ev_tx);
        Ok((conn, req_id, ev_rx))
    }
}

async fn wait_files_event(
    ev_rx: &mut mpsc::Receiver<FilesTunnelEvent>,
    conn: &Arc<NodeConn>,
    req_id: &str,
) -> Result<FilesTunnelEvent, String> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    match tokio::time::timeout_at(deadline, ev_rx.recv()).await {
        Ok(Some(event)) => {
            conn.files_reqs.lock().await.remove(req_id);
            Ok(event)
        }
        Ok(None) => {
            conn.files_reqs.lock().await.remove(req_id);
            Err("node tunnel disconnected".into())
        }
        Err(_) => {
            conn.files_reqs.lock().await.remove(req_id);
            Err("files request timed out".into())
        }
    }
}

fn append_bounded(buf: &mut Vec<u8>, chunk: &[u8], truncated: &mut bool) {
    if buf.len() >= MAX_EXEC_OUTPUT_BYTES {
        if !chunk.is_empty() {
            *truncated = true;
        }
        return;
    }
    let avail = MAX_EXEC_OUTPUT_BYTES - buf.len();
    if chunk.len() > avail {
        buf.extend_from_slice(&chunk[..avail]);
        *truncated = true;
    } else {
        buf.extend_from_slice(chunk);
    }
}

async fn fail_all_execs(
    execs: &Mutex<HashMap<String, mpsc::Sender<ExecTunnelEvent>>>,
    reason: &str,
) {
    let entries: Vec<_> = execs.lock().await.drain().collect();
    for (_, tx) in entries {
        let _ = tx.try_send(ExecTunnelEvent::Interrupted {
            reason: reason.to_string(),
        });
    }
}

async fn fail_all_files(
    reqs: &Mutex<HashMap<String, mpsc::Sender<FilesTunnelEvent>>>,
    reason: &str,
) {
    let entries: Vec<_> = reqs.lock().await.drain().collect();
    for (_, tx) in entries {
        let _ = tx.try_send(FilesTunnelEvent::Interrupted {
            reason: reason.to_string(),
        });
    }
}

struct StoreRegistrySlot {
    store: Weak<Store>,
    registry: Arc<NodeRegistry>,
}

fn lock_store_registries() -> std::sync::MutexGuard<'static, HashMap<usize, StoreRegistrySlot>> {
    static MAP: OnceLock<StdMutex<HashMap<usize, StoreRegistrySlot>>> = OnceLock::new();
    let mut map = MAP
        .get_or_init(|| StdMutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    map.retain(|_, slot| slot.store.strong_count() > 0);
    map
}

pub(crate) fn registry_for_store(store: &Arc<Store>) -> Arc<NodeRegistry> {
    let key = Arc::as_ptr(store) as usize;
    let weak = Arc::downgrade(store);
    let mut map = lock_store_registries();
    if let Some(slot) = map.get(&key)
        && Weak::ptr_eq(&slot.store, &weak)
    {
        return Arc::clone(&slot.registry);
    }
    let registry = Arc::new(NodeRegistry::new());
    map.insert(
        key,
        StoreRegistrySlot {
            store: weak,
            registry: Arc::clone(&registry),
        },
    );
    registry
}

pub(super) async fn run_node_tunnel(
    socket: WebSocket,
    registry: Arc<NodeRegistry>,
    store: Arc<Store>,
    node_id: NodeId,
) {
    let (mut ws_tx, mut ws_rx) = socket.split();
    let (out_tx, mut out_rx) = mpsc::channel::<NodeServerFrame>(128);
    let (close_tx, mut close_rx) = mpsc::channel::<()>(1);
    let conn = registry.attach(&node_id, out_tx.clone(), close_tx).await;

    let store_conn = Arc::clone(&store);
    let nid = node_id.clone();
    let initial =
        tokio::task::spawn_blocking(move || store_conn.set_node_connected(&nid, None)).await;
    let initial_status = match initial {
        Ok(Ok(rec)) => rec.status,
        _ => {
            let _ = ws_tx
                .send(Message::Close(Some(CloseFrame {
                    code: 4403,
                    reason: Utf8Bytes::from_static("node revoked or unavailable"),
                })))
                .await;
            let _ = registry.detach_if_current(&node_id, &conn).await;
            return;
        }
    };

    if send_frame(
        &mut ws_tx,
        &NodeServerFrame::Welcome {
            node_id: node_id.to_string(),
            status: initial_status.as_str().to_string(),
        },
    )
    .await
    .is_err()
    {
        if registry.detach_if_current(&node_id, &conn).await {
            let s = Arc::clone(&store);
            let nid = node_id.clone();
            let _ = tokio::task::spawn_blocking(move || s.set_node_offline(&nid)).await;
        }
        return;
    }

    if initial_status == NodeStatus::Paused {
        let _ = send_frame(
            &mut ws_tx,
            &NodeServerFrame::Control {
                action: "pause".into(),
                reason: Some("node is paused".into()),
            },
        )
        .await;
    }

    let mut keepalive = tokio::time::interval(KEEPALIVE_INTERVAL);
    keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    keepalive.tick().await;

    loop {
        tokio::select! {
            _ = close_rx.recv() => {
                let _ = ws_tx.send(Message::Close(None)).await;
                break;
            }
            _ = keepalive.tick() => {
                if ws_tx.send(Message::Ping(Bytes::new())).await.is_err() {
                    break;
                }
            }
            Some(frame) = out_rx.recv() => {
                if send_frame(&mut ws_tx, &frame).await.is_err() {
                    break;
                }
            }
            incoming = ws_rx.next() => {
                let Some(msg) = incoming else { break; };
                match msg {
                    Ok(Message::Text(txt)) => {
                        let Ok(client_frame) = serde_json::from_str::<NodeClientFrame>(&txt) else {
                            continue;
                        };
                        match client_frame {
                            NodeClientFrame::Hello { hostname, os, arch, capabilities } => {
                                let s = Arc::clone(&store);
                                let nid = node_id.clone();
                                let res = tokio::task::spawn_blocking(move || {
                                    s.set_node_connected(&nid, Some((&hostname, &os, &arch, &capabilities)))
                                }).await;
                                match res {
                                    Ok(Ok(rec)) if rec.status == NodeStatus::Paused => {
                                        let _ = out_tx.try_send(NodeServerFrame::Control {
                                            action: "pause".into(),
                                            reason: Some("node is paused".into()),
                                        });
                                    }
                                    Ok(Ok(_)) => {}
                                    _ => break,
                                }
                            }
                            NodeClientFrame::Heartbeat { .. } => {
                                let s = Arc::clone(&store);
                                let nid = node_id.clone();
                                let res = tokio::task::spawn_blocking(move || s.touch_node_heartbeat(&nid)).await;
                                match res {
                                    Ok(Ok(rec)) if rec.status == NodeStatus::Revoked => break,
                                    Ok(Ok(rec)) if rec.status == NodeStatus::Paused => {
                                        let _ = out_tx.try_send(NodeServerFrame::Control {
                                            action: "pause".into(),
                                            reason: Some("node is paused".into()),
                                        });
                                    }
                                    Ok(Ok(_)) => {}
                                    _ => break,
                                }
                            }
                            NodeClientFrame::ExecOutput { exec_id, stream, data_b64 } => {
                                if let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(data_b64.as_bytes())
                                    && let Some(tx) = conn.execs.lock().await.get(&exec_id).cloned()
                                {
                                    let _ = tx.send(ExecTunnelEvent::Output { stream, bytes }).await;
                                }
                            }
                            NodeClientFrame::ExecDone { exec_id, exit_code, timed_out, cancelled, error, duration_ms } => {
                                if let Some(tx) = conn.execs.lock().await.remove(&exec_id) {
                                    let _ = tx.send(ExecTunnelEvent::Done {
                                        exit_code,
                                        timed_out,
                                        cancelled,
                                        error,
                                        duration_ms,
                                    }).await;
                                }
                            }
                            // ── Multiplexed TCP egress stream responses ──
                            NodeClientFrame::StreamOpened { .. }
                            | NodeClientFrame::StreamData { .. }
                            | NodeClientFrame::StreamError { .. }
                            | NodeClientFrame::StreamClosed { .. } => {
                                // Stream relay frames are handled by the
                                // coordinator's proxy dispatch (future work);
                                // log and discard on the tunnel side for now.
                            }
                            // ── Scoped files responses ──
                            NodeClientFrame::FilesResult { req_id, entries, data_b64, error } => {
                                if let Some(tx) = conn.files_reqs.lock().await.remove(&req_id) {
                                    let _ = tx.send(FilesTunnelEvent::Result {
                                        entries,
                                        data_b64,
                                        error,
                                    }).await;
                                }
                            }
                        }
                    }
                    Ok(Message::Ping(payload)) => {
                        if ws_tx.send(Message::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    Ok(Message::Pong(_)) => {}
                    Ok(Message::Close(_)) | Err(_) => break,
                    Ok(Message::Binary(_)) => {}
                }
            }
        }
    }

    if registry.detach_if_current(&node_id, &conn).await {
        let s = Arc::clone(&store);
        let nid = node_id;
        let _ = tokio::task::spawn_blocking(move || s.set_node_offline(&nid)).await;
    }
}

async fn send_frame(
    ws_tx: &mut futures::stream::SplitSink<WebSocket, Message>,
    frame: &NodeServerFrame,
) -> Result<(), ()> {
    let text = serde_json::to_string(frame).map_err(|_| ())?;
    ws_tx.send(Message::Text(text.into())).await.map_err(|_| ())
}
