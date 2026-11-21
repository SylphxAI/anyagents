//! Viewer HTTP and WebSocket transport to the authenticated guest.

use super::*;

pub(super) fn try_send_control(tunnel_tx: &mpsc::Sender<OutboundTunnelMsg>, frame: &ControlFrame) {
    if let Ok(msg) = OutboundTunnelMsg::control(frame) {
        let _ = tunnel_tx.try_send(msg);
    }
}

pub(super) async fn send_control_or_err(
    tunnel_tx: &mpsc::Sender<OutboundTunnelMsg>,
    frame: &ControlFrame,
) -> Result<(), (StatusCode, &'static str)> {
    let msg = OutboundTunnelMsg::control(frame)
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "failed encoding frame"))?;
    tunnel_tx
        .send(msg)
        .await
        .map_err(|_| (StatusCode::BAD_GATEWAY, "takeover tunnel disconnected"))
}

pub(super) async fn proxy_viewer_ws(
    mut ws_upgrade: WebSocketUpgrade,
    registry: Arc<TakeoverRegistry>,
    session: Arc<TakeoverSession>,
    path: String,
    query: Option<String>,
    headers: Vec<(String, String)>,
    mint_cookie: Option<HeaderValue>,
) -> Response {
    let (generation, tunnel_tx) = match session.wait_for_tunnel(registry.grace_window).await {
        Ok(pair) => pair,
        Err(status) => return json_error_response(status, "takeover tunnel is unavailable"),
    };
    let protocols: Vec<String> = ws_upgrade
        .requested_protocols()
        .filter_map(|v| v.to_str().ok().map(str::trim).filter(|s| !s.is_empty()))
        .map(str::to_string)
        .collect();
    let stream_id = session.next_ws_id.fetch_add(1, Ordering::Relaxed);
    let (opened_tx, opened_rx) = oneshot::channel();
    let (viewer_tx, viewer_rx) = mpsc::channel(VIEWER_WS_QUEUE);
    session.inflight_ws.lock().await.insert(
        stream_id,
        PendingWs {
            generation,
            opened_tx: Some(opened_tx),
            viewer_tx,
        },
    );

    let open = ControlFrame::WsOpen {
        id: stream_id,
        path,
        query,
        protocols,
        headers,
    };
    if let Err((st, msg)) = send_control_or_err(&tunnel_tx, &open).await {
        session.inflight_ws.lock().await.remove(&stream_id);
        return json_error_response(st, msg);
    }
    let opened = match tokio::time::timeout(WS_OPEN_TIMEOUT, opened_rx).await {
        Ok(Ok(res)) if res.ok => res,
        Ok(Ok(res)) => {
            session.inflight_ws.lock().await.remove(&stream_id);
            let msg = res.error.unwrap_or_else(|| "upstream ws rejected".into());
            return json_error_response(StatusCode::BAD_GATEWAY, &msg);
        }
        Ok(Err(_)) => {
            session.inflight_ws.lock().await.remove(&stream_id);
            return json_error_response(StatusCode::BAD_GATEWAY, "tunnel closed during ws open");
        }
        Err(_) => {
            session.inflight_ws.lock().await.remove(&stream_id);
            try_send_control(
                &tunnel_tx,
                &ControlFrame::WsClose {
                    id: stream_id,
                    code: None,
                    reason: None,
                },
            );
            return json_error_response(StatusCode::GATEWAY_TIMEOUT, "upstream ws open timed out");
        }
    };
    if let Some(proto) = opened.protocol
        && let Ok(val) = HeaderValue::from_str(&proto)
    {
        ws_upgrade.set_selected_protocol(val);
    }

    let keepalive = registry.keepalive_interval;
    let (fail_session, fail_tx) = (Arc::clone(&session), tunnel_tx.clone());
    let mut resp = ws_upgrade
        .max_message_size(MAX_TUNNEL_FRAME_BYTES)
        .max_frame_size(MAX_HTTP_RESP_BYTES)
        .on_failed_upgrade(move |_| {
            tokio::spawn(async move {
                fail_session.inflight_ws.lock().await.remove(&stream_id);
                try_send_control(
                    &fail_tx,
                    &ControlFrame::WsClose {
                        id: stream_id,
                        code: None,
                        reason: None,
                    },
                );
            });
        })
        .on_upgrade(move |sock| {
            pump_viewer_ws(sock, session, tunnel_tx, stream_id, viewer_rx, keepalive)
        });
    if let Some(cookie) = mint_cookie {
        resp.headers_mut().append(header::SET_COOKIE, cookie);
    }
    resp
}

async fn pump_viewer_ws(
    socket: WebSocket,
    session: Arc<TakeoverSession>,
    tunnel_tx: mpsc::Sender<OutboundTunnelMsg>,
    stream_id: u64,
    mut viewer_rx: mpsc::Receiver<ViewerWsMsg>,
    keepalive_interval: Duration,
) {
    let (mut ws_tx, mut ws_rx) = socket.split();
    let mut ping = tokio::time::interval_at(
        tokio::time::Instant::now() + keepalive_interval,
        keepalive_interval,
    );
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = ping.tick() => {
                if ws_tx.send(Message::Ping(Bytes::from_static(b"keepalive"))).await.is_err() { break; }
            }
            from_guest = viewer_rx.recv() => match from_guest {
                Some(ViewerWsMsg::Binary(b)) => if ws_tx.send(Message::Binary(b)).await.is_err() { break; },
                Some(ViewerWsMsg::Text(t)) => if ws_tx.send(Message::Text(t.into())).await.is_err() { break; },
                Some(ViewerWsMsg::Close(cf)) => { let _ = ws_tx.send(Message::Close(cf)).await; break; }
                None => { let _ = ws_tx.send(Message::Close(None)).await; break; }
            },
            from_viewer = ws_rx.next() => match from_viewer {
                Some(Ok(Message::Binary(b))) => {
                    if b.len() > MAX_HTTP_RESP_BYTES || tunnel_tx.send(OutboundTunnelMsg::ws_binary(stream_id, &b)).await.is_err() { break; }
                }
                Some(Ok(Message::Text(t))) => {
                    let frame = ControlFrame::WsText { id: stream_id, text: t.to_string() };
                    if t.len() > MAX_HTTP_RESP_BYTES || send_control_or_err(&tunnel_tx, &frame).await.is_err() { break; }
                }
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
                Some(Ok(Message::Close(cf))) => {
                    let (code, reason) = cf.map(|c| (Some(c.code), Some(c.reason.to_string()))).unwrap_or((None, None));
                    try_send_control(&tunnel_tx, &ControlFrame::WsClose { id: stream_id, code, reason });
                    break;
                }
                Some(Err(_)) | None => {
                    try_send_control(&tunnel_tx, &ControlFrame::WsClose { id: stream_id, code: None, reason: None });
                    break;
                }
            }
        }
    }
    session.inflight_ws.lock().await.remove(&stream_id);
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn proxy_viewer_http(
    registry: Arc<TakeoverRegistry>,
    session: Arc<TakeoverSession>,
    method: String,
    path: String,
    query: Option<String>,
    headers: Vec<(String, String)>,
    body: Body,
    mint_cookie: Option<HeaderValue>,
) -> Response {
    let Ok(collected) = http_body_util::Limited::new(body, MAX_HTTP_REQ_BODY_BYTES)
        .collect()
        .await
    else {
        return json_error_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            "request body exceeds 16 MiB limit",
        );
    };
    let body_bytes = collected.to_bytes();
    let (generation, tunnel_tx) = match session.wait_for_tunnel(registry.grace_window).await {
        Ok(pair) => pair,
        Err(status) => return json_error_response(status, "takeover tunnel is unavailable"),
    };

    let req_id = session.next_req_id.fetch_add(1, Ordering::Relaxed);
    let (head_tx, head_rx) = oneshot::channel();
    let (body_tx, body_rx) = mpsc::channel(HTTP_CHUNK_QUEUE);
    session.inflight_http.lock().await.insert(
        req_id,
        PendingHttp {
            generation,
            head_tx: Some(head_tx),
            body_tx,
            is_sse: false,
            bytes_received: 0,
        },
    );

    let req = ControlFrame::HttpRequest {
        id: req_id,
        method,
        path,
        query,
        headers,
        body_b64: if body_bytes.is_empty() {
            String::new()
        } else {
            base64::engine::general_purpose::STANDARD.encode(&body_bytes)
        },
    };
    if let Err((st, msg)) = send_control_or_err(&tunnel_tx, &req).await {
        session.inflight_http.lock().await.remove(&req_id);
        return json_error_response(st, msg);
    }

    let head = match tokio::time::timeout(HTTP_HEAD_TIMEOUT, head_rx).await {
        Ok(Ok(head)) => head,
        Ok(Err(_)) => {
            session.inflight_http.lock().await.remove(&req_id);
            return json_error_response(
                StatusCode::BAD_GATEWAY,
                "tunnel closed before upstream response",
            );
        }
        Err(_) => {
            session.inflight_http.lock().await.remove(&req_id);
            try_send_control(&tunnel_tx, &ControlFrame::HttpCancel { id: req_id });
            return json_error_response(
                StatusCode::GATEWAY_TIMEOUT,
                "upstream takeover response timed out",
            );
        }
    };

    let mut is_sse = false;
    let mut rb = Response::builder()
        .status(StatusCode::from_u16(head.status).unwrap_or(StatusCode::BAD_GATEWAY));
    for (k, v) in head.headers {
        let lower = k.to_ascii_lowercase();
        if is_hop_by_hop(&lower) || (!head.done && lower == "content-length") {
            continue;
        }
        is_sse |=
            lower == "content-type" && v.to_ascii_lowercase().starts_with("text/event-stream");
        if let (Ok(n), Ok(val)) = (
            HeaderName::from_bytes(lower.as_bytes()),
            HeaderValue::from_str(&v),
        ) {
            rb = rb.header(n, val);
        }
    }
    if is_sse {
        rb = rb.header("x-accel-buffering", "no");
    }
    let response_body = if head.done {
        session.inflight_http.lock().await.remove(&req_id);
        Body::empty()
    } else {
        Body::from_stream(TunneledHttpBody {
            rx: body_rx,
            session: Arc::clone(&session),
            tunnel_tx,
            req_id,
            finished: false,
        })
    };
    let mut resp = rb
        .body(response_body)
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
    if let Some(cookie) = mint_cookie {
        resp.headers_mut().append(header::SET_COOKIE, cookie);
    }
    resp
}

struct TunneledHttpBody {
    rx: mpsc::Receiver<Result<Bytes, String>>,
    session: Arc<TakeoverSession>,
    tunnel_tx: mpsc::Sender<OutboundTunnelMsg>,
    req_id: u64,
    finished: bool,
}

impl Stream for TunneledHttpBody {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(Ok(chunk))) => Poll::Ready(Some(Ok(chunk))),
            Poll::Ready(Some(Err(err))) => {
                self.finished = true;
                Poll::Ready(Some(Err(std::io::Error::other(err))))
            }
            Poll::Ready(None) => {
                self.finished = true;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for TunneledHttpBody {
    fn drop(&mut self) {
        if !self.finished {
            try_send_control(
                &self.tunnel_tx,
                &ControlFrame::HttpCancel { id: self.req_id },
            );
            let (session, req_id) = (Arc::clone(&self.session), self.req_id);
            tokio::spawn(async move {
                session.inflight_http.lock().await.remove(&req_id);
            });
        }
    }
}
