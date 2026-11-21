//! Dispatch guest frames to their active viewer streams.

use super::*;

pub(super) async fn handle_guest_control_frame(entry: &TakeoverSession, frame: ControlFrame) {
    match frame {
        ControlFrame::HttpResponseHead {
            id,
            status,
            headers,
            done,
        } => {
            let mut http = entry.inflight_http.lock().await;
            let is_sse = headers.iter().any(|(k, v)| {
                k.eq_ignore_ascii_case("content-type")
                    && v.to_ascii_lowercase().starts_with("text/event-stream")
            });
            if let Some(p) = http.get_mut(&id) {
                p.is_sse = is_sse;
                if let Some(tx) = p.head_tx.take() {
                    let _ = tx.send(HttpHeadPayload {
                        status,
                        headers,
                        done,
                    });
                }
            }
            if done {
                http.remove(&id);
            }
        }
        ControlFrame::HttpResponseEnd { id, error } => {
            if let Some(p) = entry.inflight_http.lock().await.remove(&id)
                && let Some(err) = error
            {
                let _ = p.body_tx.try_send(Err(err));
            }
        }
        ControlFrame::WsOpened {
            id,
            ok,
            protocol,
            error,
        } => {
            let mut ws = entry.inflight_ws.lock().await;
            if let Some(p) = ws.get_mut(&id)
                && let Some(tx) = p.opened_tx.take()
            {
                let _ = tx.send(WsOpenedPayload {
                    ok,
                    protocol,
                    error,
                });
            }
            if !ok {
                ws.remove(&id);
            }
        }
        ControlFrame::WsText { id, text } => {
            let tx = entry
                .inflight_ws
                .lock()
                .await
                .get(&id)
                .map(|p| p.viewer_tx.clone());
            if let Some(tx) = tx {
                let _ = tx.send(ViewerWsMsg::Text(text)).await;
            }
        }
        ControlFrame::WsClose { id, code, reason } => {
            if let Some(p) = entry.inflight_ws.lock().await.remove(&id) {
                let cf = code.map(|c| CloseFrame {
                    code: c,
                    reason: Utf8Bytes::from(reason.unwrap_or_default()),
                });
                let _ = p.viewer_tx.send(ViewerWsMsg::Close(cf)).await;
            }
        }
        _ => {}
    }
}

pub(super) async fn handle_guest_binary_frame(entry: &TakeoverSession, raw: &[u8]) {
    let Some((tag, id, payload)) = decode_binary_frame(raw) else {
        return;
    };
    if tag == BIN_TAG_WS_DATA {
        let tx = entry
            .inflight_ws
            .lock()
            .await
            .get(&id)
            .map(|p| p.viewer_tx.clone());
        if let Some(tx) = tx {
            let _ = tx.send(ViewerWsMsg::Binary(payload)).await;
        }
    } else if tag == BIN_TAG_HTTP_CHUNK {
        let (tx, exceeded) = {
            let mut http = entry.inflight_http.lock().await;
            let Some(p) = http.get_mut(&id) else {
                return;
            };
            if !p.is_sse && {
                p.bytes_received = p.bytes_received.saturating_add(payload.len());
                p.bytes_received > MAX_HTTP_RESP_BYTES
            } {
                (http.remove(&id).map(|p| p.body_tx), true)
            } else {
                (Some(p.body_tx.clone()), false)
            }
        };
        if let Some(tx) = tx {
            if exceeded {
                let _ = tx.try_send(Err("upstream response exceeded 16 MiB limit".into()));
            } else {
                let _ = tx.send(Ok(payload)).await;
            }
        }
    }
}
