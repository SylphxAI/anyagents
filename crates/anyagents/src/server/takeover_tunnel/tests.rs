use super::*;
use anyagents_core::{ExecutionRuntime, NewAgent};
use anyagents_installer::{CommandRunner, InstallError, LineSink};
use anyagents_supervisor::HealthBoard;
use tokio_tungstenite::tungstenite::Message as ClientWsMsg;
use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;

struct NoopRunner;
impl CommandRunner for NoopRunner {
    fn run(
        &mut self,
        _: &str,
        _: &[String],
        _: &std::path::Path,
        _: &[(&str, &str)],
        _: &mut dyn LineSink,
    ) -> Result<(), InstallError> {
        Ok(())
    }
}

struct TestFixture {
    _dir: tempfile::TempDir,
    addr: std::net::SocketAddr,
    store: Arc<Store>,
    agent_a: anyagents_core::AgentId,
    agent_a_token: String,
    agent_b_token: String,
    server_task: tokio::task::JoinHandle<()>,
    stopping: tokio::sync::watch::Sender<bool>,
}

impl Drop for TestFixture {
    fn drop(&mut self) {
        self.server_task.abort();
    }
}

async fn start_fixture(grace: Duration, keepalive: Duration) -> TestFixture {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
    let a = store
        .create_agent(
            NewAgent::new("agent-a", "claude-code")
                .with_runtime(ExecutionRuntime::Sandbox { provider: None }),
        )
        .unwrap();
    let b = store
        .create_agent(
            NewAgent::new("agent-b", "claude-code")
                .with_runtime(ExecutionRuntime::Sandbox { provider: None }),
        )
        .unwrap();
    let tok_a = store.begin_remote_session(&a.id, 60_000).unwrap().token;
    let tok_b = store.begin_remote_session(&b.id, 60_000).unwrap().token;
    let reg = Arc::new(TakeoverRegistry::with_tunables(grace, keepalive));
    install_test_registry(&store, reg);

    let state = AppState {
        store: store.clone(),
        health: HealthBoard::default(),
        sign_in: Arc::new(StdMutex::new(NoopRunner)),
        auth: crate::auth::Mode::Confined {
            operator_digest: Some(crate::auth::operator_digest("op-test-secret")),
        },
        identity: Arc::new(crate::identity::ServingIdentity::capture()),
        profiles: Default::default(),
        timer_backend: Arc::new(crate::timer::NoopTimerBackend),
    };
    use crate::server::{bootstrap, in_flight};
    let (stopping, _) = tokio::sync::watch::channel(false);
    let app = in_flight::listener(crate::server::build_public_router(state), &stopping);
    let stop = bootstrap::stopped(&stopping);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_task = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(stop)
            .await
            .unwrap();
    });
    TestFixture {
        _dir: dir,
        addr,
        store,
        agent_a: a.id,
        agent_a_token: tok_a,
        agent_b_token: tok_b,
        server_task,
        stopping,
    }
}

async fn dial_guest(
    addr: std::net::SocketAddr,
    session_id: &str,
    agent_token: &str,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    let url = format!("ws://{addr}/v1/takeover/{session_id}/tunnel");
    let mut req = url.into_client_request().unwrap();
    req.headers_mut().insert(
        crate::operator::AGENT_HEADER,
        HeaderValue::from_str(agent_token).unwrap(),
    );
    let (ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    ws
}

async fn http_call(
    addr: std::net::SocketAddr,
    method: &str,
    path_and_query: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> (u16, HeaderMap, Vec<u8>) {
    use hyper_util::rt::TokioIo;
    let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let mut builder = hyper::Request::builder()
        .method(method)
        .uri(path_and_query)
        .header("host", addr.to_string());
    for (k, v) in headers {
        builder = builder.header(*k, *v);
    }
    let req = builder
        .body(http_body_util::Full::new(Bytes::from(
            body.as_bytes().to_vec(),
        )))
        .unwrap();
    let resp = sender.send_request(req).await.unwrap();
    let status = resp.status().as_u16();
    let resp_headers = resp.headers().clone();
    let bytes = resp
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes()
        .to_vec();
    (status, resp_headers, bytes)
}

fn spawn_mock_guest_pump(
    ws: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    generation_tag: &'static str,
) -> (oneshot::Sender<bool>, tokio::task::JoinHandle<()>) {
    let (stop_tx, mut stop_rx) = oneshot::channel::<bool>();
    let handle = tokio::spawn(async move {
        let (mut tx, mut rx) = ws.split();
        loop {
            tokio::select! {
                stop_clean = &mut stop_rx => {
                    if stop_clean.unwrap_or(false) {
                        let txt = serde_json::to_string(&ControlFrame::Unregister).unwrap();
                        let _ = tx.send(ClientWsMsg::Text(txt.into())).await;
                    }
                    let _ = tx.close().await;
                    break;
                }
                msg = rx.next() => match msg {
                    Some(Ok(ClientWsMsg::Text(txt))) => {
                        let Ok(frame) = serde_json::from_str::<ControlFrame>(&txt) else { continue; };
                        match frame {
                            ControlFrame::HttpRequest { id, path, .. } => {
                                let body = format!("ok:{generation_tag}:{path}");
                                let head = ControlFrame::HttpResponseHead {
                                    id,
                                    status: 200,
                                    headers: vec![("content-type".into(), "text/html; charset=utf-8".into())],
                                    done: false,
                                };
                                tx.send(ClientWsMsg::Text(serde_json::to_string(&head).unwrap().into())).await.unwrap();
                                let chunk = encode_binary_frame(BIN_TAG_HTTP_CHUNK, id, body.as_bytes());
                                tx.send(ClientWsMsg::Binary(chunk)).await.unwrap();
                                let end = ControlFrame::HttpResponseEnd { id, error: None };
                                tx.send(ClientWsMsg::Text(serde_json::to_string(&end).unwrap().into())).await.unwrap();
                            }
                            ControlFrame::WsOpen { id, protocols, .. } => {
                                let chosen = protocols.into_iter().find(|p| p == "binary");
                                let opened = ControlFrame::WsOpened { id, ok: true, protocol: chosen, error: None };
                                tx.send(ClientWsMsg::Text(serde_json::to_string(&opened).unwrap().into())).await.unwrap();
                                let banner = encode_binary_frame(BIN_TAG_WS_DATA, id, b"RFB 003.008\n");
                                tx.send(ClientWsMsg::Binary(banner)).await.unwrap();
                            }
                            ControlFrame::WsText { id, text } => {
                                let reply = ControlFrame::WsText { id, text: format!("echo:{generation_tag}:{text}") };
                                tx.send(ClientWsMsg::Text(serde_json::to_string(&reply).unwrap().into())).await.unwrap();
                            }
                            _ => {}
                        }
                    }
                    Some(Ok(ClientWsMsg::Binary(raw))) => {
                        if let Some((BIN_TAG_WS_DATA, id, payload)) = decode_binary_frame(&raw) {
                            let mut echoed = b"bin:".to_vec();
                            echoed.extend_from_slice(&payload);
                            let frame = encode_binary_frame(BIN_TAG_WS_DATA, id, &echoed);
                            tx.send(ClientWsMsg::Binary(frame)).await.unwrap();
                        }
                    }
                    Some(Ok(ClientWsMsg::Ping(p))) => { let _ = tx.send(ClientWsMsg::Pong(p)).await; }
                    Some(Ok(ClientWsMsg::Pong(_))) => {}
                    _ => break,
                }
            }
        }
    });
    (stop_tx, handle)
}

#[tokio::test]
async fn takeover_events_end_cleanly_on_shutdown_and_cancel_the_guest_subscription() {
    tokio::time::timeout(Duration::from_secs(5), async {
        use hyper_util::rt::TokioIo;
        let mut fx = start_fixture(Duration::from_secs(5), Duration::from_secs(25)).await;
        let session_id = "tk_shutdown_test";
        let token = "event-stream-fixture-token";
        let mut guest = dial_guest(fx.addr, session_id, &fx.agent_a_token).await;
        let register = ControlFrame::Register {
            token: token.into(),
            reconnect: false,
            agent_id: None,
            local_origin: None,
        };
        async fn next_control(
            guest: &mut tokio_tungstenite::WebSocketStream<
                tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
            >,
        ) -> ControlFrame {
            loop {
                if let ClientWsMsg::Text(text) = guest.next().await.unwrap().unwrap() {
                    return serde_json::from_str(&text).unwrap();
                }
            }
        }
        guest.send(ClientWsMsg::Text(serde_json::to_string(&register).unwrap().into())).await.unwrap();
        assert!(matches!(next_control(&mut guest).await, ControlFrame::Registered { .. }));

        let socket = tokio::net::TcpStream::connect(fx.addr).await.unwrap();
        let (mut client, conn) = hyper::client::conn::http1::handshake(TokioIo::new(socket)).await.unwrap();
        tokio::spawn(async move { conn.await.unwrap() });
        let request = hyper::Request::get(format!("/takeover/{session_id}/events?token={token}"))
            .header(header::HOST, fx.addr.to_string())
            .body(http_body_util::Empty::<Bytes>::new()).unwrap();
        let request = tokio::spawn(async move { client.send_request(request).await.unwrap() });
        let ControlFrame::HttpRequest { id, path, .. } = next_control(&mut guest).await else {
            panic!("expected proxy request");
        };
        assert_eq!(path, "/events");
        let head = ControlFrame::HttpResponseHead {
            id,
            status: 200,
            headers: vec![("content-type".into(), "text/event-stream; charset=utf-8".into())],
            done: false,
        };
        guest.send(ClientWsMsg::Text(serde_json::to_string(&head).unwrap().into())).await.unwrap();
        let event = b"data: ready\n\n";
        guest.send(ClientWsMsg::Binary(encode_binary_frame(BIN_TAG_HTTP_CHUNK, id, event))).await.unwrap();
        let mut response = request.await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let first = response.body_mut().frame().await.unwrap().unwrap().into_data().unwrap();
        assert_eq!(first.as_ref(), event);

        // The upstream intentionally never sends HttpResponseEnd.
        fx.stopping.send_replace(true);
        assert!(response.into_body().collect().await.unwrap().to_bytes().is_empty());
        assert!(matches!(next_control(&mut guest).await, ControlFrame::HttpCancel { id: canceled } if canceled == id));
        (&mut fx.server_task).await.unwrap();
    }).await.expect("open SSE stream prevented shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::await_holding_lock)]
async fn takeover_tunnel_auth_cookie_ws_relay_hijack_reconnect_and_teardown() {
    let _env = crate::server::tests::env_lock();
    let prev_pub = std::env::var_os("ANYAGENTS_PUBLIC_URL");
    unsafe {
        std::env::remove_var("ANYAGENTS_PUBLIC_URL");
    }
    let fx = start_fixture(Duration::from_secs(5), Duration::from_secs(25)).await;
    let session_id = "tk_e2e_session_01";
    let takeover_token = "tok_secret_takeover_987654321";

    // 1. Unknown session returns 404.
    let (st, _, _) = http_call(fx.addr, "GET", "/takeover/tk_missing/?token=x", &[], "").await;
    assert_eq!(st, 404);

    // 2. Register guest tunnel as agent A.
    let mut guest_ws = dial_guest(fx.addr, session_id, &fx.agent_a_token).await;
    let reg = serde_json::to_string(&ControlFrame::Register {
        token: takeover_token.into(),
        reconnect: false,
        agent_id: None,
        local_origin: Some("http://127.0.0.1:6080".into()),
    })
    .unwrap();
    guest_ws.send(ClientWsMsg::Text(reg.into())).await.unwrap();
    let ack_txt = match guest_ws.next().await.unwrap().unwrap() {
        ClientWsMsg::Text(t) => t,
        other => panic!("expected Registered text frame, got {other:?}"),
    };
    let ControlFrame::Registered {
        public_base_url,
        public_url,
        session_id: ack_sid,
        ttl_ms: None,
    } = serde_json::from_str(&ack_txt).unwrap()
    else {
        panic!("expected Registered frame: {ack_txt}");
    };
    assert_eq!(ack_sid, session_id);
    assert_eq!(
        public_base_url,
        format!("http://{}/takeover/{session_id}/", fx.addr)
    );
    assert_eq!(
        public_url,
        format!(
            "http://{}/takeover/{session_id}/?token={takeover_token}",
            fx.addr
        )
    );
    let (stop_g1, task_g1) = spawn_mock_guest_pump(guest_ws, "gen1");

    // 3. Missing or invalid token fails closed with 401.
    let (st, _, _) = http_call(fx.addr, "GET", &format!("/takeover/{session_id}/"), &[], "").await;
    assert_eq!(st, 401);
    let (st, _, _) = http_call(
        fx.addr,
        "GET",
        &format!("/takeover/{session_id}/?token=wrong_token"),
        &[],
        "",
    )
    .await;
    assert_eq!(st, 401);

    // 4. No trailing slash redirects 302 to `/takeover/{id}/?token=...` and mints cookie.
    let (st, hdrs, _) = http_call(
        fx.addr,
        "GET",
        &format!("/takeover/{session_id}?token={takeover_token}"),
        &[],
        "",
    )
    .await;
    assert_eq!(st, 302);
    assert_eq!(
        hdrs.get(header::LOCATION).unwrap().to_str().unwrap(),
        format!("/takeover/{session_id}/?token={takeover_token}")
    );

    // 5. Valid `?token=` fetches HTML over tunnel and mints scoped HttpOnly cookie.
    let (st, hdrs, body) = http_call(
        fx.addr,
        "GET",
        &format!("/takeover/{session_id}/?token={takeover_token}"),
        &[],
        "",
    )
    .await;
    assert_eq!(st, 200);
    assert_eq!(String::from_utf8(body).unwrap(), "ok:gen1:/");
    let set_cookie = hdrs
        .get(header::SET_COOKIE)
        .expect("Set-Cookie minted")
        .to_str()
        .unwrap()
        .to_string();
    assert!(
        set_cookie.contains(&format!("anyagents_takeover_{session_id}={takeover_token}"))
            && set_cookie.contains(&format!("Path=/takeover/{session_id}/"))
            && set_cookie.contains("HttpOnly")
            && set_cookie.contains("SameSite=Lax"),
        "{set_cookie}"
    );
    let cookie_pair = set_cookie.split(';').next().unwrap();

    // 6. Sub-resource request authenticated solely by scoped cookie (no query token) succeeds.
    let (st, _, body) = http_call(
        fx.addr,
        "GET",
        &format!("/takeover/{session_id}/app.js"),
        &[("cookie", cookie_pair)],
        "",
    )
    .await;
    assert_eq!(st, 200);
    assert_eq!(String::from_utf8(body).unwrap(), "ok:gen1:/app.js");

    // 7. Cross-origin request is rejected with 403.
    let (st, _, _) = http_call(
        fx.addr,
        "POST",
        &format!("/takeover/{session_id}/take"),
        &[("cookie", cookie_pair), ("origin", "https://evil.example")],
        "",
    )
    .await;
    assert_eq!(st, 403);

    // 8. Viewer WebSocket upgrade (cookie auth + `binary` subprotocol) relays binary & text frames.
    let mut viewer_req = format!("ws://{}/takeover/{session_id}/ws", fx.addr)
        .into_client_request()
        .unwrap();
    viewer_req
        .headers_mut()
        .insert(header::COOKIE, HeaderValue::from_str(cookie_pair).unwrap());
    viewer_req.headers_mut().insert(
        header::SEC_WEBSOCKET_PROTOCOL,
        HeaderValue::from_static("binary"),
    );
    let (mut viewer_ws, ws_resp) = tokio_tungstenite::connect_async(viewer_req).await.unwrap();
    assert_eq!(
        ws_resp
            .headers()
            .get(header::SEC_WEBSOCKET_PROTOCOL)
            .and_then(|v| v.to_str().ok()),
        Some("binary")
    );
    assert_eq!(
        viewer_ws
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_data()
            .as_ref(),
        b"RFB 003.008\n"
    );
    viewer_ws
        .send(ClientWsMsg::Binary(Bytes::from_static(b"\x04key")))
        .await
        .unwrap();
    assert_eq!(
        viewer_ws
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_data()
            .as_ref(),
        b"bin:\x04key"
    );
    viewer_ws
        .send(ClientWsMsg::Text("hello-ws".into()))
        .await
        .unwrap();
    assert_eq!(
        viewer_ws
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_text()
            .unwrap(),
        "echo:gen1:hello-ws"
    );
    let _ = viewer_ws.close(None).await;

    // 9. Hijack attempt by agent B (even knowing the takeover token) fails closed.
    let mut attacker_ws = dial_guest(fx.addr, session_id, &fx.agent_b_token).await;
    let hijack_reg = serde_json::to_string(&ControlFrame::Register {
        token: takeover_token.into(),
        reconnect: false,
        agent_id: None,
        local_origin: None,
    })
    .unwrap();
    attacker_ws
        .send(ClientWsMsg::Text(hijack_reg.into()))
        .await
        .unwrap();
    let err_txt = attacker_ws
        .next()
        .await
        .unwrap()
        .unwrap()
        .into_text()
        .unwrap();
    let ControlFrame::Error { code, .. } = serde_json::from_str(&err_txt).unwrap() else {
        panic!("expected Error frame: {err_txt}");
    };
    assert_eq!(code, "FORBIDDEN");

    // 10. Same agent A with wrong takeover token fails closed on reconnect.
    let mut wrong_tok_ws = dial_guest(fx.addr, session_id, &fx.agent_a_token).await;
    let bad_reg = serde_json::to_string(&ControlFrame::Register {
        token: "wrong_tok".into(),
        reconnect: false,
        agent_id: None,
        local_origin: None,
    })
    .unwrap();
    wrong_tok_ws
        .send(ClientWsMsg::Text(bad_reg.into()))
        .await
        .unwrap();
    let err_txt = wrong_tok_ws
        .next()
        .await
        .unwrap()
        .unwrap()
        .into_text()
        .unwrap();
    let ControlFrame::Error { code, .. } = serde_json::from_str(&err_txt).unwrap() else {
        panic!("expected Error frame: {err_txt}");
    };
    assert_eq!(code, "UNAUTHORIZED");

    // 11. Unexpected tunnel drop enters grace window; agent A re-attaches with same id + token.
    let _ = stop_g1.send(false);
    let _ = task_g1.await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut guest_ws2 = dial_guest(fx.addr, session_id, &fx.agent_a_token).await;
    let reg2 = serde_json::to_string(&ControlFrame::Register {
        token: takeover_token.into(),
        reconnect: true,
        agent_id: None,
        local_origin: None,
    })
    .unwrap();
    guest_ws2
        .send(ClientWsMsg::Text(reg2.into()))
        .await
        .unwrap();
    let ack2 = guest_ws2
        .next()
        .await
        .unwrap()
        .unwrap()
        .into_text()
        .unwrap();
    assert!(matches!(
        serde_json::from_str::<ControlFrame>(&ack2).unwrap(),
        ControlFrame::Registered { .. }
    ));
    let (stop_g2, task_g2) = spawn_mock_guest_pump(guest_ws2, "gen2");

    let (st, _, body) = http_call(
        fx.addr,
        "GET",
        &format!("/takeover/{session_id}/state"),
        &[("cookie", cookie_pair)],
        "",
    )
    .await;
    assert_eq!(st, 200);
    assert_eq!(String::from_utf8(body).unwrap(), "ok:gen2:/state");

    // 12. Explicit Unregister tears down the session immediately (404).
    let _ = stop_g2.send(true);
    let _ = task_g2.await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let (st, _, _) = http_call(
        fx.addr,
        "GET",
        &format!("/takeover/{session_id}/?token={takeover_token}"),
        &[],
        "",
    )
    .await;
    assert_eq!(st, 404);

    if let Some(v) = prev_pub {
        unsafe {
            std::env::set_var("ANYAGENTS_PUBLIC_URL", v);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::await_holding_lock)]
async fn tunnel_sends_keepalive_pings_and_evicts_after_grace_window() {
    let _env = crate::server::tests::env_lock();
    let fx = start_fixture(Duration::from_millis(250), Duration::from_millis(80)).await;
    let session_id = "tk_keepalive_grace";
    let token = "tok_keepalive_123456";

    let mut guest_ws = dial_guest(fx.addr, session_id, &fx.agent_a_token).await;
    let reg = serde_json::to_string(&ControlFrame::Register {
        token: token.into(),
        reconnect: false,
        agent_id: None,
        local_origin: None,
    })
    .unwrap();
    guest_ws.send(ClientWsMsg::Text(reg.into())).await.unwrap();
    let _ = guest_ws.next().await.unwrap().unwrap();

    let ping_seen = tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(Ok(msg)) = guest_ws.next().await {
            if matches!(msg, ClientWsMsg::Ping(_)) {
                return true;
            }
        }
        false
    })
    .await
    .unwrap();
    assert!(ping_seen, "expected keepalive Ping frame on guest tunnel");

    drop(guest_ws);
    tokio::time::sleep(Duration::from_millis(400)).await;
    let (st, _, _) = http_call(
        fx.addr,
        "GET",
        &format!("/takeover/{session_id}/?token={token}"),
        &[],
        "",
    )
    .await;
    assert_eq!(st, 404);
}

async fn register_guest(fx: &TestFixture, sid: &str, token: &str) -> ClientWs {
    let mut guest = dial_guest(fx.addr, sid, &fx.agent_a_token).await;
    let frame = ControlFrame::Register {
        token: token.into(),
        reconnect: false,
        agent_id: None,
        local_origin: None,
    };
    guest
        .send(ClientWsMsg::Text(
            serde_json::to_string(&frame).unwrap().into(),
        ))
        .await
        .unwrap();
    assert!(matches!(
        next_guest_control(&mut guest).await,
        ControlFrame::Registered { .. }
    ));
    guest
}

type ClientWs =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn next_guest_control(guest: &mut ClientWs) -> ControlFrame {
    loop {
        if let ClientWsMsg::Text(text) = guest.next().await.unwrap().unwrap() {
            return serde_json::from_str(&text).unwrap();
        }
    }
}

async fn guest_response(guest: &mut ClientWs, id: u64, media: &str, body: &[u8]) {
    let head = ControlFrame::HttpResponseHead {
        id,
        status: 200,
        headers: vec![("content-type".into(), media.into())],
        done: false,
    };
    let end = ControlFrame::HttpResponseEnd { id, error: None };
    guest
        .send(ClientWsMsg::Text(
            serde_json::to_string(&head).unwrap().into(),
        ))
        .await
        .unwrap();
    guest
        .send(ClientWsMsg::Binary(encode_binary_frame(
            BIN_TAG_HTTP_CHUNK,
            id,
            body,
        )))
        .await
        .unwrap();
    guest
        .send(ClientWsMsg::Text(
            serde_json::to_string(&end).unwrap().into(),
        ))
        .await
        .unwrap();
}

#[tokio::test]
async fn reset_negotiates_lifecycle_and_requires_operator_then_coalesces_wake() {
    tokio::time::timeout(Duration::from_secs(8), async {
        let fx = start_fixture(Duration::from_secs(5), KEEPALIVE_INTERVAL).await;
        let html = [("accept", "text/html")];
        let op = [
            (crate::operator::HEADER, "op-test-secret"),
            ("x-anyagents-admin-csrf", "1"),
        ];
        let guest = register_guest(&fx, "newer", "new-link-fixture").await;
        let hash = agent_tools::sha256_hex(b"expired-link-fixture");
        fx.store
            .remember_takeover_link(
                "past",
                &hash,
                &fx.agent_a,
                Some(anyagents_core::now_ms() - 1000),
            )
            .unwrap();
        let page = "/takeover/past/?token=expired-link-fixture";
        let reset = "/takeover/past/_reopen?token=expired-link-fixture";
        let (status, headers, body) = http_call(fx.addr, "GET", page, &html, "").await;
        assert_eq!(status, 410);
        assert_eq!(headers[header::CACHE_CONTROL], "no-store");
        assert_eq!(headers[header::VARY], "Accept, Cookie");
        let body = String::from_utf8(body).unwrap();
        assert!(body.contains("This takeover expired") && !body.contains("expired-link-fixture"));
        assert!(
            headers[header::SET_COOKIE]
                .to_str()
                .unwrap()
                .contains("Path=/takeover/past/; HttpOnly; SameSite=Lax")
        );
        for accept in ["application/json", "*/*", "application/json, text/html;q=0"] {
            let (status, headers, _) =
                http_call(fx.addr, "GET", page, &[("accept", accept)], "").await;
            assert_eq!(status, 404); // existing JSON response when there is no in-memory registry entry
            assert_eq!(headers[header::CONTENT_TYPE], "application/json");
        }
        let (status, _, body) =
            http_call(fx.addr, "GET", "/takeover/past/?token=wrong", &html, "").await;
        assert_eq!(status, 404);
        assert!(
            String::from_utf8(body)
                .unwrap()
                .contains("Takeover not found")
        );
        for (headers, expected) in [
            (&op[1..], 401),
            (&op[..1], 403),
            (
                &[
                    (crate::operator::AGENT_HEADER, fx.agent_a_token.as_str()),
                    op[1],
                ][..],
                401,
            ),
        ] {
            assert_eq!(
                http_call(fx.addr, "POST", reset, headers, "").await.0,
                expected
            );
        }
        assert_eq!(
            http_call(
                fx.addr,
                "POST",
                "/takeover/past/_reopen?token=wrong",
                &op,
                ""
            )
            .await
            .0,
            404
        );
        let (status, _, body) = http_call(fx.addr, "POST", reset, &op, "").await;
        assert_eq!(status, 200);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap()["url"],
            "/takeover/newer/?token=new-link-fixture"
        );
        assert!(
            fx.store
                .list_undelivered(&fx.agent_a, 10)
                .unwrap()
                .is_empty()
        );
        let (stop, done) = spawn_mock_guest_pump(guest, "newer");
        stop.send(true).unwrap();
        done.await.unwrap();
        // Wait for the explicit unregister to become visible to the HTTP surface.
        loop {
            if http_call(
                fx.addr,
                "GET",
                "/takeover/newer/?token=new-link-fixture",
                &html,
                "",
            )
            .await
            .0 == 410
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        for blocked in ["paused", "archived"] {
            fx.store.set_agent_state(&fx.agent_a, blocked).unwrap();
            assert_eq!(http_call(fx.addr, "POST", reset, &op, "").await.0, 409);
            assert!(
                fx.store
                    .list_undelivered(&fx.agent_a, 10)
                    .unwrap()
                    .is_empty()
            );
        }
        fx.store.set_agent_state(&fx.agent_a, "idle").unwrap();
        let (a, b) = tokio::join!(
            http_call(fx.addr, "POST", reset, &op, ""),
            http_call(fx.addr, "POST", reset, &op, "")
        );
        assert_eq!((a.0, b.0), (202, 202));
        let pending = fx.store.list_undelivered(&fx.agent_a, 10).unwrap();
        assert_eq!(pending.len(), 1);
        assert!(pending[0].text.contains("[Browser takeover reopen]"));
    })
    .await
    .expect("reset test timed out");
}

#[tokio::test]
async fn handback_waits_for_workstation_release_then_closes_and_wakes_idle_owner_once() {
    tokio::time::timeout(Duration::from_secs(8), async {
        let fx = start_fixture(Duration::from_secs(5), KEEPALIVE_INTERVAL).await;
        fx.store.set_agent_state(&fx.agent_a, "idle").unwrap();
        let mut guest = register_guest(&fx, "handback", "handback-link-fixture").await;
        let addr = fx.addr;
        let page = tokio::spawn(http_call(addr, "GET", "/takeover/handback/?token=handback-link-fixture", &[("accept", "text/html")], ""));
        let ControlFrame::HttpRequest {id, path, ..} = next_guest_control(&mut guest).await else { panic!("expected viewer root") };
        assert_eq!(path, "/");
        guest_response(&mut guest, id, "text/html", br#"<!doctype html><script type="module" src="app.js"></script>"#).await;
        let (status, headers, body) = page.await.unwrap();
        assert_eq!(status, 200);
        let cookie = headers.get_all(header::SET_COOKIE).iter().find_map(|v| v.to_str().ok()).unwrap().split(';').next().unwrap().to_owned();
        assert!(String::from_utf8(body).unwrap().contains("/assets/takeover-viewer.js"));
        let action = "/takeover/handback/_handback";
        assert_eq!(http_call(addr, "POST", action, &[("cookie", &cookie)], "").await.0, 403);
        let csrf = ("x-anyagents-takeover-action", "handback");
        assert_eq!(http_call(addr, "POST", action, &[("cookie", &cookie), csrf, ("origin", "https://other.invalid")], "").await.0, 403);
        let send = |cookie: String| async move { http_call(addr, "POST", action, &[("cookie", &cookie), csrf], "").await };
        let action_request = tokio::spawn(send(cookie.clone()));
        let ControlFrame::HttpRequest {id, method, path, body_b64, ..} = next_guest_control(&mut guest).await else { panic!("expected release") };
        assert_eq!((method.as_str(), path.as_str(), body_b64.as_str()), ("POST", "/release", ""));
        assert!(fx.store.list_undelivered(&fx.agent_a, 10).unwrap().is_empty());
        assert!(!action_request.is_finished());
        let duplicate = tokio::spawn(send(cookie.clone()));
        guest_response(&mut guest, id, "application/json", br#"{"holder":"agent","browser_running":true}"#).await;
        assert_eq!(action_request.await.unwrap().0, 200);
        assert_eq!(duplicate.await.unwrap().0, 200);
        assert!(matches!(next_guest_control(&mut guest).await, ControlFrame::Error {code, ..} if code == "ENDED"));
        assert_eq!(send(cookie.clone()).await.0, 200);
        let (status, _, body) = http_call(addr, "GET", "/takeover/handback/_returned", &[("cookie", &cookie)], "").await;
        assert_eq!(status, 200);
        assert!(String::from_utf8(body).unwrap().contains("The agent is back in control"));
        let pending = fx.store.list_undelivered(&fx.agent_a, 10).unwrap();
        assert_eq!(pending.len(), 1);
        assert!(pending[0].text.contains("[Browser handback]") && !pending[0].text.contains("handback-link-fixture"));
    }).await.expect("handback test timed out");
}
