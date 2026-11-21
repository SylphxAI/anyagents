use super::*;
use anyagents_core::stateless::SandboxBinding;

const TOKEN: &str = "viewer-test-token";
const TEST_LIMIT: Duration = Duration::from_secs(4);

async fn upsert(store: &Arc<Store>, binding: &SandboxBinding) {
    let (store, binding) = (store.clone(), binding.clone());
    tokio::task::spawn_blocking(move || store.sandbox_binding_store().upsert(&binding).unwrap())
        .await
        .unwrap();
}

async fn durable_session() -> (TestFixture, SandboxBinding, String) {
    let dir = tempfile::tempdir().unwrap();
    let sid = format!(
        "restart-{}",
        dir.path()
            .file_name()
            .unwrap()
            .to_string_lossy()
            .trim_start_matches('.')
    );
    let mut fx =
        start_fixture_with_store(dir, DEFAULT_RECONNECT_GRACE, KEEPALIVE_INTERVAL, |dir| {
            match std::env::var("ANYAGENTS_TEST_POSTGRES_URL") {
                Ok(url) => Store::open_postgres_isolated(
                    &url,
                    dir,
                    dir.file_name().unwrap().to_string_lossy().into_owned(),
                )
                .unwrap(),
                Err(_) => Store::open(dir.join("state.db")).unwrap(),
            }
        })
        .await;
    let mut binding = SandboxBinding::new(fx.agent_a.clone(), "remote");
    binding.lease_id = Some("lease-1".into());
    binding.lease_state = "ready".into();
    upsert(&fx.store, &binding).await;
    let guest = register_guest(&fx, &sid, TOKEN).await;
    // Stop the original listener and discard all process-local session state.
    // The same Store keeps both lease custody and the authenticated guest identity.
    fx.server_task.abort();
    drop(guest);
    let fresh = Arc::new(TakeoverRegistry::new());
    assert!(fresh.get_active(&sid).await.is_none());
    install_test_registry(&fx.store, fresh);
    (fx.addr, fx.server_task, fx.stopping) = serve(&fx.store).await;
    (fx, binding, sid)
}

#[tokio::test]
async fn takeover_restart_holds_navigation_and_cookie_websocket_until_registration() {
    let (fx, _, sid) = durable_session().await;
    let path = format!("/takeover/{sid}/?token={TOKEN}");
    let cookie = format!("{COOKIE_PREFIX}{sid}={TOKEN}");
    let mut upgrade = format!("ws://{}/takeover/{sid}/ws", fx.addr)
        .into_client_request()
        .unwrap();
    upgrade
        .headers_mut()
        .insert(header::COOKIE, cookie.parse().unwrap());
    let mut nav = Box::pin(http_call(
        fx.addr,
        "GET",
        &path,
        &[("accept", "text/html")],
        "",
    ));
    let mut ws = Box::pin(tokio_tungstenite::connect_async(upgrade));
    tokio::select! {
        r = &mut nav => panic!("navigation finished before registration: {}", r.0),
        r = &mut ws => panic!("upgrade finished before registration: {:?}", r.err()),
        _ = tokio::time::sleep(Duration::from_millis(300)) => {}
    }
    let guest = async {
        let mut guest = register_guest_mode(&fx, &sid, TOKEN, true).await;
        for _ in 0..2 {
            match next_guest_control(&mut guest).await {
                ControlFrame::HttpRequest { id, path, .. } => {
                    assert_eq!(path, "/");
                    guest_response(&mut guest, id, "text/html", b"<p>Desktop ready</p>").await;
                }
                ControlFrame::WsOpen { id, path, .. } => {
                    assert_eq!(path, "/ws");
                    let opened = ControlFrame::WsOpened {
                        id,
                        ok: true,
                        protocol: None,
                        error: None,
                    };
                    guest
                        .send(ClientWsMsg::Text(
                            serde_json::to_string(&opened).unwrap().into(),
                        ))
                        .await
                        .unwrap();
                    guest
                        .send(ClientWsMsg::Binary(encode_binary_frame(
                            BIN_TAG_WS_DATA,
                            id,
                            b"rfb-frame",
                        )))
                        .await
                        .unwrap();
                }
                _ => panic!("unexpected frame"),
            }
        }
        guest
    };
    tokio::time::timeout(TEST_LIMIT, async {
        let ((status, headers, body), ws, _guest) = tokio::join!(&mut nav, &mut ws, guest);
        assert_eq!(status, 200);
        assert!(
            headers[header::SET_COOKIE]
                .to_str()
                .unwrap()
                .contains("HttpOnly")
        );
        assert_eq!(body, b"<p>Desktop ready</p>");
        let (mut ws, response) = ws.unwrap();
        assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
        assert_eq!(
            ws.next().await.unwrap().unwrap().into_data(),
            &b"rfb-frame"[..]
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn takeover_restart_timeout_negotiates_retry_without_cache() {
    let (fx, _, sid) = durable_session().await;
    let path = format!("/takeover/{sid}/?token={TOKEN}");
    let start = Instant::now();
    let ((api, ah, ab), (html, hh, hb)) =
        tokio::time::timeout(RECONNECT_WAIT_CAP + TEST_LIMIT, async {
            tokio::join!(
                http_call(fx.addr, "GET", &path, &[("accept", "application/json")], ""),
                http_call(fx.addr, "GET", &path, &[("accept", "text/html")], ""),
            )
        })
        .await
        .unwrap();
    assert!(start.elapsed() >= RECONNECT_WAIT_CAP);
    assert_eq!((api, html), (503, 503));
    for headers in [&ah, &hh] {
        assert_eq!(headers[header::RETRY_AFTER], "1");
        assert_eq!(headers[header::CACHE_CONTROL], "no-store");
    }
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&ab).unwrap(),
        serde_json::json!({"error":"takeover reconnecting","retry_after_secs":1})
    );
    assert!(
        hh[header::CONTENT_TYPE]
            .to_str()
            .unwrap()
            .starts_with("text/html")
    );
    let html = String::from_utf8(hb).unwrap();
    assert!(html.contains("Reconnecting to desktop…"));
    assert!(html.contains("/assets/takeover-retry.js"));
}

#[tokio::test]
async fn takeover_restart_rejects_unknown_invalid_expired_and_ended_without_wait() {
    let (fx, mut binding, sid) = durable_session().await;
    let addr = fx.addr;
    async fn status(addr: std::net::SocketAddr, path: &str) -> u16 {
        let (code, headers, _) = tokio::time::timeout(
            Duration::from_secs(1),
            http_call(addr, "GET", path, &[], ""),
        )
        .await
        .expect("rejected session entered the reconnect wait");
        assert!(!headers.contains_key(header::RETRY_AFTER));
        code
    }
    assert_eq!(
        status(addr, "/takeover/unknown/?token=viewer-test-token").await,
        404
    );
    assert_eq!(
        status(addr, &format!("/takeover/{sid}/?token=invalid-token")).await,
        404
    );
    let path = format!("/takeover/{sid}/?token={TOKEN}");
    binding.lease_expires_at_ms = Some(anyagents_core::now_ms() - 1);
    upsert(&fx.store, &binding).await;
    assert_eq!(status(addr, &path).await, 410);
    binding.lease_expires_at_ms = None;
    binding.lease_state = "provisioning".into();
    upsert(&fx.store, &binding).await;
    assert_eq!(status(addr, &path).await, 404);
    binding.lease_state = "ready".into();
    upsert(&fx.store, &binding).await;
    let (store, agent) = (fx.store.clone(), fx.agent_a.clone());
    tokio::task::spawn_blocking(move || {
        crate::sandbox_exec::takeover_pin::unregister(&store, &agent, &sid).unwrap();
    })
    .await
    .unwrap();
    assert_eq!(status(addr, &path).await, 404);
}
