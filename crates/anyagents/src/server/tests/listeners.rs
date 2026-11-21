use super::super::bootstrap::{
    binds_loopback, local_listen_addr, local_listen_addr_with_port, run_with_identity, stopped,
};
use super::super::*;
use super::helpers::{
    SingleUidEnvironment, archetype_validation_state, env_lock, listed_tools, post_mcp_as,
    raw_http, single_test_worker, tools_list,
};
use crate::vendor::HarnessHost;
use anyagents_core::Error;
use axum::http::StatusCode;
use std::io::{Read, Write};

#[tokio::test]
async fn every_listener_stops_taking_calls_when_the_server_is_told_to_stop() {
    let (stopping, _) = tokio::sync::watch::channel(false);
    let socket = stopped(&stopping);
    let public = stopped(&stopping);
    tokio::pin!(socket, public);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), &mut socket)
            .await
            .is_err(),
        "stopped before the stop"
    );
    let _ = stopping.send(true);
    tokio::time::timeout(std::time::Duration::from_secs(1), socket)
        .await
        .expect("the socket listener kept taking calls");
    tokio::time::timeout(std::time::Duration::from_secs(1), public)
        .await
        .expect("a TCP listener kept taking calls");
    // A listener started after the stop stops at once; so does one whose
    // server is gone.
    tokio::time::timeout(std::time::Duration::from_secs(1), stopped(&stopping))
        .await
        .unwrap();
    let (gone, _) = tokio::sync::watch::channel(false);
    let orphan = stopped(&gone);
    drop(gone);
    tokio::time::timeout(std::time::Duration::from_secs(1), orphan)
        .await
        .unwrap();
}

/// A write arriving on either TCP listener (public or local webhook)
/// counts toward the stop until it is done, as on the socket.
#[test]
fn writes_on_the_tcp_listeners_hold_the_stop() {
    let _env = env_lock();
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
    let state = AppState {
        store,
        health: HealthBoard::default(),
        sign_in: Arc::new(Mutex::new(ProcessRunner)),
        auth: auth::Mode::Trusted {
            data_dir: dir.path().into(),
        },
        identity: Arc::new(ServingIdentity::capture()),
        profiles: Default::default(),
        timer_backend: Arc::new(crate::timer::NoopTimerBackend),
    };
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        use tokio::io::AsyncWriteExt;
        for (name, router) in [
            ("public", build_public_router(state.clone())),
            ("webhook", build_webhook_router(state.clone())),
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let served = tokio::spawn(async move {
                let _ = axum::serve(listener, router).await;
            });
            // A webhook whose body is still arriving: the call is running.
            let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            stream
                .write_all(
                    b"POST /v1/webhooks/telegram/some-channel HTTP/1.1\r\nhost: x\r\n\
                      content-type: application/json\r\ncontent-length: 100\r\n\r\n{",
                )
                .await
                .unwrap();
            let until = std::time::Instant::now() + std::time::Duration::from_secs(5);
            while in_flight::REQUESTS.running() == 0 && std::time::Instant::now() < until {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            assert!(
                in_flight::REQUESTS.running() > 0,
                "a write on the {name} listener does not hold the stop"
            );
            drop(stream);
            served.abort();
            tokio::time::timeout(
                std::time::Duration::from_secs(5),
                in_flight::REQUESTS.wait_until_idle(),
            )
            .await
            .unwrap();
        }
    });
}

#[test]
fn readiness_answers_while_the_once_only_image_hash_is_blocked() {
    if single_test_worker(
        module_path!(),
        "readiness_answers_while_the_once_only_image_hash_is_blocked",
    ) {
        return;
    }
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};
    let _env = env_lock();
    let _single_uid = SingleUidEnvironment::new();
    let data = tempfile::tempdir().unwrap();
    let token = data.path().join("operator-token");
    std::fs::write(&token, "an-operator-token-0123456789abcdef\n").unwrap();
    let settings = [
        (
            "ANYAGENTS_OPERATOR_TOKEN_FILE",
            token.to_string_lossy().into_owned(),
        ),
        ("ANYAGENTS_FAKE_MODE", "1".into()),
    ];
    struct Restore(Vec<(&'static str, Option<std::ffi::OsString>)>);
    impl Drop for Restore {
        fn drop(&mut self) {
            for (key, value) in &self.0 {
                // SAFETY: this fixture holds env_lock until its server stops.
                unsafe {
                    if let Some(value) = value {
                        std::env::set_var(key, value);
                    } else {
                        std::env::remove_var(key);
                    }
                }
            }
        }
    }
    let _restore = Restore(
        settings
            .iter()
            .map(|(key, _)| (*key, std::env::var_os(key)))
            .collect(),
    );
    for (key, value) in settings {
        // SAFETY: every server fixture uses env_lock.
        unsafe {
            std::env::set_var(key, value);
        }
    }
    let image = data.path().join("image");
    std::fs::write(&image, b"abc").unwrap();
    let (release, blocked) = std::sync::mpsc::channel();
    let (entered, running) = std::sync::mpsc::channel();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let identity = Arc::new(ServingIdentity::with_hasher(move || {
        use sha2::{Digest, Sha256};
        observed.fetch_add(1, Ordering::SeqCst);
        entered.send(()).unwrap();
        blocked.recv().unwrap();
        Ok(format!("{:x}", Sha256::digest(std::fs::read(image)?)))
    }));
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let path = data.path().to_path_buf();
    let serving = Arc::clone(&identity);
    let server = std::thread::spawn(move || {
        run_with_identity(
            &path,
            Arc::new(Mutex::new(ProcessRunner)),
            stopped,
            serving,
            HarnessHost::InProcess,
        )
    });
    struct Stop(
        Option<tokio::sync::oneshot::Sender<()>>,
        Option<std::thread::JoinHandle<Result<(), Error>>>,
    );
    impl Drop for Stop {
        fn drop(&mut self) {
            let _ = self.0.take().unwrap().send(());
            self.1.take().unwrap().join().unwrap().unwrap();
        }
    }
    let _server = Stop(Some(stop), Some(server));
    let deadline = Instant::now() + Duration::from_secs(5);
    let socket = socket_path(data.path());
    loop {
        if std::os::unix::net::UnixStream::connect(&socket).is_ok() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "readiness waited for the image hash"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    running.recv_timeout(Duration::from_secs(5)).unwrap();
    let health = || {
        let mut stream = std::os::unix::net::UnixStream::connect(&socket).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        write!(
            stream,
            "GET /v1/webhooks/healthz HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n"
        )
        .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        serde_json::from_str::<serde_json::Value>(response.split_once("\r\n\r\n").unwrap().1)
            .unwrap()
    };
    let computing = health();
    assert_eq!(computing["binary_sha256_state"], "computing");
    assert!(computing["binary_sha256"].is_null());
    assert_eq!(computing["version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(computing["source_sha"], env!("ANYAGENTS_SOURCE_SHA"));
    identity.start_hash();
    identity.clone().start_hash();
    release.send(()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let ready = health();
        if ready["binary_sha256_state"] == "ready" {
            // SHA-256 of the exact bytes written to the fixture image.
            assert_eq!(
                ready["binary_sha256"],
                "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
            );
            break;
        }
        assert!(Instant::now() < deadline, "{ready}");
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[test]
fn the_public_listener_answers_readiness_only_and_the_socket_keeps_the_identity() {
    let _env = env_lock();
    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    let socket_identity = state.identity.status();
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let public = build_webhook_router(state.clone());
        let served = tokio::spawn(async move {
            let _ = axum::serve(listener, public).await;
        });
        for path in ["/", "/healthz", "/v1/webhooks/healthz"] {
            let raw = tokio::task::spawn_blocking(move || {
                use std::io::{Read as _, Write as _};
                let mut stream = std::net::TcpStream::connect(addr).unwrap();
                write!(
                    stream,
                    "GET {path} HTTP/1.1\r\nHost: public\r\nConnection: close\r\n\r\n"
                )
                .unwrap();
                let mut raw = String::new();
                stream.read_to_string(&mut raw).unwrap();
                raw
            })
            .await
            .unwrap();
            assert!(raw.starts_with("HTTP/1.1 200"), "{path}: {raw}");
            assert!(
                raw.to_ascii_lowercase().contains("cache-control: no-store"),
                "{path}: {raw}"
            );
            let body: serde_json::Value =
                serde_json::from_str(raw.split_once("\r\n\r\n").unwrap().1).unwrap();
            assert_eq!(body, serde_json::json!({ "ok": true }), "{path}");
        }
        served.abort();
    });
    // The operator's socket still serves the full serving identity.
    for field in [
        "version",
        "source_sha",
        "binary_sha256",
        "binary_sha256_state",
    ] {
        assert!(socket_identity.get(field).is_some(), "{field}");
    }
}

#[test]
fn the_public_listener_serves_only_the_public_surface_and_remote_tokens() {
    use anyagents_core::CredentialStore as _;
    let _env = env_lock();
    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    std::fs::write(
        dir.path().join(crate::remote::CONFIG_FILE),
        "[server]\npublic_url = \"https://agents.example.com\"\npublic_listen = \"127.0.0.1:0\"\n",
    )
    .unwrap();
    let cloud = state
        .store
        .create_agent(anyagents_core::NewAgent::new(
            "cloud-public",
            "claude-cloud",
        ))
        .unwrap();
    let local = state
        .store
        .create_agent(anyagents_core::NewAgent::new("local-public", "claude-code"))
        .unwrap();
    let local_cap = state.store.issue_capability(&local.id).unwrap();
    crate::remote::begin_session(&state.store, &cloud).unwrap();
    let remote =
        crate::remote::session_token(&anyagents_core::agent_config_dir(dir.path(), &cloud.id))
            .unwrap();
    // A Telegram channel, so the webhook route reaches its secret check.
    let channel = state
        .store
        .create_channel(anyagents_core::NewChannel {
            kind: "telegram".into(),
            identity: None,
            config: "{}".into(),
            credential_ref: None,
        })
        .unwrap();
    let cred_ref = format!("channels/{}", channel.id);
    anyagents_core::FileCredentialStore::new(dir.path())
        .write(&cred_ref, b"777:fake-token-not-real")
        .unwrap();
    state
        .store
        .set_credential_ref(&channel.id, &cred_ref)
        .unwrap();
    state.store.attach_channel(&channel.id, &local.id).unwrap();
    let telegram_path = format!("/v1/webhooks/telegram/{}", channel.id);

    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let public = build_public_router(state.clone());
        let served = tokio::spawn(async move {
            let _ = axum::serve(listener, public).await;
        });
        let call = move |method: &'static str,
                         path: String,
                         headers: Vec<(&'static str, String)>,
                         body: String| {
            tokio::task::spawn_blocking(move || {
                let headers: Vec<(&str, &str)> =
                    headers.iter().map(|(k, v)| (*k, v.as_str())).collect();
                raw_http(addr, method, &path, &headers, &body)
            })
        };
        let list = tools_list().to_string();

        for path in ["/", "/healthz", "/v1/webhooks/healthz"] {
            let (status, body) = call("GET", path.into(), vec![], String::new())
                .await
                .unwrap();
            assert_eq!(status, 200, "{path}");
            assert_eq!(body, serde_json::json!({ "ok": true }), "{path}");
        }

        // Nothing of the local API, nor the MCP aliases, is reachable here,
        // whatever credential is presented.
        for (method, path) in [
            ("GET", "/v1/agents"),
            ("GET", "/v1/whoami"),
            ("POST", "/v1/send"),
            ("POST", "/v1/browser"),
            ("POST", "/v1/operator/agents/local-public/capability"),
            (
                "POST",
                "/v1/operator/agents/cloud-public/remote-sessions/revoke",
            ),
            ("POST", "/v1/mcp"),
            ("POST", "/mcp"),
            ("GET", "/v1/webhooks/unknown"),
        ] {
            // No body: a 404 closes without reading one, and unread bytes
            // would turn that close into a reset on some platforms.
            let (status, _) = call(
                method,
                path.into(),
                vec![("x-anyagents-agent", local_cap.clone())],
                String::new(),
            )
            .await
            .unwrap();
            assert_eq!(status, 404, "{method} {path}");
        }

        // MCP takes remote session tokens only. A local capability is
        // refused in either header, and no forwarding header changes that.
        for headers in [
            vec![("authorization", format!("Bearer {local_cap}"))],
            vec![("x-anyagents-agent", local_cap.clone())],
            vec![
                ("authorization", format!("Bearer {local_cap}")),
                ("x-forwarded-for", "127.0.0.1".to_string()),
                ("forwarded", "for=127.0.0.1".to_string()),
            ],
        ] {
            let (status, body) = call("POST", "/v1/webhooks/mcp".into(), headers, list.clone())
                .await
                .unwrap();
            assert_eq!(status, 401, "{body}");
            assert!(
                body["error"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("only remote session tokens"),
                "{body}"
            );
        }
        let (status, body) = call(
            "GET",
            "/v1/webhooks/mcp".into(),
            vec![("authorization", format!("Bearer {local_cap}"))],
            String::new(),
        )
        .await
        .unwrap();
        assert_eq!(status, 401, "{body}");

        let (status, body) = call(
            "POST",
            "/v1/webhooks/mcp".into(),
            vec![("authorization", format!("Bearer {remote}"))],
            list.clone(),
        )
        .await
        .unwrap();
        assert_eq!(status, 200, "{body}");
        assert_eq!(listed_tools(&body), crate::mcp::UNIVERSAL_MCP_TOOLS);

        // Telegram's webhook is verified by the channel's secret token here
        // too: a missing or wrong secret is refused before any delivery.
        let update = serde_json::json!({ "update_id": 1 }).to_string();
        for headers in [
            vec![],
            vec![("x-telegram-bot-api-secret-token", "wrong".to_string())],
        ] {
            let (status, _) = call("POST", telegram_path.clone(), headers, update.clone())
                .await
                .unwrap();
            assert_eq!(status, 401);
        }
        assert!(state.store.inbox(&local.id, None).unwrap().is_empty());
        served.abort();
    });

    // The local listener refuses a local capability too (#272), with 403
    // there, and serves the remote token.
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let refused = post_mcp_as(&state, &local_cap, None, tools_list())
            .await
            .unwrap_err();
        assert_eq!(refused.status, StatusCode::FORBIDDEN);
        let (status, _) = post_mcp_as(&state, &remote, None, tools_list())
            .await
            .unwrap();
        assert_eq!(status, StatusCode::OK);
    });
}

#[test]
fn the_local_listener_keeps_its_default_until_a_public_listener_exists() {
    // Unset public listener: today's behaviour.
    assert_eq!(
        local_listen_addr(None, false, false).as_deref(),
        Some("0.0.0.0:8788")
    );
    // A configured public listener moves the local default to loopback.
    assert_eq!(
        local_listen_addr(None, false, true).as_deref(),
        Some("127.0.0.1:8788")
    );
    // An explicit address or `off` always wins; test doubles bind nothing.
    assert_eq!(
        local_listen_addr(Some(" 127.0.0.1:9000 "), false, true).as_deref(),
        Some("127.0.0.1:9000")
    );
    assert_eq!(local_listen_addr(Some("off"), false, false), None);
    assert_eq!(local_listen_addr(None, true, false), None);
    assert!(binds_loopback("127.0.0.1:8788"));
    assert!(binds_loopback("[::1]:8788"));
    assert!(binds_loopback("localhost:8788"));
    assert!(!binds_loopback("0.0.0.0:8788"));
    assert!(!binds_loopback("10.0.0.5:8788"));
}

#[test]
fn serverless_listen_addr_honors_port_precedence_and_binds_all_interfaces() {
    // Explicit `ANYAGENTS_WEBHOOK_LISTEN` always takes priority.
    assert_eq!(
        local_listen_addr_with_port(
            Some("127.0.0.1:9999"),
            Some(9090),
            Some(8081),
            Some(8080),
            true,
            false,
            true,
        )
        .as_deref(),
        Some("127.0.0.1:9999")
    );
    // CLI `--port` beats `ANYAGENTS_PORT` and `PORT`.
    assert_eq!(
        local_listen_addr_with_port(
            None,
            Some(9090),
            Some(8081),
            Some(8080),
            false,
            false,
            false
        )
        .as_deref(),
        Some("0.0.0.0:9090")
    );
    // `ANYAGENTS_PORT` beats `PORT`.
    assert_eq!(
        local_listen_addr_with_port(None, None, Some(8081), Some(8080), false, false, false)
            .as_deref(),
        Some("0.0.0.0:8081")
    );
    // Standard `PORT` fallback binds `0.0.0.0:${PORT}` even if `public_configured`.
    assert_eq!(
        local_listen_addr_with_port(None, None, None, Some(8080), false, false, true).as_deref(),
        Some("0.0.0.0:8080")
    );
    // `ANYAGENTS_SERVERLESS=true` binds `0.0.0.0:${PORT}` even when `public_configured`.
    assert_eq!(
        local_listen_addr_with_port(None, Some(8080), None, None, true, false, true).as_deref(),
        Some("0.0.0.0:8080")
    );
    assert_eq!(
        local_listen_addr_with_port(None, None, None, None, true, false, true).as_deref(),
        Some("0.0.0.0:8788")
    );
}
