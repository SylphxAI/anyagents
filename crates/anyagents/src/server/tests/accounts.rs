use super::super::*;
use super::helpers::{SingleUidEnvironment, env_lock, single_test_worker};
use anyagents_installer::{CommandRunner, InstallError, LineSink};
use axum::http::StatusCode;
use std::io::{Read, Write};
use std::path::PathBuf;

/// Answers one sign-in with two lines and records where it ran.
struct Scripted {
    dir: Arc<Mutex<Option<PathBuf>>>,
}

impl CommandRunner for Scripted {
    fn run(
        &mut self,
        _program: &str,
        _args: &[String],
        dir: &Path,
        env: &[(&str, &str)],
        on_line: &mut dyn LineSink,
    ) -> Result<(), InstallError> {
        *self.dir.lock().unwrap() = Some(dir.to_path_buf());
        assert!(env.iter().any(|(key, _)| *key == "CODEX_HOME"));
        on_line.line("LINK https://auth.example/device");
        on_line.line("SIGNED_IN ada@example");
        Ok(())
    }
}

/// A streaming sign-in returns its response headers before the blocking
/// vendor command finishes, so its carried `in_flight` token must keep the
/// stop waiting until the blocking work and capability rotation return.
#[test]
fn sign_in_route_carries_in_flight_token_until_blocking_work_finishes() {
    struct HeldSignIn {
        entered: std::sync::mpsc::SyncSender<()>,
        release: std::sync::mpsc::Receiver<()>,
    }
    impl CommandRunner for HeldSignIn {
        fn run(
            &mut self,
            _program: &str,
            _args: &[String],
            _dir: &Path,
            _env: &[(&str, &str)],
            on_line: &mut dyn LineSink,
        ) -> Result<(), InstallError> {
            let _ = self.entered.send(());
            let _ = self.release.recv();
            on_line.line("SIGNED_IN ada@example");
            Ok(())
        }
    }
    let _env = env_lock();
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
    let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
    let state = AppState {
        store,
        health: HealthBoard::default(),
        sign_in: Arc::new(Mutex::new(HeldSignIn {
            entered: entered_tx,
            release: release_rx,
        })),
        auth: auth::Mode::Confined {
            operator_digest: Some(auth::operator_digest("test-op-token")),
        },
        identity: Arc::new(ServingIdentity::capture()),
        profiles: Default::default(),
        timer_backend: Arc::new(crate::timer::NoopTimerBackend),
    };
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        use hyper::service::Service as _;
        let router = build_router(state);
        let service = hyper_util::service::TowerToHyperService::new(router);
        let req = hyper::Request::builder()
            .method("POST")
            .uri("http://localhost/v1/harnesses/codex/sign-in")
            .header(crate::operator::HEADER, "test-op-token")
            .body(http_body_util::Full::new(bytes::Bytes::new()))
            .unwrap();
        let response = service.call(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        tokio::task::spawn_blocking(move || {
            entered_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("blocking sign-in started");
        })
        .await
        .unwrap();
        // Even after the handler future returned its streaming response
        // (and even if the caller drops the body), the blocking task still
        // holds its carried `in_flight` token until `sign_in` finishes.
        drop(response);
        assert!(
            in_flight::REQUESTS.running() > 0,
            "streaming sign-in dropped its in_flight token before blocking work finished"
        );
        release_tx.send(()).unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            in_flight::REQUESTS.wait_until_idle(),
        )
        .await
        .unwrap();
    });
}

#[test]
fn a_sign_in_streams_the_link_before_the_outcome() {
    if single_test_worker(
        module_path!(),
        "a_sign_in_streams_the_link_before_the_outcome",
    ) {
        return;
    }
    let _env = env_lock();
    let _single_uid = SingleUidEnvironment::new();
    let data = tempfile::tempdir().unwrap();
    let socket = socket_path(data.path());
    let dir = Arc::new(Mutex::new(None));
    let runner = Arc::new(Mutex::new(Scripted {
        dir: Arc::clone(&dir),
    }));
    let data_path = data.path().to_path_buf();
    // This deployment has an operator, so the sign-in presents one.
    let token_file = data.path().join("operator-token");
    std::fs::write(&token_file, "an-operator-token-0123456789abcdef\n").unwrap();
    // SAFETY: the test process sets these before the server thread reads
    // them, and sets them for its own duration only.
    unsafe {
        std::env::set_var("ANYAGENTS_OPERATOR_TOKEN_FILE", &token_file);
        // Authentication is independent of optional multi-UID seats.
        std::env::set_var("ANYAGENTS_FAKE_MODE", "1");
    }
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let started = Arc::new(Mutex::new(None));
    let outcome = Arc::clone(&started);
    let server = std::thread::spawn(move || {
        let result = run_with(&data_path, runner, stop_rx);
        if let Err(err) = &result {
            *outcome.lock().unwrap_or_else(|e| e.into_inner()) = Some(err.to_string());
        }
        result
    });
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !socket.exists() && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    assert!(
        socket.exists(),
        "the server did not start: {:?}",
        started.lock().unwrap_or_else(|e| e.into_inner())
    );
    let mut stream = std::os::unix::net::UnixStream::connect(&socket).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();
    stream
        .write_all(
            format!(
                "POST /v1/harnesses/codex/sign-in HTTP/1.1\r\nHost: local\r\n{}: an-operator-token-0123456789abcdef\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                crate::operator::HEADER
            )
            .as_bytes(),
        )
        .unwrap();
    let mut buf = String::new();
    let mut chunk = [0u8; 256];
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while std::time::Instant::now() < deadline && !buf.contains("\"kind\":\"done\"") {
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => buf.push_str(&String::from_utf8_lossy(&chunk[..n])),
            Err(_) => break,
        }
    }
    let link = buf.find("\"kind\":\"link\"").expect(&buf);
    let done = buf.find("\"kind\":\"done\"").expect(&buf);
    assert!(link < done, "{buf}");
    assert!(buf.contains("https://auth.example/device"), "{buf}");
    assert!(buf.contains("ada@example"), "{buf}");
    let recorded = dir.lock().unwrap().clone().unwrap();
    assert!(recorded.ends_with("config/codex/default"), "{recorded:?}");
    drop(stream);
    let _ = stop_tx.send(());
    let _ = server.join();
    // SAFETY: as above, for this test only.
    unsafe {
        std::env::remove_var("ANYAGENTS_OPERATOR_TOKEN_FILE");
        std::env::remove_var("ANYAGENTS_AGENT_UID");
        std::env::remove_var("ANYAGENTS_AGENT_GID");
        std::env::remove_var("ANYAGENTS_SOCKET_GID");
        std::env::remove_var("ANYAGENTS_FAKE_MODE");
        std::env::remove_var("ANYAGENTS_LAUNCHER");
    }
}

/// A sign-in call from an agent's own capability is refused, and no vendor
/// command runs.
#[test]
fn an_agent_capability_cannot_sign_a_harness_in() {
    if single_test_worker(
        module_path!(),
        "an_agent_capability_cannot_sign_a_harness_in",
    ) {
        return;
    }
    let _env = env_lock();
    let _single_uid = SingleUidEnvironment::new();
    let data = tempfile::tempdir().unwrap();
    let socket = socket_path(data.path());
    let dir = Arc::new(Mutex::new(None));
    let runner = Arc::new(Mutex::new(Scripted {
        dir: Arc::clone(&dir),
    }));
    let data_path = data.path().to_path_buf();
    let token_file = data.path().join("operator-token");
    std::fs::write(&token_file, "an-operator-token-0123456789abcdef\n").unwrap();
    // SAFETY: the test process sets these before the server thread reads
    // them, and sets them for its own duration only.
    unsafe {
        std::env::set_var("ANYAGENTS_OPERATOR_TOKEN_FILE", &token_file);
        std::env::set_var(
            "ANYAGENTS_CAPABILITY_KEY",
            "0123456789abcdef0123456789abcdef",
        );
    }
    // SAFETY: as above, for this test's own duration under the lock.
    unsafe {
        std::env::set_var("ANYAGENTS_FAKE_MODE", "1");
    }
    let store = Store::open(data.path().join("state.db")).unwrap();
    let configured = "0123456789abcdef0123456789abcdef";
    store.hold_capability_key(configured).unwrap();
    let agent = store
        .create_agent(anyagents_core::NewAgent::new("ada", "codex"))
        .unwrap();
    drop(store);
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let started = Arc::new(Mutex::new(None));
    let outcome = Arc::clone(&started);
    let server = std::thread::spawn(move || {
        let result = run_with(&data_path, runner, stop_rx);
        if let Err(err) = &result {
            *outcome.lock().unwrap_or_else(|e| e.into_inner()) = Some(err.to_string());
        }
        result
    });
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !socket.exists() && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    assert!(
        socket.exists(),
        "the server did not start: {:?}",
        started.lock().unwrap_or_else(|e| e.into_inner())
    );
    // The server mints each agent's capability when it starts, so this test
    // reads the one it left and the server recognises.
    let capability = std::fs::read_to_string(
        data.path()
            .join("agents")
            .join(agent.id.as_str())
            .join("config")
            .join(crate::operator::CAPABILITY_FILE),
    )
    .unwrap()
    .trim()
    .to_string();
    let mut stream = std::os::unix::net::UnixStream::connect(&socket).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();
    stream
        .write_all(
            format!(
                "POST /v1/harnesses/codex/sign-in HTTP/1.1\r\nHost: local\r\n{}: {capability}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                crate::operator::AGENT_HEADER
            )
            .as_bytes(),
        )
        .unwrap();
    let mut buf = String::new();
    let _ = stream.read_to_string(&mut buf);
    assert!(buf.starts_with("HTTP/1.1 403"), "{buf}");
    assert!(buf.contains("cannot administer"), "{buf}");
    assert!(dir.lock().unwrap().is_none(), "the vendor command ran");
    drop(stream);
    let _ = stop_tx.send(());
    let _ = server.join();
    // SAFETY: as above, under `ENV`.
    unsafe {
        std::env::remove_var("ANYAGENTS_OPERATOR_TOKEN_FILE");
        std::env::remove_var("ANYAGENTS_CAPABILITY_KEY");
        std::env::remove_var("ANYAGENTS_AGENT_UID");
        std::env::remove_var("ANYAGENTS_AGENT_GID");
        std::env::remove_var("ANYAGENTS_SOCKET_GID");
        std::env::remove_var("ANYAGENTS_FAKE_MODE");
        std::env::remove_var("ANYAGENTS_LAUNCHER");
    }
}

#[test]
fn spiron_and_sylphx_share_compatible_harnesses_and_seed_auth_json_paths() {
    let dir = crate::test_tempdir();
    let data = dir.path();
    let store = Store::open(data.join("state.db")).unwrap();
    let sylphx_profile = data.join("profiles/sylphx/default/.config/sylphx");
    std::fs::create_dir_all(&sylphx_profile).unwrap();
    std::fs::write(
        sylphx_profile.join("auth.json"),
        r#"{"access_token":"spiron-shared-token-123","org_id":"org_shared"}"#,
    )
    .unwrap();

    assert_eq!(
        crate::create::compatible_harnesses_for("spiron"),
        &["spiron", "spiron-cloud", "sylphx"]
    );
    assert_eq!(
        crate::create::compatible_harnesses_for("spiron-cloud"),
        &["spiron", "spiron-cloud", "sylphx"]
    );
    assert_eq!(
        crate::create::compatible_harnesses_for("sylphx"),
        &["spiron", "spiron-cloud", "sylphx"]
    );

    let agent = store
        .create_agent(anyagents_core::NewAgent::new("spiron-seat", "spiron"))
        .unwrap();
    let target_cfg = data.join("agents").join(agent.id.as_str());
    std::fs::create_dir_all(&target_cfg).unwrap();
    crate::create::seed_harness_credentials(&store, data, &agent, &target_cfg).unwrap();
    crate::spiron::write_spiron_config(
        &target_cfg,
        agent.model.as_deref(),
        Path::new("/usr/bin/anyagents"),
        &["mcp".to_string()],
    )
    .unwrap();

    for rel in ["auth.json", ".config/sylphx/auth.json", ".spiron/auth.json"] {
        let raw = std::fs::read_to_string(target_cfg.join(rel))
            .unwrap_or_else(|e| panic!("missing {rel}: {e}"));
        let val: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(val["access_token"], "spiron-shared-token-123", "in {rel}");
    }
}
