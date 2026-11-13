//! A request that waits on the disk or the store holds only itself.
//!
//! The server below runs on a runtime with exactly one async worker. If any
//! step of a request (the caller's authentication or the handler) blocked that
//! worker, the cheap health route could not answer until the blocked request
//! finished; each test holds a request inside a blocking read and asks the
//! health route meanwhile.

use std::io::{Read as _, Write as _};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyagents_core::Store;
use anyagents_supervisor::HealthBoard;

use super::{AppState, ProcessRunner, build_router};
use crate::auth;
use crate::identity::ServingIdentity;

const OPERATOR: &str = "0123456789abcdef0123456789abcdef";

/// The router on its own one-worker runtime, until the returned sender drops.
fn one_worker_server(state: AppState, socket: PathBuf) -> tokio::sync::oneshot::Sender<()> {
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let (bound, ready) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            let listener = tokio::net::UnixListener::bind(&socket).unwrap();
            bound.send(()).unwrap();
            let _ = axum::serve(listener, build_router(state))
                .with_graceful_shutdown(async move {
                    let _ = stopped.await;
                })
                .await;
        });
    });
    ready.recv().unwrap();
    stop
}

/// One HTTP/1.1 exchange; the status line, or the error when nothing came back
/// within `wait`.
fn call(
    socket: &Path,
    path: &str,
    headers: &[(&str, &str)],
    wait: Duration,
) -> Result<String, String> {
    let mut stream = UnixStream::connect(socket).map_err(|err| err.to_string())?;
    stream
        .set_read_timeout(Some(wait))
        .map_err(|err| err.to_string())?;
    let mut request = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n");
    for (name, value) in headers {
        request.push_str(&format!("{name}: {value}\r\n"));
    }
    request.push_str("\r\n");
    stream
        .write_all(request.as_bytes())
        .map_err(|err| err.to_string())?;
    let mut answer = String::new();
    stream
        .read_to_string(&mut answer)
        .map_err(|err| format!("no answer within {wait:?}: {err}"))?;
    Ok(answer)
}

fn state(dir: &Path) -> AppState {
    AppState {
        store: Arc::new(Store::open(dir.join("state.db")).unwrap()),
        health: HealthBoard::default(),
        sign_in: Arc::new(Mutex::new(ProcessRunner)),
        auth: auth::Mode::Confined {
            operator_digest: Some(auth::operator_digest(OPERATOR)),
        },
        identity: Arc::new(ServingIdentity::capture()),
    }
}

fn healthz_answers(socket: &Path) {
    let answer = call(socket, "/v1/webhooks/healthz", &[], Duration::from_secs(2))
        .expect("the health route answers while another request waits");
    assert!(answer.starts_with("HTTP/1.1 200"), "{answer}");
}

/// Authentication that waits on the store holds its own request only. A server
/// whose store does not hold the capability key asks the primary's socket; that
/// primary accepts and never answers, so the check waits in a blocking read.
#[test]
fn an_authentication_that_waits_never_holds_the_async_worker() {
    let dir = crate::test_tempdir();
    let primary = UnixListener::bind(dir.path().join("anyagents.sock")).unwrap();
    let api = dir.path().join("api.sock");
    let stop = one_worker_server(state(dir.path()), api.clone());

    let waiting = {
        let api = api.clone();
        std::thread::spawn(move || {
            call(
                &api,
                "/v1/agents",
                &[("x-anyagents-agent", "a-capability-the-primary-must-check")],
                Duration::from_secs(10),
            )
        })
    };
    // The check is now inside its blocking read, with nobody answering.
    let (held, _) = primary.accept().unwrap();
    healthz_answers(&api);
    drop(held);
    let refused = waiting.join().unwrap().unwrap();
    assert!(refused.starts_with("HTTP/1.1 401"), "{refused}");
    drop(stop);
}

/// A handler whose file read waits on the disk holds its own request only. The
/// subagent defaults are a FIFO with no writer, so the read waits until the
/// test writes them.
#[test]
fn a_handler_whose_read_waits_never_holds_the_async_worker() {
    use std::os::unix::ffi::OsStrExt as _;
    use std::os::unix::fs::OpenOptionsExt as _;
    let dir = crate::test_tempdir();
    let defaults = dir.path().join("subagent-defaults.json");
    let fifo = std::ffi::CString::new(defaults.as_os_str().as_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
    let api = dir.path().join("api.sock");
    let stop = one_worker_server(state(dir.path()), api.clone());

    let waiting = {
        let api = api.clone();
        std::thread::spawn(move || {
            call(
                &api,
                "/v1/subagent-defaults",
                &[("x-anyagents-operator", OPERATOR)],
                Duration::from_secs(10),
            )
        })
    };
    // A writer can open the FIFO only once the handler has it open for reading;
    // the handler then waits in its read until the writer speaks.
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut writer = loop {
        match std::fs::OpenOptions::new()
            .write(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(&defaults)
        {
            Ok(writer) => break writer,
            Err(err) if err.raw_os_error() == Some(libc::ENXIO) && Instant::now() < deadline => {
                std::thread::yield_now();
            }
            Err(err) => panic!("the handler never opened its defaults: {err}"),
        }
    };
    healthz_answers(&api);
    writer
        .write_all(br#"{"default":{"mode":"preset","harness":"codex"}}"#)
        .unwrap();
    drop(writer);
    let answered = waiting.join().unwrap().unwrap();
    assert!(answered.starts_with("HTTP/1.1 200"), "{answered}");
    assert!(answered.contains(r#""harness":"codex""#), "{answered}");
    drop(stop);
}
