//! Track admitted HTTP handlers and the blocking work they start. A disconnected
//! client drops the handler future, but not its blocking work: exiting during
//! that work could cut a mutation between steps. Axum separately drains the
//! listeners and their response bodies.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::Request;
use axum::http::{StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use futures::StreamExt as _;
use tokio::sync::{Notify, watch};

/// Counts HTTP calls whose handlers or blocking work are still running.
#[derive(Default)]
pub(crate) struct Tracker {
    running: AtomicUsize,
    idle: Notify,
}

/// One call's claim on the stop: held by the call's future and by the
/// blocking work it starts; the call counts until the last holder lets go.
pub(crate) struct Token {
    tracker: &'static Tracker,
}

impl Drop for Token {
    fn drop(&mut self) {
        if self.tracker.running.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.tracker.idle.notify_waiters();
        }
    }
}

impl Tracker {
    pub(crate) const fn new() -> Self {
        Self {
            running: AtomicUsize::new(0),
            idle: Notify::const_new(),
        }
    }

    pub(crate) fn begin(&'static self) -> Arc<Token> {
        self.running.fetch_add(1, Ordering::SeqCst);
        Arc::new(Token { tracker: self })
    }

    pub(crate) fn running(&self) -> usize {
        self.running.load(Ordering::SeqCst)
    }

    pub(crate) async fn wait_until_idle(&self) {
        loop {
            // Subscribe before sampling the count so the last drop cannot be missed.
            let idle = self.idle.notified();
            if self.running() == 0 {
                return;
            }
            idle.await;
        }
    }
}

pub(crate) static REQUESTS: Tracker = Tracker::new();

/// Bound both the listener drain (including finite response bodies) and any
/// blocking work that outlived its connection by the same shutdown deadline.
pub(super) async fn drain<T>(
    listeners: impl Future<Output = T>,
    requests: &Tracker,
    within: Duration,
) -> Result<T, tokio::time::error::Elapsed> {
    tokio::time::timeout(within, async {
        let result = listeners.await;
        // No listener can admit new top-level work after all listeners return.
        requests.wait_until_idle().await;
        result
    })
    .await
}

/// The stop signal for request handlers that own an indefinite response body.
#[derive(Clone)]
pub(super) struct Stopping(watch::Receiver<bool>);

impl Stopping {
    async fn notified(mut self) {
        let _ = self.0.wait_for(|stop| *stop).await;
    }

    /// End an explicitly open-ended SSE response with a clean EOF rather than
    /// a connection reset. Keep finite responses on the normal drain path.
    pub(super) fn close_event_stream(response: Response, stop: Option<Self>) -> Response {
        let is_sse = response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.split(';').next())
            .is_some_and(|mime| mime.trim().eq_ignore_ascii_case("text/event-stream"));
        if let Some(stop) = stop
            && is_sse
        {
            let (mut parts, body) = response.into_parts();
            parts.headers.remove(header::CONTENT_LENGTH);
            let body = Body::from_stream(body.into_data_stream().take_until(stop.notified()));
            return Response::from_parts(parts, body);
        }
        response
    }
}

/// Apply only at the actual HTTP listeners, not the internal router used by
/// an already-admitted MCP call. That call may still need to dispatch work.
pub(super) fn listener(app: Router, stopping: &watch::Sender<bool>) -> Router {
    app.layer(axum::middleware::from_fn({
        let stopping = Stopping(stopping.subscribe());
        move |mut request: Request, next: Next| {
            let stopping = stopping.clone();
            async move {
                // Axum shuts down the sockets asynchronously. Don't admit work
                // on a connection that races this signal or is already open.
                if *stopping.0.borrow() || stopping.0.has_changed().is_err() {
                    return StatusCode::SERVICE_UNAVAILABLE.into_response();
                }
                request.extensions_mut().insert(stopping);
                next.run(request).await
            }
        }
    }))
}

tokio::task_local! {
    static CURRENT: Arc<Token>;
}

/// Every handler counts, regardless of HTTP method or MCP action.
pub(crate) async fn track(request: Request, next: Next) -> Response {
    track_with(&REQUESTS, request, next).await
}

async fn track_with(tracker: &'static Tracker, request: Request, next: Next) -> Response {
    CURRENT.scope(tracker.begin(), next.run(request)).await
}

/// The current call's token, to move into blocking work it starts, so the call
/// keeps counting after its future is dropped and until that work returns.
pub(crate) fn carry() -> Option<Arc<Token>> {
    CURRENT.try_with(Arc::clone).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    static TEST_TRACKER: Tracker = Tracker::new();

    #[tokio::test(start_paused = true)]
    async fn blocking_work_keeps_its_call_counted_after_the_future_is_dropped() {
        let token = TEST_TRACKER.begin();
        let carried = CURRENT
            .scope(token, async { carry().expect("inside a call") })
            .await;
        // The future is gone; the blocking work it started still holds the
        // call, and the stop waits for it.
        assert_eq!(TEST_TRACKER.running(), 1);
        assert!(
            drain(async {}, &TEST_TRACKER, Duration::from_millis(100))
                .await
                .is_err()
        );
        drop(carried);
        drain(async {}, &TEST_TRACKER, Duration::from_secs(5))
            .await
            .expect("drained after the last holder released the token");
        assert_eq!(TEST_TRACKER.running(), 0);
        assert!(carry().is_none(), "outside a call nothing is carried");
    }
}

#[cfg(test)]
mod listener_tests {
    use super::*;
    use crate::server::bootstrap::stopped;
    use axum::body::Bytes;
    use axum::http::Method;
    use axum::routing::any;
    use http_body_util::{BodyExt as _, Empty};
    use hyper::client::conn::http1;
    use hyper_util::rt::TokioIo;
    use tokio::io::{AsyncRead, AsyncWrite};
    use tokio::net::{TcpListener, TcpStream, UnixListener, UnixStream};
    use tokio::sync::oneshot;
    use tokio::time::{Instant, advance, timeout};

    const WATCHDOG: Duration = Duration::from_secs(5);
    const BUDGET: Duration = Duration::from_secs(60);

    struct Fixture {
        address: std::net::SocketAddr,
        stopping: watch::Sender<bool>,
        stopped: tokio::task::JoinHandle<()>,
        app: Router,
    }

    impl Fixture {
        async fn start(app: Router, tracker: &'static Tracker) -> Self {
            let (stopping, _) = watch::channel(false);
            let app = listener(
                app.layer(axum::middleware::from_fn(move |req, next| {
                    track_with(tracker, req, next)
                })),
                &stopping,
            );
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = socket.local_addr().unwrap();
            let stop = stopped(&stopping);
            let serve = axum::serve(socket, app.clone()).with_graceful_shutdown(stop);
            let stopped = tokio::spawn(async move { serve.await.unwrap() });
            Self {
                address,
                stopping,
                stopped,
                app,
            }
        }

        async fn client(&self) -> http1::SendRequest<Empty<Bytes>> {
            client(TcpStream::connect(self.address).await.unwrap()).await
        }
    }

    async fn client(
        io: impl AsyncRead + AsyncWrite + Unpin + Send + 'static,
    ) -> http1::SendRequest<Empty<Bytes>> {
        let (mut client, conn) = http1::handshake(TokioIo::new(io)).await.unwrap();
        tokio::spawn(async move {
            let _ = conn.await;
        });
        client.ready().await.unwrap();
        client
    }

    fn request(method: Method, path: &str) -> axum::http::Request<Empty<Bytes>> {
        axum::http::Request::builder()
            .method(method)
            .uri(path)
            .header(header::HOST, "localhost")
            .body(Empty::new())
            .unwrap()
    }

    #[tokio::test]
    async fn slow_reads_writes_and_finite_sse_drain_after_admission_closes() {
        static TRACKER: Tracker = Tracker::new();
        timeout(WATCHDOG, async {
            use tower::ServiceExt as _;
            let read = Arc::new(Notify::new());
            let head = Arc::new(Notify::new());
            let write = Arc::new(Notify::new());
            let release = Arc::new(Notify::new());
            let (tail, rest) = oneshot::channel();
            let rest = Arc::new(std::sync::Mutex::new(Some(rest)));
            let app = Router::new()
                .route(
                    "/rpc",
                    any({
                        let (read, head, write, release) = (
                            Arc::clone(&read),
                            Arc::clone(&head),
                            Arc::clone(&write),
                            Arc::clone(&release),
                        );
                        move |method: Method| {
                            let (entered, release) = (
                                Arc::clone(match method {
                                    Method::GET => &read,
                                    Method::HEAD => &head,
                                    Method::POST => &write,
                                    _ => unreachable!(),
                                }),
                                Arc::clone(&release),
                            );
                            async move {
                                let done = release.notified();
                                entered.notify_one();
                                done.await;
                                if method == Method::GET {
                                    "read completed"
                                } else {
                                    "write completed"
                                }
                            }
                        }
                    }),
                )
                .route(
                    "/finite",
                    any(move || {
                        let rest = rest.lock().unwrap().take().unwrap();
                        async move {
                            let stream =
                                futures::stream::iter([Ok::<_, std::io::Error>("data: hello\n\n")])
                                    .chain(futures::stream::once(async move {
                                        Ok(rest.await.unwrap())
                                    }));
                            (
                                [(header::CONTENT_TYPE, "text/event-stream")],
                                Body::from_stream(stream),
                            )
                        }
                    }),
                );
            let fx = Fixture::start(app, &TRACKER).await;
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("rpc.sock");
            let unix = UnixListener::bind(&path).unwrap();
            let unix_serve =
                axum::serve(unix, fx.app.clone()).with_graceful_shutdown(stopped(&fx.stopping));
            let unix_task = tokio::spawn(async move { unix_serve.await.unwrap() });
            let call = |mut client: http1::SendRequest<Empty<Bytes>>, method, path| {
                tokio::spawn(
                    async move { client.send_request(request(method, path)).await.unwrap() },
                )
            };
            let read_resp = call(fx.client().await, Method::GET, "/rpc");
            let head_resp = call(
                client(UnixStream::connect(&path).await.unwrap()).await,
                Method::HEAD,
                "/rpc",
            );
            let write_resp = call(fx.client().await, Method::POST, "/rpc");
            let finite = call(fx.client().await, Method::POST, "/finite");
            read.notified().await;
            head.notified().await;
            write.notified().await;
            assert_eq!(TRACKER.running(), 3, "each RPC handler is tracked");
            let mut finite = finite.await.unwrap();
            assert_eq!(finite.status(), StatusCode::OK);
            assert_eq!(
                finite
                    .body_mut()
                    .frame()
                    .await
                    .unwrap()
                    .unwrap()
                    .into_data()
                    .unwrap(),
                "data: hello\n\n"
            );
            fx.stopping.send_replace(true);
            // Exercise the middleware even before Axum observes the signal, as
            // on a keep-alive connection racing shutdown.
            let new_request = fx
                .app
                .oneshot(
                    axum::http::Request::get("/rpc")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(new_request.status(), StatusCode::SERVICE_UNAVAILABLE);
            // With active requests still held, the OS listener must close.
            while TcpStream::connect(fx.address).await.is_ok() {
                tokio::task::yield_now().await;
            }
            while UnixStream::connect(&path).await.is_ok() {
                tokio::task::yield_now().await;
            }
            let listeners = async {
                let (tcp, unix) = tokio::join!(fx.stopped, unix_task);
                tcp.unwrap();
                unix.unwrap();
            };
            let draining = tokio::spawn(drain(listeners, &TRACKER, BUDGET));
            release.notify_waiters();
            for (resp, expected) in [
                (read_resp, "read completed"),
                (head_resp, ""),
                (write_resp, "write completed"),
            ] {
                let resp = resp.await.unwrap();
                assert_eq!(resp.status(), StatusCode::OK);
                assert_eq!(
                    resp.into_body().collect().await.unwrap().to_bytes(),
                    expected
                );
            }
            assert!(!draining.is_finished(), "discarded a finite response body");
            tail.send("data: done\n\n").unwrap();
            assert_eq!(
                finite.into_body().collect().await.unwrap().to_bytes(),
                "data: done\n\n"
            );
            draining.await.unwrap().unwrap();
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn hung_http_request_hits_the_shared_deadline() {
        static TRACKER: Tracker = Tracker::new();
        // Establish real I/O before pausing the timer; use virtual time only
        // for the deadline so this does not wait a minute in CI.
        let entered = Arc::new(Notify::new());
        let (fx, call) = timeout(WATCHDOG, async {
            let app = Router::new().route(
                "/hung",
                any({
                    let entered = Arc::clone(&entered);
                    move || {
                        let entered = Arc::clone(&entered);
                        async move {
                            entered.notify_one();
                            std::future::pending::<&'static str>().await
                        }
                    }
                }),
            );
            let fx = Fixture::start(app, &TRACKER).await;
            let mut client = fx.client().await;
            let call =
                tokio::spawn(
                    async move { client.send_request(request(Method::GET, "/hung")).await },
                );
            entered.notified().await;
            (fx, call)
        })
        .await
        .unwrap();
        tokio::time::pause();
        let start = Instant::now();
        fx.stopping.send_replace(true);
        let drain = drain(fx.stopped, &TRACKER, BUDGET);
        tokio::pin!(drain);
        assert!(futures::poll!(&mut drain).is_pending());
        advance(BUDGET - Duration::from_secs(1)).await;
        assert!(futures::poll!(&mut drain).is_pending());
        assert!(drain.await.is_err());
        assert!(start.elapsed() >= BUDGET);
        assert!(start.elapsed() < BUDGET + Duration::from_millis(10));
        call.abort();
    }
}
