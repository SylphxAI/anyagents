//! Public serverless `browser_takeover` via outbound WebSocket reverse-tunnel.
//!
//! Sandboxes and local agents open an authenticated outbound WebSocket to
//! `GET /v1/takeover/{session_id}/tunnel` using their coordinator credential
//! (`x-anyagents-agent` or `x-anyagents-operator`). The coordinator serves the
//! live noVNC takeover UI, SSE state stream, control endpoints, and RFB
//! WebSocket publicly at:
//!
//!   `https://<ANYAGENTS_PUBLIC_URL>/takeover/{session_id}/?token={takeover_token}`
//!
//! Security & proxy-resilience invariants:
//! - Public viewer endpoints require no admin OIDC login and authenticate
//!   strictly fail-closed via constant-time verification of `takeover_token`
//!   (`?token=`, scoped `HttpOnly` cookie, or `Authorization: Bearer`).
//! - Reconnect re-attachment requires BOTH matching owner principal (same
//!   `AgentId`, or `Operator`) AND matching `takeover_token` (fail-closed).
//! - Both the guest tunnel WS and every viewer WS send `Ping` keepalives every
//!   25s (< 60s) to clear proxy/CDN idle cutoffs.
//! - Unexpected drops do not end a remote lease pin. Standalone sessions
//!   retain their brief reconnect grace window; explicit unregister ends either.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock, Weak};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use anyagents_core::{Store, constant_time_eq};
use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::ws::{CloseFrame, Message, Utf8Bytes, WebSocket, WebSocketUpgrade};
use axum::extract::{FromRequestParts, Path as AxumPath, Request, State};
use axum::http::header::{self, HeaderName, HeaderValue};
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use base64::Engine as _;
use futures::{SinkExt, Stream, StreamExt};
use http_body_util::BodyExt as _;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, Notify, RwLock, mpsc, oneshot, watch};

use crate::auth::{Principal, is_valid_takeover_session_id};

use super::in_flight::Stopping;
use super::serverless;
use super::types::AppState;

mod guest_frames;
mod lease;
mod proxy;
mod recovery;
mod viewer;
use guest_frames::{handle_guest_binary_frame, handle_guest_control_frame};
use lease::{ENDED, EXPIRED, LeasePin, RegisterError};
use proxy::{proxy_viewer_http, proxy_viewer_ws};

pub(crate) const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(25);
pub(crate) const DEFAULT_RECONNECT_GRACE: Duration = Duration::from_secs(15);
const EXPIRED_VIEWER_RETENTION: Duration = Duration::from_secs(300);
const RECONNECT_WAIT_CAP: Duration = Duration::from_secs(8);
const REGISTER_TIMEOUT: Duration = Duration::from_secs(10);
const HTTP_HEAD_TIMEOUT: Duration = Duration::from_secs(30);
const WS_OPEN_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_SESSIONS: usize = 256;
const MAX_HTTP_REQ_BODY_BYTES: usize = 16 * 1024 * 1024;
const MAX_HTTP_RESP_BYTES: usize = 16 * 1024 * 1024;
const MAX_TUNNEL_FRAME_BYTES: usize = 24 * 1024 * 1024;
const MAX_TOKEN_LEN: usize = 256;
const TUNNEL_OUTBOUND_QUEUE: usize = 256;
const HTTP_CHUNK_QUEUE: usize = 64;
const VIEWER_WS_QUEUE: usize = 256;
const BIN_TAG_WS_DATA: u8 = 0x01;
const BIN_TAG_HTTP_CHUNK: u8 = 0x02;
const COOKIE_PREFIX: &str = "anyagents_takeover_";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ControlFrame {
    Register {
        token: String,
        #[serde(default)]
        reconnect: bool,
        #[serde(default)]
        agent_id: Option<String>,
        #[serde(default)]
        local_origin: Option<String>,
    },
    Registered {
        public_base_url: String,
        public_url: String,
        session_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ttl_ms: Option<u64>,
    },
    Unregister,
    Error {
        code: String,
        message: String,
    },
    HttpRequest {
        id: u64,
        method: String,
        path: String,
        #[serde(default)]
        query: Option<String>,
        #[serde(default)]
        headers: Vec<(String, String)>,
        #[serde(default)]
        body_b64: String,
    },
    HttpResponseHead {
        id: u64,
        status: u16,
        #[serde(default)]
        headers: Vec<(String, String)>,
        #[serde(default)]
        done: bool,
    },
    HttpResponseEnd {
        id: u64,
        #[serde(default)]
        error: Option<String>,
    },
    HttpCancel {
        id: u64,
    },
    WsOpen {
        id: u64,
        path: String,
        #[serde(default)]
        query: Option<String>,
        #[serde(default)]
        protocols: Vec<String>,
        #[serde(default)]
        headers: Vec<(String, String)>,
    },
    WsOpened {
        id: u64,
        ok: bool,
        #[serde(default)]
        protocol: Option<String>,
        #[serde(default)]
        error: Option<String>,
    },
    WsText {
        id: u64,
        text: String,
    },
    WsClose {
        id: u64,
        #[serde(default)]
        code: Option<u16>,
        #[serde(default)]
        reason: Option<String>,
    },
}

enum OutboundTunnelMsg {
    Text(String),
    Binary(Bytes),
}

impl OutboundTunnelMsg {
    fn control(frame: &ControlFrame) -> Result<Self, serde_json::Error> {
        serde_json::to_string(frame).map(Self::Text)
    }

    fn ws_binary(stream_id: u64, payload: &[u8]) -> Self {
        Self::Binary(encode_binary_frame(BIN_TAG_WS_DATA, stream_id, payload))
    }
}

fn encode_binary_frame(tag: u8, id: u64, payload: &[u8]) -> Bytes {
    let mut buf = Vec::with_capacity(9 + payload.len());
    buf.push(tag);
    buf.extend_from_slice(&id.to_be_bytes());
    buf.extend_from_slice(payload);
    Bytes::from(buf)
}

fn decode_binary_frame(raw: &[u8]) -> Option<(u8, u64, Bytes)> {
    if raw.len() < 9 {
        return None;
    }
    let id = u64::from_be_bytes(raw[1..9].try_into().ok()?);
    Some((raw[0], id, Bytes::copy_from_slice(&raw[9..])))
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum TunnelOwner {
    Operator,
    Agent(anyagents_core::AgentId),
}

impl TunnelOwner {
    fn from_principal(principal: &Principal) -> Self {
        match principal {
            Principal::Operator => Self::Operator,
            Principal::Agent { agent_id, .. } => Self::Agent(agent_id.clone()),
        }
    }

    fn may_reattach(&self, candidate: &Self) -> bool {
        self == candidate || matches!(candidate, Self::Operator)
    }
}

struct TunnelConnState {
    generation: u64,
    tx: Option<mpsc::Sender<OutboundTunnelMsg>>,
    disconnected_at: Option<Instant>,
}

impl TunnelConnState {
    fn is_expired(&self, grace: Duration) -> bool {
        self.tx.is_none() && self.disconnected_at.is_some_and(|at| at.elapsed() >= grace)
    }
}

struct HttpHeadPayload {
    status: u16,
    headers: Vec<(String, String)>,
    done: bool,
}

struct PendingHttp {
    generation: u64,
    head_tx: Option<oneshot::Sender<HttpHeadPayload>>,
    body_tx: mpsc::Sender<Result<Bytes, String>>,
    is_sse: bool,
    bytes_received: usize,
}

enum ViewerWsMsg {
    Binary(Bytes),
    Text(String),
    Close(Option<CloseFrame>),
}

struct WsOpenedPayload {
    ok: bool,
    protocol: Option<String>,
    error: Option<String>,
}

struct PendingWs {
    generation: u64,
    opened_tx: Option<oneshot::Sender<WsOpenedPayload>>,
    viewer_tx: mpsc::Sender<ViewerWsMsg>,
}

struct TakeoverSession {
    owner: TunnelOwner,
    store: Weak<Store>,
    created: Instant,
    token: String,
    public_origin: String,
    public_base_url: String,
    cookie_path: String,
    secure_cookie: bool,
    pin: Option<LeasePin>,
    control: Mutex<bool>,
    returned: watch::Sender<bool>,
    conn: RwLock<TunnelConnState>,
    reconnect_notify: Notify,
    next_req_id: AtomicU64,
    next_ws_id: AtomicU64,
    inflight_http: Mutex<HashMap<u64, PendingHttp>>,
    inflight_ws: Mutex<HashMap<u64, PendingWs>>,
}

impl TakeoverSession {
    fn is_evictable(&self, grace: Duration) -> bool {
        if let Some(pin) = &self.pin {
            return Instant::now().saturating_duration_since(pin.deadline())
                >= EXPIRED_VIEWER_RETENTION;
        }
        self.conn.try_read().is_ok_and(|c| c.is_expired(grace))
    }

    async fn wait_for_tunnel(
        &self,
        grace: Duration,
    ) -> Result<(u64, mpsc::Sender<OutboundTunnelMsg>), StatusCode> {
        let deadline = Instant::now() + grace.min(RECONNECT_WAIT_CAP);
        let grace = if self.pin.is_some() {
            Duration::MAX
        } else {
            grace
        };
        loop {
            let notified = self.reconnect_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let remaining = {
                let conn = self.conn.read().await;
                if let Some(ref tx) = conn.tx {
                    return Ok((conn.generation, tx.clone()));
                }
                let disc = conn.disconnected_at.ok_or(StatusCode::NOT_FOUND)?;
                let elapsed = disc.elapsed();
                if elapsed >= grace {
                    return Err(StatusCode::NOT_FOUND);
                }
                grace
                    .saturating_sub(elapsed)
                    .min(deadline.saturating_duration_since(Instant::now()))
            };
            if remaining.is_zero() || tokio::time::timeout(remaining, notified).await.is_err() {
                let conn = self.conn.read().await;
                if let Some(ref tx) = conn.tx {
                    return Ok((conn.generation, tx.clone()));
                }
                return Err(if conn.is_expired(grace) {
                    StatusCode::NOT_FOUND
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                });
            }
        }
    }

    async fn abort_generation_streams(&self, generation: u64) {
        self.inflight_http
            .lock()
            .await
            .retain(|_, p| p.generation != generation);
        let mut stale = Vec::new();
        self.inflight_ws.lock().await.retain(|_, p| {
            if p.generation == generation {
                stale.push(p.viewer_tx.clone());
                false
            } else {
                true
            }
        });
        for tx in stale {
            let _ = tx.try_send(ViewerWsMsg::Close(None));
        }
    }
}

/// Bounded in-memory registry of active takeover reverse-tunnel sessions.
pub(crate) struct TakeoverRegistry {
    sessions: RwLock<HashMap<String, Arc<TakeoverSession>>>,
    grace_window: Duration,
    keepalive_interval: Duration,
    max_sessions: usize,
}

impl TakeoverRegistry {
    pub(crate) fn new() -> Self {
        Self::with_tunables(DEFAULT_RECONNECT_GRACE, KEEPALIVE_INTERVAL)
    }

    fn with_tunables(grace_window: Duration, keepalive_interval: Duration) -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
            grace_window,
            keepalive_interval,
            max_sessions: MAX_SESSIONS,
        }
    }

    async fn get_active(&self, session_id: &str) -> Option<Arc<TakeoverSession>> {
        let entry = self.sessions.read().await.get(session_id).cloned()?;
        (!entry.is_evictable(self.grace_window)).then_some(entry)
    }

    async fn register_or_reattach(
        &self,
        session_id: &str,
        store: Arc<Store>,
        owner: TunnelOwner,
        register: Register,
        public: PublicEndpoint,
        tx: mpsc::Sender<OutboundTunnelMsg>,
    ) -> Result<(Arc<TakeoverSession>, u64), RegisterError> {
        let Register { token, reconnect } = register;
        let mut sessions = self.sessions.write().await;
        sessions.retain(|_, s| !s.is_evictable(self.grace_window));
        if let Some(existing) = sessions.get(session_id).cloned() {
            if !existing.owner.may_reattach(&owner) {
                return Err(("FORBIDDEN", "session owned by another principal", 4403));
            }
            if !constant_time_eq(existing.token.as_bytes(), token.as_bytes()) {
                return Err(("UNAUTHORIZED", "takeover token mismatch on reconnect", 4401));
            }
            if *existing.returned.borrow() {
                return Err(ENDED);
            }
            if let Some(pin) = &existing.pin {
                pin.check().await?;
            }
            let generation = {
                let mut conn = existing.conn.write().await;
                conn.generation = conn.generation.saturating_add(1);
                conn.tx = Some(tx);
                conn.disconnected_at = None;
                conn.generation
            };
            existing.reconnect_notify.notify_waiters();
            return Ok((existing, generation));
        }
        if sessions.len() >= self.max_sessions {
            return Err((
                "CAPACITY_EXCEEDED",
                "maximum takeover sessions reached",
                4429,
            ));
        }
        let pin =
            LeasePin::register(Arc::clone(&store), &owner, session_id, &token, reconnect).await?;
        recovery::remember_link(&store, &owner, session_id, &token, pin.as_ref()).await;
        let entry = Arc::new(TakeoverSession {
            owner,
            store: Arc::downgrade(&store),
            created: Instant::now(),
            token,
            public_origin: public.origin,
            public_base_url: public.base_url,
            cookie_path: public.cookie_path,
            secure_cookie: public.secure,
            pin,
            control: Mutex::new(false),
            returned: watch::channel(false).0,
            conn: RwLock::new(TunnelConnState {
                generation: 1,
                tx: Some(tx),
                disconnected_at: None,
            }),
            reconnect_notify: Notify::new(),
            next_req_id: AtomicU64::new(1),
            next_ws_id: AtomicU64::new(1),
            inflight_http: Mutex::new(HashMap::new()),
            inflight_ws: Mutex::new(HashMap::new()),
        });
        sessions.insert(session_id.to_string(), Arc::clone(&entry));
        Ok((entry, 1))
    }

    async fn remove_entry(
        &self,
        session_id: &str,
        entry: &Arc<TakeoverSession>,
        generation: u64,
        force: bool,
    ) {
        // Take locks in the same order as registration. A superseded socket
        // cannot revoke the generation that replaced it.
        let mut sessions = self.sessions.write().await;
        if !sessions
            .get(session_id)
            .is_some_and(|cur| Arc::ptr_eq(cur, entry))
        {
            return;
        }
        let mut conn = entry.conn.write().await;
        if conn.generation != generation || (!force && conn.tx.is_some()) {
            return;
        }
        if force && let Some(pin) = &entry.pin {
            pin.end().await;
        }
        conn.tx = None;
        conn.disconnected_at = None;
        entry.reconnect_notify.notify_waiters();
        if matches!(entry.owner, TunnelOwner::Agent(_)) {
            recovery::end_link(entry.store.upgrade(), session_id, &entry.token).await;
        }
        sessions.remove(session_id);
    }
}

struct StoreRegistrySlot {
    store: Weak<Store>,
    registry: Arc<TakeoverRegistry>,
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

/// Share one [`TakeoverRegistry`] across every listener backed by the same `Arc<Store>`,
/// isolating independent tests and pruning dropped stores automatically.
pub(crate) fn registry_for_store(store: &Arc<Store>) -> Arc<TakeoverRegistry> {
    let key = Arc::as_ptr(store) as usize;
    let weak = Arc::downgrade(store);
    let mut map = lock_store_registries();
    if let Some(slot) = map.get(&key)
        && Weak::ptr_eq(&slot.store, &weak)
    {
        return Arc::clone(&slot.registry);
    }
    let registry = Arc::new(TakeoverRegistry::new());
    map.insert(
        key,
        StoreRegistrySlot {
            store: weak,
            registry: Arc::clone(&registry),
        },
    );
    registry
}

#[cfg(test)]
fn install_test_registry(store: &Arc<Store>, registry: Arc<TakeoverRegistry>) {
    lock_store_registries().insert(
        Arc::as_ptr(store) as usize,
        StoreRegistrySlot {
            store: Arc::downgrade(store),
            registry,
        },
    );
}

/// Mount both the authenticated guest reverse-tunnel endpoint and the public
/// token-authenticated takeover viewer routes.
pub(super) fn router(console_available: bool) -> Router<AppState> {
    Router::new()
        .route("/v1/takeover/{session_id}/tunnel", get(tunnel_ws_route))
        .route("/takeover/{session_id}", any(viewer_no_slash_route))
        .route("/takeover/{session_id}/", any(viewer_root_route))
        .route("/takeover/{session_id}/{*rest}", any(viewer_subpath_route))
        .merge(recovery::router())
        .merge(viewer::router())
        .layer(axum::Extension(recovery::ConsoleAvailable(
            console_available,
        )))
}

#[derive(Clone, Debug)]
struct PublicEndpoint {
    origin: String,
    base_url: String,
    cookie_path: String,
    secure: bool,
}

fn forwarded_proto(headers: &HeaderMap) -> Option<&'static str> {
    let first = headers
        .get("x-forwarded-proto")?
        .to_str()
        .ok()?
        .split(',')
        .next()?
        .trim();
    if first.eq_ignore_ascii_case("https") {
        Some("https")
    } else if first.eq_ignore_ascii_case("http") {
        Some("http")
    } else {
        None
    }
}

fn request_host(headers: &HeaderMap) -> Option<String> {
    let first = headers
        .get("x-forwarded-host")
        .or_else(|| headers.get(header::HOST))?
        .to_str()
        .ok()?
        .split(',')
        .next()?
        .trim();
    if first.is_empty()
        || first.len() > 255
        || first.bytes().any(|b| {
            b.is_ascii_control()
                || b.is_ascii_whitespace()
                || matches!(b, b'/' | b'\\' | b'@' | b'?' | b'#')
        })
    {
        return None;
    }
    Some(first.to_string())
}

fn resolve_public_endpoint(
    data_dir: &std::path::Path,
    headers: &HeaderMap,
    session_id: &str,
) -> Result<PublicEndpoint, String> {
    let raw_base = serverless::resolve_public_url(data_dir)
        .filter(|u| u.starts_with("http://") || u.starts_with("https://"))
        .or_else(|| {
            let host = request_host(headers)?;
            let proto = forwarded_proto(headers).unwrap_or("http");
            Some(format!("{proto}://{host}"))
        })
        .ok_or_else(|| {
            "cannot resolve public coordinator URL (set ANYAGENTS_PUBLIC_URL or send Host header)"
                .to_string()
        })?;
    let mut base = url::Url::parse(&format!(
        "{}/takeover/{session_id}/",
        raw_base.trim_end_matches('/')
    ))
    .map_err(|e| format!("invalid public URL `{raw_base}`: {e}"))?;
    if !matches!(base.scheme(), "http" | "https") || base.host_str().is_none() {
        return Err(format!("public URL `{raw_base}` must use http or https"));
    }
    base.set_query(None);
    base.set_fragment(None);
    Ok(PublicEndpoint {
        origin: base.origin().ascii_serialization(),
        cookie_path: base.path().to_string(),
        secure: base.scheme() == "https" || forwarded_proto(headers) == Some("https"),
        base_url: base.to_string(),
    })
}

fn is_valid_takeover_token(token: &str) -> bool {
    !token.is_empty()
        && token.len() <= MAX_TOKEN_LEN
        && token.bytes().all(|b| {
            !b.is_ascii_control()
                && !b.is_ascii_whitespace()
                && !matches!(b, b';' | b',' | b'"' | b'\\')
        })
}

fn json_error_response(status: StatusCode, message: &str) -> Response {
    (status, axum::Json(serde_json::json!({ "error": message }))).into_response()
}

/// `GET /v1/takeover/{session_id}/tunnel`: authenticated guest outbound WS reverse-tunnel.
async fn tunnel_ws_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    if !is_valid_takeover_session_id(&session_id) {
        return json_error_response(StatusCode::BAD_REQUEST, "invalid takeover session_id");
    }
    let public = match resolve_public_endpoint(state.store.data_dir(), &headers, &session_id) {
        Ok(p) => p,
        Err(why) => return json_error_response(StatusCode::BAD_REQUEST, &why),
    };
    let registry = registry_for_store(&state.store);
    let owner = TunnelOwner::from_principal(&principal);
    let store = state.store;
    ws.max_message_size(MAX_TUNNEL_FRAME_BYTES)
        .max_frame_size(MAX_TUNNEL_FRAME_BYTES)
        .on_upgrade(move |socket| {
            run_guest_tunnel(socket, registry, store, session_id, owner, public)
        })
}

async fn send_tunnel_error_and_close(
    ws_tx: &mut futures::stream::SplitSink<WebSocket, Message>,
    code: &str,
    message: &str,
    close_code: u16,
) {
    if let Ok(text) = serde_json::to_string(&ControlFrame::Error {
        code: code.to_string(),
        message: message.to_string(),
    }) {
        let _ = ws_tx.send(Message::Text(text.into())).await;
    }
    let _ = ws_tx
        .send(Message::Close(Some(CloseFrame {
            code: close_code,
            reason: Utf8Bytes::from(message.to_string()),
        })))
        .await;
}

struct Register {
    token: String,
    reconnect: bool,
}

async fn read_register(
    ws_rx: &mut futures::stream::SplitStream<WebSocket>,
) -> Result<Register, (&'static str, String, u16)> {
    let frame = tokio::time::timeout(REGISTER_TIMEOUT, async {
        while let Some(msg) = ws_rx.next().await {
            match msg {
                Ok(Message::Text(txt)) => {
                    return serde_json::from_str::<ControlFrame>(&txt)
                        .map_err(|e| format!("invalid register frame: {e}"));
                }
                Ok(Message::Ping(_) | Message::Pong(_)) => continue,
                Ok(Message::Close(_)) => return Err("closed before register".into()),
                Ok(Message::Binary(_)) => return Err("expected JSON Register text frame".into()),
                Err(err) => return Err(format!("websocket read error: {err}")),
            }
        }
        Err("disconnected before register".into())
    })
    .await
    .map_err(|_| ("TIMEOUT", "registration timed out".to_string(), 4408))?
    .map_err(|msg| ("BAD_REQUEST", msg, 4400))?;

    let ControlFrame::Register {
        token, reconnect, ..
    } = frame
    else {
        return Err((
            "BAD_REQUEST",
            "first tunnel frame must be `register`".into(),
            4400,
        ));
    };
    if !is_valid_takeover_token(&token) {
        return Err(("BAD_REQUEST", "invalid takeover token format".into(), 4400));
    }
    Ok(Register { token, reconnect })
}

async fn run_guest_tunnel(
    socket: WebSocket,
    registry: Arc<TakeoverRegistry>,
    store: Arc<Store>,
    session_id: String,
    owner: TunnelOwner,
    public: PublicEndpoint,
) {
    let (mut ws_tx, mut ws_rx) = socket.split();
    let register = match read_register(&mut ws_rx).await {
        Ok(frame) => frame,
        Err((code, msg, close)) => {
            send_tunnel_error_and_close(&mut ws_tx, code, &msg, close).await;
            return;
        }
    };

    let (out_tx, mut out_rx) = mpsc::channel::<OutboundTunnelMsg>(TUNNEL_OUTBOUND_QUEUE);
    let (entry, generation) = match registry
        .register_or_reattach(&session_id, store, owner, register, public, out_tx)
        .await
    {
        Ok(res) => res,
        Err((code, msg, close)) => {
            send_tunnel_error_and_close(&mut ws_tx, code, msg, close).await;
            return;
        }
    };

    let mut full_url = url::Url::parse(&entry.public_base_url).expect("validated base url");
    full_url
        .query_pairs_mut()
        .append_pair("token", &entry.token);
    let Ok(ack_text) = serde_json::to_string(&ControlFrame::Registered {
        public_base_url: entry.public_base_url.clone(),
        public_url: full_url.to_string(),
        session_id: session_id.clone(),
        ttl_ms: entry.pin.as_ref().map(LeasePin::remaining_ms),
    }) else {
        return;
    };
    if ws_tx.send(Message::Text(ack_text.into())).await.is_err() {
        handle_tunnel_disconnect(&registry, &session_id, &entry, generation, false).await;
        return;
    }

    let mut ping_interval = tokio::time::interval_at(
        tokio::time::Instant::now() + registry.keepalive_interval,
        registry.keepalive_interval,
    );
    ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let expiry = async {
        if let Some(pin) = &entry.pin {
            tokio::time::sleep_until(pin.deadline().into()).await;
        } else {
            std::future::pending::<()>().await;
        }
    };
    tokio::pin!(expiry);
    let mut returned = entry.returned.subscribe();
    let mut unregistered = false;
    let mut terminal = None;
    loop {
        tokio::select! {
            _ = async { drop(returned.wait_for(|value| *value).await); } => {
                unregistered = true;
                break;
            }
            _ = &mut expiry => {
                terminal = Some(EXPIRED);
                break;
            }
            _ = ping_interval.tick() => {
                if let Some(pin) = &entry.pin && let Err(err) = pin.check().await {
                    terminal = Some(err);
                    break;
                }
                if ws_tx.send(Message::Ping(Bytes::from_static(b"keepalive"))).await.is_err() {
                    break;
                }
            }
            outbound = out_rx.recv() => {
                let Some(msg) = outbound else { break; };
                let wire = match msg {
                    OutboundTunnelMsg::Text(t) => Message::Text(t.into()),
                    OutboundTunnelMsg::Binary(b) => Message::Binary(b),
                };
                if ws_tx.send(wire).await.is_err() {
                    break;
                }
            }
            incoming = ws_rx.next() => match incoming {
                Some(Ok(Message::Text(txt))) => {
                    let Ok(frame) = serde_json::from_str::<ControlFrame>(&txt) else { continue; };
                    if matches!(frame, ControlFrame::Unregister) {
                        unregistered = true;
                        break;
                    }
                    handle_guest_control_frame(&entry, frame).await;
                }
                Some(Ok(Message::Binary(raw))) => handle_guest_binary_frame(&entry, &raw).await,
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
                Some(Ok(Message::Close(cf))) => {
                    unregistered = cf.as_ref().is_some_and(|c| c.code == 1000 && c.reason.as_str() == "takeover_stopped");
                    break;
                }
                Some(Err(_)) | None => break,
            }
        }
    }

    handle_tunnel_disconnect(&registry, &session_id, &entry, generation, unregistered).await;
    if let Some((code, msg, close)) = terminal.or_else(|| unregistered.then_some(ENDED)) {
        send_tunnel_error_and_close(&mut ws_tx, code, msg, close).await;
    } else {
        let _ = ws_tx.send(Message::Close(None)).await;
    }
}

async fn handle_tunnel_disconnect(
    registry: &Arc<TakeoverRegistry>,
    session_id: &str,
    entry: &Arc<TakeoverSession>,
    generation: u64,
    unregistered: bool,
) {
    entry.abort_generation_streams(generation).await;
    if unregistered {
        registry
            .remove_entry(session_id, entry, generation, true)
            .await;
        return;
    }
    let became_disconnected = {
        let mut conn = entry.conn.write().await;
        if conn.generation == generation {
            conn.tx = None;
            conn.disconnected_at = Some(Instant::now());
            true
        } else {
            false
        }
    };
    if became_disconnected {
        entry.reconnect_notify.notify_waiters();
    }
    if became_disconnected && entry.pin.is_none() {
        let (reg, sid, sess, grace) = (
            Arc::clone(registry),
            session_id.to_string(),
            Arc::clone(entry),
            registry.grace_window,
        );
        tokio::spawn(async move {
            tokio::time::sleep(grace).await;
            reg.remove_entry(&sid, &sess, generation, false).await;
            sess.reconnect_notify.notify_waiters();
        });
    }
}

enum ViewerAuth {
    QueryToken,
    CookieOrBearer,
}

fn authenticate_viewer(
    session_id: &str,
    expected_token: &str,
    headers: &HeaderMap,
    uri: &Uri,
) -> Option<ViewerAuth> {
    let eq = |s: &str| constant_time_eq(s.as_bytes(), expected_token.as_bytes());
    if uri.query().is_some_and(|q| {
        url::form_urlencoded::parse(q.as_bytes()).any(|(k, v)| k == "token" && eq(&v))
    }) {
        return Some(ViewerAuth::QueryToken);
    }
    let cname = format!("{COOKIE_PREFIX}{session_id}");
    let cookie_ok = headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|r| r.split(';'))
        .any(|p| {
            p.trim()
                .split_once('=')
                .is_some_and(|(k, v)| k.trim() == cname && eq(v.trim()))
        });
    let bearer_ok = headers
        .get_all(header::AUTHORIZATION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .filter_map(|r| {
            let t = r.trim();
            t.strip_prefix("Bearer ")
                .or_else(|| t.strip_prefix("bearer "))
        })
        .any(|tok| eq(tok.trim()));
    (cookie_ok || bearer_ok).then_some(ViewerAuth::CookieOrBearer)
}

fn check_viewer_origin(headers: &HeaderMap, session: &TakeoverSession) -> bool {
    let Some(origin_val) = headers.get(header::ORIGIN) else {
        return true;
    };
    origin_val.to_str().is_ok_and(|o| {
        o == session.public_origin
            || request_host(headers)
                .is_some_and(|h| o.split_once("://").map(|(_, r)| r) == Some(h.as_str()))
    })
}

fn build_session_set_cookie(
    session_id: &str,
    session: &TakeoverSession,
    headers: &HeaderMap,
) -> Option<HeaderValue> {
    let secure = if session.secure_cookie || forwarded_proto(headers) == Some("https") {
        "; Secure"
    } else {
        ""
    };
    HeaderValue::from_str(&format!(
        "{COOKIE_PREFIX}{session_id}={}; Path={}; HttpOnly; SameSite=Lax{secure}",
        session.token, session.cookie_path
    ))
    .ok()
}

type ResolvedViewer = (
    Arc<TakeoverRegistry>,
    Arc<TakeoverSession>,
    Option<HeaderValue>,
);

async fn resolve_and_auth_viewer(
    state: &AppState,
    session_id: &str,
    headers: &HeaderMap,
    uri: &Uri,
) -> Result<ResolvedViewer, (StatusCode, &'static str)> {
    let not_found = (StatusCode::NOT_FOUND, "takeover session not found");
    if !is_valid_takeover_session_id(session_id) {
        return Err(not_found);
    }
    let registry = registry_for_store(&state.store);
    let session = registry.get_active(session_id).await.ok_or(not_found)?;
    let auth = authenticate_viewer(session_id, &session.token, headers, uri)
        .ok_or((StatusCode::UNAUTHORIZED, "valid takeover token required"))?;
    if *session.returned.borrow() {
        return Err(not_found);
    }
    if let Some(pin) = &session.pin {
        match pin.check().await {
            Ok(()) => {}
            Err(EXPIRED) => return Err((StatusCode::GONE, "takeover lease expired")),
            Err(ENDED) => {
                recovery::end_link(session.store.upgrade(), session_id, &session.token).await;
                return Err(not_found);
            }
            Err(_) => {
                return Err((
                    StatusCode::SERVICE_UNAVAILABLE,
                    "cannot verify takeover lease",
                ));
            }
        }
    }
    let mint = matches!(auth, ViewerAuth::QueryToken)
        .then(|| build_session_set_cookie(session_id, &session, headers))
        .flatten();
    Ok((registry, session, mint))
}

async fn viewer_no_slash_route(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
    req: Request,
) -> Response {
    let (_, session, mint_cookie) =
        match resolve_and_auth_viewer(&state, &session_id, req.headers(), req.uri()).await {
            Ok(v) => v,
            Err(error) => {
                return recovery::viewer_error(
                    &state,
                    &session_id,
                    req.method(),
                    req.headers(),
                    req.uri(),
                    error,
                    req.extensions().get().copied().unwrap_or_default(),
                )
                .await;
            }
        };
    let mut location = session.cookie_path.clone();
    if let Some(q) = req.uri().query().filter(|q| !q.is_empty()) {
        location.push('?');
        location.push_str(q);
    }
    let mut resp = Response::builder()
        .status(StatusCode::FOUND)
        .header(header::LOCATION, location)
        .header(header::CACHE_CONTROL, "no-store")
        .header(header::REFERRER_POLICY, "no-referrer")
        .body(Body::empty())
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
    if let Some(cookie) = mint_cookie {
        resp.headers_mut().append(header::SET_COOKIE, cookie);
    }
    resp
}

async fn viewer_root_route(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
    req: Request,
) -> Response {
    handle_viewer_request(state, session_id, "/".to_string(), req).await
}

async fn viewer_subpath_route(
    State(state): State<AppState>,
    AxumPath((session_id, rest)): AxumPath<(String, String)>,
    req: Request,
) -> Response {
    if rest.contains("..") || rest.contains('\\') || rest.contains('\0') {
        return json_error_response(StatusCode::BAD_REQUEST, "invalid takeover subpath");
    }
    handle_viewer_request(
        state,
        session_id,
        format!("/{}", rest.trim_start_matches('/')),
        req,
    )
    .await
}

fn is_hop_by_hop(name: &str) -> bool {
    matches!(
        name,
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

fn filter_viewer_request_headers(headers: &HeaderMap) -> Vec<(String, String)> {
    const STRIP: &[&str] = &[
        "host",
        "content-length",
        "authorization",
        "cookie",
        "origin",
        "sec-websocket-key",
        "sec-websocket-version",
        "sec-websocket-extensions",
        "sec-websocket-protocol",
    ];
    headers
        .iter()
        .filter_map(|(name, val)| {
            let k = name.as_str().to_ascii_lowercase();
            let skip = is_hop_by_hop(&k)
                || STRIP.contains(&k.as_str())
                || k.starts_with("x-anyagents-")
                || k.starts_with("x-forwarded-");
            (!skip)
                .then(|| val.to_str().ok().map(|s| (k, s.to_string())))
                .flatten()
        })
        .collect()
}

async fn handle_viewer_request(
    state: AppState,
    session_id: String,
    subpath: String,
    req: Request,
) -> Response {
    let (mut parts, body) = req.into_parts();
    let (registry, session, mint_cookie) =
        match resolve_and_auth_viewer(&state, &session_id, &parts.headers, &parts.uri).await {
            Ok(v) => v,
            Err(error) if subpath == "/" => {
                return recovery::viewer_error(
                    &state,
                    &session_id,
                    &parts.method,
                    &parts.headers,
                    &parts.uri,
                    error,
                    parts.extensions.get().copied().unwrap_or_default(),
                )
                .await;
            }
            Err((st, msg)) => return json_error_response(st, msg),
        };
    if !check_viewer_origin(&parts.headers, &session) {
        return json_error_response(
            StatusCode::FORBIDDEN,
            "cross-origin takeover request denied",
        );
    }
    let root_document = parts.method == axum::http::Method::GET
        && subpath == "/"
        && matches!(session.owner, TunnelOwner::Agent(_));
    // Keep ownership changes ordered with the Coordinator handback action;
    // never hold this lock for a streaming RFB or SSE request.
    let _control = if parts.method == axum::http::Method::POST
        && matches!(subpath.as_str(), "/take" | "/release")
    {
        let guard = session.control.lock().await;
        if *guard {
            return recovery::no_store(json_error_response(
                StatusCode::GONE,
                "desktop handed back",
            ));
        }
        Some(guard)
    } else {
        None
    };
    let query = parts.uri.query().map(str::to_string);
    let headers = filter_viewer_request_headers(&parts.headers);
    if let Ok(ws_upgrade) = WebSocketUpgrade::from_request_parts(&mut parts, &state).await {
        return proxy_viewer_ws(
            ws_upgrade,
            registry,
            session.clone(),
            subpath,
            query,
            headers,
            mint_cookie,
        )
        .await;
    }
    let stop_events = (subpath == "/events")
        .then(|| parts.extensions.get::<Stopping>().cloned())
        .flatten();
    let response = proxy_viewer_http(
        registry,
        session.clone(),
        parts.method.as_str().to_string(),
        subpath,
        query,
        headers,
        body,
        mint_cookie,
    )
    .await;
    let response = if root_document {
        viewer::add_controls(response).await
    } else {
        response
    };
    // Dropping the event body on shutdown also cancels the guest request.
    Stopping::close_event_stream(response, stop_events)
}

#[cfg(test)]
mod tests;
