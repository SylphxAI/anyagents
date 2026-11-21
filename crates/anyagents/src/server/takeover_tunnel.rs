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
use tokio::sync::{Mutex, Notify, RwLock, mpsc, oneshot};

use crate::auth::{Principal, is_valid_takeover_session_id};

use super::in_flight::Stopping;
use super::serverless;
use super::types::AppState;

mod guest_frames;
mod lease;
use guest_frames::{handle_guest_binary_frame, handle_guest_control_frame};
use lease::{ENDED, EXPIRED, LeasePin, RegisterError};

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
    token: String,
    public_origin: String,
    public_base_url: String,
    cookie_path: String,
    secure_cookie: bool,
    pin: Option<LeasePin>,
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
        let pin = LeasePin::register(store, &owner, session_id, &token, reconnect).await?;
        let entry = Arc::new(TakeoverSession {
            owner,
            token,
            public_origin: public.origin,
            public_base_url: public.base_url,
            cookie_path: public.cookie_path,
            secure_cookie: public.secure,
            pin,
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
pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/v1/takeover/{session_id}/tunnel", get(tunnel_ws_route))
        .route("/takeover/{session_id}", any(viewer_no_slash_route))
        .route("/takeover/{session_id}/", any(viewer_root_route))
        .route("/takeover/{session_id}/{*rest}", any(viewer_subpath_route))
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
    let mut unregistered = false;
    let mut terminal = None;
    loop {
        tokio::select! {
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
    if let Some(pin) = &session.pin {
        match pin.check().await {
            Ok(()) => {}
            Err(EXPIRED) => return Err((StatusCode::GONE, "takeover lease expired")),
            Err(ENDED) => return Err(not_found),
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
            Err((st, msg)) => return json_error_response(st, msg),
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
            Err((st, msg)) => return json_error_response(st, msg),
        };
    if !check_viewer_origin(&parts.headers, &session) {
        return json_error_response(
            StatusCode::FORBIDDEN,
            "cross-origin takeover request denied",
        );
    }
    let query = parts.uri.query().map(str::to_string);
    let headers = filter_viewer_request_headers(&parts.headers);
    if let Ok(ws_upgrade) = WebSocketUpgrade::from_request_parts(&mut parts, &state).await {
        return proxy_viewer_ws(
            ws_upgrade,
            registry,
            session,
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
        session,
        parts.method.as_str().to_string(),
        subpath,
        query,
        headers,
        body,
        mint_cookie,
    )
    .await;
    // Dropping the event body on shutdown also cancels the guest request.
    Stopping::close_event_stream(response, stop_events)
}

fn try_send_control(tunnel_tx: &mpsc::Sender<OutboundTunnelMsg>, frame: &ControlFrame) {
    if let Ok(msg) = OutboundTunnelMsg::control(frame) {
        let _ = tunnel_tx.try_send(msg);
    }
}

async fn send_control_or_err(
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

async fn proxy_viewer_ws(
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
async fn proxy_viewer_http(
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

#[cfg(test)]
mod tests {
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
            store,
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
    ) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>
    {
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
        let (st, _, _) =
            http_call(fx.addr, "GET", &format!("/takeover/{session_id}/"), &[], "").await;
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
}
