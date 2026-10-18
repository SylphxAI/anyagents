use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::Response;
use axum::routing::post;
use futures::StreamExt;
use serde_json::{Value, json};
use tokio::net::TcpListener;

use crate::route::{MemoryRoutes, Protocol, ProviderRoute};
use crate::usage::{MemoryUsage, UsageEvent, UsageSink};

use super::router;

const TOKEN: &str = "gateway-token";
const STORED: &str = "stored-key";
const MODEL: &str = "route-model";

#[derive(Clone)]
struct Shared(Arc<MemoryUsage>);

impl UsageSink for Shared {
    fn record(&self, event: UsageEvent) {
        self.0.record(event);
    }
}

#[derive(Clone)]
struct Seen {
    path: String,
    headers: HeaderMap,
    body: Bytes,
}

struct Mock {
    seen: Mutex<Vec<Seen>>,
    reply: Reply,
}

enum Reply {
    Auto {
        json_body: String,
        sse_body: String,
    },
    Gate {
        first: String,
        rest: String,
        release: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    },
}

struct Gate {
    first: String,
    rest: String,
    rx: Option<tokio::sync::oneshot::Receiver<()>>,
    step: u8,
}

struct Running {
    base: String,
    usage: Arc<MemoryUsage>,
    seen: Arc<Mock>,
}

async fn serve(app: Router) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    format!("http://{addr}")
}

fn provider_route(base: &str, protocol: Protocol) -> ProviderRoute {
    let provider = match protocol {
        Protocol::AnthropicMessages => "anthropic",
        Protocol::OpenAiChat | Protocol::OpenAiResponses => "openai",
    };
    ProviderRoute {
        agent: "ada".into(),
        provider: provider.into(),
        protocol,
        base_url: base.into(),
        model: MODEL.into(),
        api_key: STORED.into(),
    }
}

async fn start(protocol: Protocol, reply: Reply) -> Running {
    let seen = Arc::new(Mock {
        seen: Mutex::new(Vec::new()),
        reply,
    });
    let provider = serve(mock_router(Arc::clone(&seen))).await;
    let routes = MemoryRoutes::new();
    routes.insert(TOKEN, provider_route(&provider, protocol));
    let usage = Arc::new(MemoryUsage::new());
    let base = serve(router(routes, Shared(Arc::clone(&usage)))).await;
    Running { base, usage, seen }
}

fn mock_router(mock: Arc<Mock>) -> Router {
    Router::new()
        .route("/v1/messages", post(hit_messages))
        .route("/v1/chat/completions", post(hit_chat))
        .route("/v1/responses", post(hit_responses))
        .with_state(mock)
}

async fn hit_messages(State(mock): State<Arc<Mock>>, headers: HeaderMap, body: Bytes) -> Response {
    answer(&mock, "/v1/messages", headers, body)
}

async fn hit_chat(State(mock): State<Arc<Mock>>, headers: HeaderMap, body: Bytes) -> Response {
    answer(&mock, "/v1/chat/completions", headers, body)
}

async fn hit_responses(State(mock): State<Arc<Mock>>, headers: HeaderMap, body: Bytes) -> Response {
    answer(&mock, "/v1/responses", headers, body)
}

fn answer(mock: &Mock, path: &str, headers: HeaderMap, body: Bytes) -> Response {
    mock.seen.lock().expect("seen").push(Seen {
        path: path.to_string(),
        headers,
        body: body.clone(),
    });
    let value: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    let stream = value
        .get("stream")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    match &mock.reply {
        Reply::Auto {
            json_body,
            sse_body,
        } => {
            if stream {
                typed(StatusCode::OK, "text/event-stream", sse_body.clone())
            } else {
                typed(StatusCode::OK, "application/json", json_body.clone())
            }
        }
        Reply::Gate {
            first,
            rest,
            release,
        } => {
            let rx = release.lock().expect("gate").take().expect("one stream");
            let state = Gate {
                first: first.clone(),
                rest: rest.clone(),
                rx: Some(rx),
                step: 0,
            };
            let stream = futures::stream::unfold(state, |mut state| async move {
                match state.step {
                    0 => {
                        state.step = 1;
                        Some((
                            Ok::<Bytes, std::io::Error>(Bytes::from(state.first.clone())),
                            state,
                        ))
                    }
                    1 => {
                        if let Some(rx) = state.rx.take() {
                            let _ = rx.await;
                        }
                        state.step = 2;
                        Some((Ok(Bytes::from(state.rest.clone())), state))
                    }
                    _ => None,
                }
            });
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "text/event-stream")
                .body(Body::from_stream(stream))
                .expect("stream")
        }
    }
}

fn typed(status: StatusCode, content_type: &str, body: String) -> Response {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, content_type)
        .body(Body::from(body))
        .expect("response")
}

async fn send(base: &str, path: &str, auth: Option<&str>, body: Value) -> reqwest::Response {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("client");
    let mut request = client
        .post(format!("{base}{path}"))
        .header("x-api-key", TOKEN)
        .header("cookie", format!("session={TOKEN}"))
        .header("anthropic-version", "2023-06-01")
        .json(&body);
    if let Some(auth) = auth {
        request = request.header(header::AUTHORIZATION, auth);
    }
    request.send().await.expect("send")
}

fn bearer() -> String {
    format!("Bearer {TOKEN}")
}

fn assert_provider_auth(headers: &HeaderMap, protocol: Protocol) {
    let blob = headers
        .iter()
        .map(|(name, value)| format!("{name}: {}", value.to_str().unwrap_or("")))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(!blob.contains(TOKEN), "{blob}");
    assert!(headers.get(header::COOKIE).is_none(), "{blob}");
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    assert!(content_type.contains("application/json"), "{blob}");
    match protocol {
        Protocol::AnthropicMessages => {
            assert_eq!(
                headers
                    .get("x-api-key")
                    .and_then(|value| value.to_str().ok()),
                Some(STORED),
                "{blob}"
            );
            assert!(headers.get(header::AUTHORIZATION).is_none(), "{blob}");
            assert_eq!(
                headers
                    .get("anthropic-version")
                    .and_then(|value| value.to_str().ok()),
                Some("2023-06-01"),
                "{blob}"
            );
        }
        Protocol::OpenAiChat | Protocol::OpenAiResponses => {
            assert_eq!(
                headers
                    .get(header::AUTHORIZATION)
                    .and_then(|value| value.to_str().ok()),
                Some(format!("Bearer {STORED}")).as_deref(),
                "{blob}"
            );
            assert!(headers.get("x-api-key").is_none(), "{blob}");
            assert!(headers.get("anthropic-version").is_none(), "{blob}");
        }
    }
}

fn assert_usage(
    usage: &MemoryUsage,
    provider: &str,
    input: u64,
    output: u64,
    cache_read: u64,
    cache_write: u64,
) {
    assert_eq!(
        usage.events(),
        vec![UsageEvent {
            agent: "ada".into(),
            provider: provider.into(),
            model: MODEL.into(),
            input_tokens: input,
            output_tokens: output,
            cache_read_tokens: cache_read,
            cache_write_tokens: cache_write,
        }]
    );
}

fn only_seen(mock: &Mock) -> Seen {
    let seen = mock.seen.lock().expect("seen").clone();
    assert_eq!(seen.len(), 1, "provider calls");
    seen.into_iter().next().expect("hit")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_or_unknown_token_is_401() {
    let running = start(
        Protocol::AnthropicMessages,
        Reply::Auto {
            json_body: "{}".into(),
            sse_body: String::new(),
        },
    )
    .await;
    for (auth, path) in [
        (None, "/v1/messages"),
        (Some("Bearer nope"), "/v1/chat/completions"),
        (Some("Bearer "), "/v1/messages"),
        (Some("Token gateway-token"), "/v1/chat/completions"),
    ] {
        let response = send(&running.base, path, auth, json!({"model": "x"})).await;
        assert_eq!(
            response.status(),
            StatusCode::UNAUTHORIZED,
            "{auth:?} {path}"
        );
    }
    assert!(running.seen.seen.lock().expect("seen").is_empty());
    assert!(running.usage.events().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_protocol_passes_the_body_through() {
    let provider_body = serde_json::to_string(&json!({
        "id": "msg_1",
        "type": "message",
        "role": "assistant",
        "model": "provider-model",
        "content": [{"type": "text", "text": "Hi"}],
        "stop_reason": "end_turn",
        "usage": {
            "input_tokens": 5,
            "output_tokens": 6,
            "cache_read_input_tokens": 7,
            "cache_creation_input_tokens": 8
        }
    }))
    .expect("json");
    let running = start(
        Protocol::AnthropicMessages,
        Reply::Auto {
            json_body: provider_body.clone(),
            sse_body: String::new(),
        },
    )
    .await;
    let response = send(
        &running.base,
        "/v1/messages",
        Some(&bearer()),
        json!({
            "model": "client-model",
            "max_tokens": 16,
            "metadata": {"user_id": "abc"},
            "messages": [{"role": "user", "content": "Hello"}]
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let text = response.text().await.expect("body");
    assert_eq!(text, provider_body);

    let seen = only_seen(&running.seen);
    assert_eq!(seen.path, "/v1/messages");
    assert_provider_auth(&seen.headers, Protocol::AnthropicMessages);
    let sent: Value = serde_json::from_slice(&seen.body).expect("json");
    assert_eq!(sent["model"], MODEL);
    assert_eq!(sent["metadata"]["user_id"], "abc");
    assert_eq!(sent["messages"][0]["content"], "Hello");
    assert_eq!(sent["max_tokens"], 16);
    assert_usage(&running.usage, "anthropic", 5, 6, 7, 8);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn chat_pass_through_replaces_only_the_model() {
    let provider_body = serde_json::to_string(&json!({
        "id": "chatcmpl-pass",
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": "Hi"},
            "finish_reason": "stop"
        }],
        "usage": {
            "prompt_tokens": 3,
            "completion_tokens": 4,
            "prompt_tokens_details": {"cached_tokens": 1}
        }
    }))
    .expect("json");
    let running = start(
        Protocol::OpenAiChat,
        Reply::Auto {
            json_body: provider_body.clone(),
            sse_body: String::new(),
        },
    )
    .await;
    let response = send(
        &running.base,
        "/v1/chat/completions",
        Some(&bearer()),
        json!({
            "model": "client-model",
            "user": "keep-me",
            "messages": [{"role": "user", "content": "Hello"}]
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.text().await.expect("body"), provider_body);
    let seen = only_seen(&running.seen);
    assert_eq!(seen.path, "/v1/chat/completions");
    assert_provider_auth(&seen.headers, Protocol::OpenAiChat);
    let sent: Value = serde_json::from_slice(&seen.body).expect("json");
    assert_eq!(sent["model"], MODEL);
    assert_eq!(sent["user"], "keep-me");
    assert_eq!(sent["messages"][0]["content"], "Hello");
    assert_usage(&running.usage, "openai", 3, 4, 1, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn anthropic_to_openai_translates_the_turn() {
    let provider_body = serde_json::to_string(&json!({
        "id": "chatcmpl-abc",
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": "Hello"},
            "finish_reason": "stop"
        }],
        "usage": {
            "prompt_tokens": 11,
            "completion_tokens": 4,
            "total_tokens": 15,
            "prompt_tokens_details": {"cached_tokens": 2}
        }
    }))
    .expect("json");
    let running = start(
        Protocol::OpenAiChat,
        Reply::Auto {
            json_body: provider_body,
            sse_body: String::new(),
        },
    )
    .await;
    let response = send(
        &running.base,
        "/v1/messages",
        Some(&bearer()),
        json!({
            "model": "client-model",
            "max_tokens": 32,
            "messages": [{"role": "user", "content": "Hello"}]
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value = response.json().await.expect("json");
    assert_eq!(body["type"], "message");
    assert_eq!(body["role"], "assistant");
    assert_eq!(body["model"], MODEL);
    assert_eq!(body["content"][0]["text"], "Hello");
    assert_eq!(body["stop_reason"], "end_turn");
    assert_eq!(body["usage"]["input_tokens"], 11);
    assert_eq!(body["usage"]["output_tokens"], 4);
    assert_eq!(body["usage"]["cache_read_input_tokens"], 2);
    assert_eq!(body["id"], "msg_chatcmpl-abc");

    let seen = only_seen(&running.seen);
    assert_eq!(seen.path, "/v1/chat/completions");
    assert_provider_auth(&seen.headers, Protocol::OpenAiChat);
    let sent: Value = serde_json::from_slice(&seen.body).expect("json");
    assert_eq!(sent["model"], MODEL);
    assert_eq!(sent["max_tokens"], 32);
    assert_eq!(sent["stream"], false);
    assert_eq!(sent["messages"][0]["role"], "user");
    assert_eq!(sent["messages"][0]["content"], "Hello");
    assert!(sent.get("stream_options").is_none());
    assert_usage(&running.usage, "openai", 11, 4, 2, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn anthropic_to_openai_streams_without_buffering() {
    let (first, rest) = chat_stream_parts();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let running = start(
        Protocol::OpenAiChat,
        Reply::Gate {
            first,
            rest,
            release: Mutex::new(Some(rx)),
        },
    )
    .await;
    let client = reqwest::Client::builder().build().expect("client");
    let response = tokio::time::timeout(
        Duration::from_secs(3),
        client
            .post(format!("{}/v1/messages", running.base))
            .header(header::AUTHORIZATION, bearer())
            .header("x-api-key", TOKEN)
            .header("cookie", format!("session={TOKEN}"))
            .json(&json!({
                "model": "client-model",
                "max_tokens": 32,
                "stream": true,
                "messages": [{"role": "user", "content": "Hello"}]
            }))
            .send(),
    )
    .await
    .expect("headers timed out; the response was buffered")
    .expect("send");
    assert_eq!(response.status(), StatusCode::OK);
    let mut stream = response.bytes_stream();
    let first = tokio::time::timeout(Duration::from_secs(3), stream.next())
        .await
        .expect("first chunk timed out; the response was buffered")
        .expect("ended")
        .expect("chunk");
    let first_text = String::from_utf8_lossy(&first);
    assert!(first_text.contains("message_start"), "{first_text}");
    assert!(!first_text.contains("Hello"), "{first_text}");
    tx.send(()).expect("release");
    let mut all = first.to_vec();
    let rest = tokio::time::timeout(Duration::from_secs(3), async {
        let mut buf = Vec::new();
        while let Some(chunk) = stream.next().await {
            buf.extend_from_slice(&chunk.expect("chunk"));
        }
        buf
    })
    .await
    .expect("rest timed out");
    all.extend_from_slice(&rest);
    let text = String::from_utf8_lossy(&all);
    assert!(text.contains("Hello"), "{text}");
    assert!(text.contains(" there"), "{text}");
    assert!(text.contains("message_stop"), "{text}");

    let seen = only_seen(&running.seen);
    assert_eq!(seen.path, "/v1/chat/completions");
    assert_provider_auth(&seen.headers, Protocol::OpenAiChat);
    let sent: Value = serde_json::from_slice(&seen.body).expect("json");
    assert_eq!(sent["model"], MODEL);
    assert_eq!(sent["stream"], true);
    assert_eq!(sent["stream_options"]["include_usage"], true);
    assert_eq!(sent["messages"][0]["content"], "Hello");
    assert_usage(&running.usage, "openai", 11, 4, 2, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_protocol_stream_is_forwarded_and_counted() {
    let sse = anthropic_sse();
    let running = start(
        Protocol::AnthropicMessages,
        Reply::Auto {
            json_body: String::new(),
            sse_body: sse.clone(),
        },
    )
    .await;
    let response = send(
        &running.base,
        "/v1/messages",
        Some(&bearer()),
        json!({
            "model": "client-model",
            "max_tokens": 16,
            "stream": true,
            "messages": [{"role": "user", "content": "Hello"}]
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.text().await.expect("body"), sse);
    let seen = only_seen(&running.seen);
    assert_eq!(seen.path, "/v1/messages");
    assert_provider_auth(&seen.headers, Protocol::AnthropicMessages);
    let sent: Value = serde_json::from_slice(&seen.body).expect("json");
    assert_eq!(sent["model"], MODEL);
    assert_eq!(sent["stream"], true);
    assert_usage(&running.usage, "anthropic", 5, 6, 7, 8);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unsupported_target_feature_is_400_named() {
    let running = start(
        Protocol::OpenAiResponses,
        Reply::Auto {
            json_body: "{}".into(),
            sse_body: String::new(),
        },
    )
    .await;
    let response = send(
        &running.base,
        "/v1/messages",
        Some(&bearer()),
        json!({
            "model": "client-model",
            "max_tokens": 8,
            "stop_sequences": ["END"],
            "messages": [{"role": "user", "content": "Hi"}]
        }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body: Value = response.json().await.expect("json");
    let message = body["error"]["message"].as_str().expect("message");
    assert!(message.contains("stop_sequences"), "{message}");
    assert!(running.seen.seen.lock().expect("seen").is_empty());
    assert!(running.usage.events().is_empty());
}

fn chat_stream_parts() -> (String, String) {
    let first = chat_chunk(
        json!({"role": "assistant", "content": ""}),
        Value::Null,
        None,
    );
    let mut rest = String::new();
    rest.push_str(&chat_chunk(json!({"content": "Hello"}), Value::Null, None));
    rest.push_str(&chat_chunk(json!({"content": " there"}), Value::Null, None));
    rest.push_str(&chat_chunk(
        json!({}),
        json!("stop"),
        Some(json!({
            "prompt_tokens": 11,
            "completion_tokens": 4,
            "total_tokens": 15,
            "prompt_tokens_details": {"cached_tokens": 2}
        })),
    ));
    rest.push_str("data: [DONE]\n\n");
    (first, rest)
}

fn chat_chunk(delta: Value, finish: Value, usage: Option<Value>) -> String {
    let mut body = json!({
        "id": "chatcmpl-abc",
        "object": "chat.completion.chunk",
        "created": 1710000000,
        "model": "gpt-src",
        "choices": [{"index": 0, "delta": delta, "finish_reason": finish}]
    });
    if let Some(usage) = usage {
        body["usage"] = usage;
    }
    format!("data: {}\n\n", serde_json::to_string(&body).expect("json"))
}

fn anthropic_sse() -> String {
    let mut raw = String::new();
    raw.push_str(&named(
        "message_start",
        json!({
            "type": "message_start",
            "message": {
                "usage": {
                    "input_tokens": 5,
                    "output_tokens": 1,
                    "cache_read_input_tokens": 7,
                    "cache_creation_input_tokens": 8
                }
            }
        }),
    ));
    raw.push_str(&named(
        "message_delta",
        json!({
            "type": "message_delta",
            "usage": {"output_tokens": 6}
        }),
    ));
    raw.push_str(&named("message_stop", json!({"type": "message_stop"})));
    raw
}

fn named(event: &str, value: Value) -> String {
    format!(
        "event: {event}\ndata: {}\n\n",
        serde_json::to_string(&value).expect("json")
    )
}
