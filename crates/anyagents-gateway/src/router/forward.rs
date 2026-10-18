use std::pin::Pin;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use axum::response::Response;
use bytes::Bytes;
use futures::StreamExt;
use serde_json::{Value, json};

use crate::canonical::{encode_request, encode_response, parse_request, parse_response};
use crate::error::Fail;
use crate::route::{Protocol, ProviderRoute};
use crate::sse::ByteSse;
use crate::stream::StreamX;
use crate::usage::{TokenUsage, UsageSink, patch_anthropic, patch_chat, patch_responses};

use super::{App, json_response, record};

pub(super) async fn forward<R, U>(
    app: &Arc<App<R, U>>,
    route: ProviderRoute,
    inbound: Protocol,
    inbound_headers: &HeaderMap,
    body: &[u8],
) -> Result<Response, Fail>
where
    R: Send + Sync + 'static,
    U: UsageSink + 'static,
{
    let payload: Value = serde_json::from_slice(body).map_err(|_| Fail::invalid("json"))?;
    let stream = stream_flag(&payload)?;
    if stream && inbound != route.protocol {
        // Reject a pair that cannot stream before the provider is called.
        StreamX::between(route.protocol, inbound, &route.model)?;
    }
    let outbound = if inbound == route.protocol {
        let mut payload = payload;
        set_model(&mut payload, &route.model)?;
        serde_json::to_vec(&payload).map_err(|_| Fail::invalid("json"))?
    } else {
        let request = parse_request(inbound, &payload)?;
        let encoded = encode_request(route.protocol, &request, &route.model, stream)?;
        serde_json::to_vec(&encoded).map_err(|_| Fail::invalid("json"))?
    };
    let response = app
        .http
        .post(endpoint(&route.base_url, route.protocol))
        .body(outbound)
        .headers(provider_headers(&route, inbound_headers)?)
        .send()
        .await
        .map_err(|_| Fail::Upstream)?;
    adapt(Arc::clone(app), route, inbound, stream, response).await
}

fn stream_flag(value: &Value) -> Result<bool, Fail> {
    match value.get("stream") {
        None | Some(Value::Null) => Ok(false),
        Some(Value::Bool(flag)) => Ok(*flag),
        Some(_) => Err(Fail::invalid("stream")),
    }
}

fn set_model(value: &mut Value, model: &str) -> Result<(), Fail> {
    let obj = value
        .as_object_mut()
        .ok_or_else(|| Fail::invalid("request"))?;
    obj.insert("model".to_string(), json!(model));
    Ok(())
}

fn endpoint(base: &str, protocol: Protocol) -> String {
    let base = base.trim_end_matches('/');
    let path = match protocol {
        Protocol::AnthropicMessages => "/v1/messages",
        Protocol::OpenAiChat => "/v1/chat/completions",
        Protocol::OpenAiResponses => "/v1/responses",
    };
    format!("{base}{path}")
}

/// Provider auth is the stored key and nothing else. Inbound `Authorization`,
/// `x-api-key` and `Cookie` are never copied.
fn provider_headers(route: &ProviderRoute, inbound: &HeaderMap) -> Result<HeaderMap, Fail> {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    match route.protocol {
        Protocol::AnthropicMessages => {
            headers.insert(
                HeaderName::from_static("x-api-key"),
                header_value(&route.api_key)?,
            );
            copy_header(inbound, &mut headers, "anthropic-version");
            copy_header(inbound, &mut headers, "anthropic-beta");
        }
        Protocol::OpenAiChat | Protocol::OpenAiResponses => {
            headers.insert(
                header::AUTHORIZATION,
                header_value(&format!("Bearer {}", route.api_key))?,
            );
        }
    }
    Ok(headers)
}

fn header_value(value: &str) -> Result<HeaderValue, Fail> {
    HeaderValue::from_str(value).map_err(|_| Fail::invalid("api_key"))
}

fn copy_header(from: &HeaderMap, to: &mut HeaderMap, name: &'static str) {
    if let Some(value) = from.get(name) {
        to.insert(HeaderName::from_static(name), value.clone());
    }
}

async fn adapt<R, U>(
    app: Arc<App<R, U>>,
    route: ProviderRoute,
    inbound: Protocol,
    stream: bool,
    response: reqwest::Response,
) -> Result<Response, Fail>
where
    R: Send + Sync + 'static,
    U: UsageSink + 'static,
{
    let status = response.status();
    let streaming = stream && is_event_stream(response.headers(), status);
    if route.protocol == inbound {
        if streaming {
            return Ok(passthrough_stream(app, route, status, response));
        }
        return buffered_pass(&app.usage, &route, response).await;
    }
    if !status.is_success() {
        return Err(Fail::Upstream);
    }
    if streaming {
        return Ok(translated_stream(app, route, inbound, response));
    }
    buffered_translate(&app.usage, &route, inbound, response).await
}

fn is_event_stream(headers: &HeaderMap, status: StatusCode) -> bool {
    if !status.is_success() {
        return false;
    }
    match headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
    {
        Some(value) if value.contains("text/event-stream") => true,
        Some(value) if value.contains("application/json") => false,
        Some(_) => false,
        None => true,
    }
}

async fn buffered_pass<U: UsageSink>(
    usage: &U,
    route: &ProviderRoute,
    response: reqwest::Response,
) -> Result<Response, Fail> {
    let status = response.status();
    let content_type = content_type_of(response.headers());
    let bytes = response.bytes().await.map_err(|_| Fail::Upstream)?;
    record(usage, route, counts_in(route.protocol, &bytes));
    Ok(Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, content_type)
        .body(Body::from(bytes))
        .expect("response"))
}

async fn buffered_translate<U: UsageSink>(
    usage: &U,
    route: &ProviderRoute,
    inbound: Protocol,
    response: reqwest::Response,
) -> Result<Response, Fail> {
    let bytes = response.bytes().await.map_err(|_| Fail::Upstream)?;
    let value: Value = serde_json::from_slice(&bytes).map_err(|_| Fail::Upstream)?;
    let completion = parse_response(route.protocol, &value)?;
    record(usage, route, completion.usage);
    let encoded = encode_response(inbound, &completion, &route.model)?;
    let body = serde_json::to_vec(&encoded).map_err(|_| Fail::Upstream)?;
    Ok(json_response(StatusCode::OK, body))
}

fn content_type_of(headers: &HeaderMap) -> HeaderValue {
    headers
        .get(header::CONTENT_TYPE)
        .cloned()
        .unwrap_or_else(|| HeaderValue::from_static("application/json"))
}

fn counts_in(protocol: Protocol, bytes: &[u8]) -> TokenUsage {
    let Ok(value) = serde_json::from_slice::<Value>(bytes) else {
        return TokenUsage::default();
    };
    let Some(usage) = value.get("usage").filter(|usage| usage.is_object()) else {
        return TokenUsage::default();
    };
    let mut counts = TokenUsage::default();
    counts.apply(match protocol {
        Protocol::AnthropicMessages => patch_anthropic(usage),
        Protocol::OpenAiChat => patch_chat(usage),
        Protocol::OpenAiResponses => patch_responses(usage),
    });
    counts
}

type Upstream = Pin<Box<dyn futures::Stream<Item = Result<Bytes, reqwest::Error>> + Send>>;

fn passthrough_stream<R, U>(
    app: Arc<App<R, U>>,
    route: ProviderRoute,
    status: StatusCode,
    response: reqwest::Response,
) -> Response
where
    R: Send + Sync + 'static,
    U: UsageSink + 'static,
{
    let content_type = content_type_of(response.headers());
    let protocol = route.protocol;
    let stream = scan_stream(app, route, protocol, Box::pin(response.bytes_stream()));
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, content_type)
        .body(Body::from_stream(stream))
        .expect("response")
}

struct ScanState<R, U> {
    incoming: Upstream,
    scan: UsageScan,
    app: Arc<App<R, U>>,
    route: ProviderRoute,
    recorded: bool,
}

fn scan_stream<R, U>(
    app: Arc<App<R, U>>,
    route: ProviderRoute,
    protocol: Protocol,
    incoming: Upstream,
) -> impl futures::Stream<Item = Result<Bytes, std::io::Error>> + Send
where
    R: Send + Sync + 'static,
    U: UsageSink + 'static,
{
    let state = ScanState {
        incoming,
        scan: UsageScan::new(protocol),
        app,
        route,
        recorded: false,
    };
    futures::stream::unfold(state, |mut state| async move {
        if state.recorded {
            return None;
        }
        match state.incoming.next().await {
            Some(Ok(chunk)) => {
                state.scan.push(&chunk);
                Some((Ok(chunk), state))
            }
            Some(Err(_)) => {
                state.scan.finish();
                state.recorded = true;
                record(&state.app.usage, &state.route, state.scan.usage);
                Some((Err(std::io::Error::other("upstream")), state))
            }
            None => {
                state.scan.finish();
                state.recorded = true;
                record(&state.app.usage, &state.route, state.scan.usage);
                None
            }
        }
    })
}

struct UsageScan {
    protocol: Protocol,
    sse: ByteSse,
    usage: TokenUsage,
}

impl UsageScan {
    fn new(protocol: Protocol) -> Self {
        Self {
            protocol,
            sse: ByteSse::default(),
            usage: TokenUsage::default(),
        }
    }

    fn push(&mut self, chunk: &[u8]) {
        for frame in self.sse.push(chunk) {
            self.note(&frame.data);
        }
    }

    fn finish(&mut self) {
        for frame in self.sse.finish() {
            self.note(&frame.data);
        }
    }

    fn note(&mut self, data: &str) {
        if data.trim() == "[DONE]" {
            return;
        }
        let Ok(value) = serde_json::from_str::<Value>(data) else {
            return;
        };
        match self.protocol {
            Protocol::AnthropicMessages => {
                if let Some(usage) = value
                    .pointer("/message/usage")
                    .filter(|usage| usage.is_object())
                {
                    self.usage.apply(patch_anthropic(usage));
                }
                if let Some(usage) = value.get("usage").filter(|usage| usage.is_object()) {
                    self.usage.apply(patch_anthropic(usage));
                }
            }
            Protocol::OpenAiChat => {
                if let Some(usage) = value.get("usage").filter(|usage| usage.is_object()) {
                    self.usage.apply(patch_chat(usage));
                }
            }
            Protocol::OpenAiResponses => {
                if let Some(usage) = value.get("usage").filter(|usage| usage.is_object()) {
                    self.usage.apply(patch_responses(usage));
                }
            }
        }
    }
}

fn translated_stream<R, U>(
    app: Arc<App<R, U>>,
    route: ProviderRoute,
    inbound: Protocol,
    response: reqwest::Response,
) -> Response
where
    R: Send + Sync + 'static,
    U: UsageSink + 'static,
{
    let translator = StreamX::between(route.protocol, inbound, &route.model)
        .expect("stream pair was checked before the provider call");
    let state = TrState {
        incoming: Box::pin(response.bytes_stream()),
        translator,
        phase: Emit::Reading,
        app,
        route,
        inbound,
    };
    let stream = futures::stream::unfold(state, |mut state| async move {
        if matches!(state.phase, Emit::Done) {
            return None;
        }
        loop {
            let step = state.incoming.next().await;
            match step {
                Some(Ok(chunk)) => match state.translator.push(&chunk) {
                    Ok(text) if text.is_empty() => continue,
                    Ok(text) => {
                        return Some((
                            Ok::<Bytes, std::convert::Infallible>(Bytes::from(text)),
                            state,
                        ));
                    }
                    Err(err) => {
                        record(&state.app.usage, &state.route, TokenUsage::default());
                        state.phase = Emit::Done;
                        return Some((Ok(Bytes::from(error_sse(state.inbound, &err))), state));
                    }
                },
                Some(Err(_)) => {
                    record(&state.app.usage, &state.route, TokenUsage::default());
                    state.phase = Emit::Done;
                    let err = Fail::Upstream;
                    return Some((Ok(Bytes::from(error_sse(state.inbound, &err))), state));
                }
                None => {
                    let finished = state.translator.finish();
                    state.phase = Emit::Done;
                    match finished {
                        Ok((tail, counts)) => {
                            record(&state.app.usage, &state.route, counts);
                            if tail.is_empty() {
                                return None;
                            }
                            return Some((Ok(Bytes::from(tail)), state));
                        }
                        Err(err) => {
                            record(&state.app.usage, &state.route, TokenUsage::default());
                            return Some((Ok(Bytes::from(error_sse(state.inbound, &err))), state));
                        }
                    }
                }
            }
        }
    });
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .body(Body::from_stream(stream))
        .expect("response")
}

enum Emit {
    Reading,
    Done,
}

struct TrState<R, U> {
    incoming: Upstream,
    translator: StreamX,
    phase: Emit,
    app: Arc<App<R, U>>,
    route: ProviderRoute,
    inbound: Protocol,
}

fn error_sse(protocol: Protocol, err: &Fail) -> String {
    let body = json!({
        "type": err.error_type(),
        "message": err.to_string(),
    });
    match protocol {
        Protocol::AnthropicMessages => {
            crate::sse::sse_event("error", &json!({"type": "error", "error": body}))
        }
        Protocol::OpenAiChat | Protocol::OpenAiResponses => {
            let mut out = crate::sse::sse_data(&json!({"error": body}));
            out.push_str(&crate::sse::sse_done());
            out
        }
    }
}
