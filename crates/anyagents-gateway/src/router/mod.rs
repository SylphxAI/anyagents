//! Harness-facing HTTP routes.
//!
//! `POST /v1/messages` and `POST /v1/chat/completions` authenticate a per-agent
//! bearer token, then forward to that route's provider. Matching protocols pass
//! the body through with the model replaced. Anything else is translated. The
//! provider is authenticated with the stored key only.

mod forward;

#[cfg(test)]
mod tests;

use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use reqwest::Client;

use crate::error::Fail;
use crate::route::{Protocol, ProviderRoute, RouteResolver};
use crate::usage::{TokenUsage, UsageEvent, UsageSink};

pub(super) struct App<R, U> {
    routes: R,
    usage: U,
    http: Client,
}

/// Routes `POST /v1/messages` and `POST /v1/chat/completions`. A route table
/// that is cheap to share (such as [`crate::MemoryRoutes`]) carries changes made
/// after the gateway started.
pub fn router<R, U>(routes: R, usage: U) -> Router
where
    R: RouteResolver + 'static,
    U: UsageSink + 'static,
{
    let http = Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("http client");
    let state = Arc::new(App {
        routes,
        usage,
        http,
    });
    Router::new()
        .route("/v1/messages", post(on_messages::<R, U>))
        .route("/v1/chat/completions", post(on_chat::<R, U>))
        .with_state(state)
}

async fn on_messages<R, U>(
    State(app): State<Arc<App<R, U>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response
where
    R: RouteResolver + 'static,
    U: UsageSink + 'static,
{
    dispatch(app, Protocol::AnthropicMessages, headers, body).await
}

async fn on_chat<R, U>(
    State(app): State<Arc<App<R, U>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response
where
    R: RouteResolver + 'static,
    U: UsageSink + 'static,
{
    dispatch(app, Protocol::OpenAiChat, headers, body).await
}

async fn dispatch<R, U>(
    app: Arc<App<R, U>>,
    inbound: Protocol,
    headers: HeaderMap,
    body: Bytes,
) -> Response
where
    R: RouteResolver + 'static,
    U: UsageSink + 'static,
{
    let token = match bearer(&headers) {
        Ok(token) => token,
        Err(err) => return err.into_response(),
    };
    let Some(route) = app.routes.resolve(&token) else {
        return Fail::Unauthorized.into_response();
    };
    match forward::forward(&app, route, inbound, &headers, &body).await {
        Ok(response) => response,
        Err(err) => err.into_response(),
    }
}

fn bearer(headers: &HeaderMap) -> Result<String, Fail> {
    let Some(value) = headers.get(header::AUTHORIZATION) else {
        return Err(Fail::Unauthorized);
    };
    let Ok(text) = value.to_str() else {
        return Err(Fail::Unauthorized);
    };
    let Some((scheme, token)) = text.split_once(char::is_whitespace) else {
        return Err(Fail::Unauthorized);
    };
    if !scheme.eq_ignore_ascii_case("bearer") {
        return Err(Fail::Unauthorized);
    }
    let token = token.trim();
    if token.is_empty() || token.contains(char::is_whitespace) {
        return Err(Fail::Unauthorized);
    }
    Ok(token.to_string())
}

pub(super) fn record<U: UsageSink>(usage: &U, route: &ProviderRoute, counts: TokenUsage) {
    usage.record(UsageEvent {
        agent: route.agent.clone(),
        provider: route.provider.clone(),
        model: route.model.clone(),
        input_tokens: counts.input,
        output_tokens: counts.output,
        cache_read_tokens: counts.cache_read,
        cache_write_tokens: counts.cache_write,
    });
}

pub(super) fn json_response(status: StatusCode, bytes: impl Into<Bytes>) -> Response {
    axum::response::Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(axum::body::Body::from(bytes.into()))
        .expect("response")
}
