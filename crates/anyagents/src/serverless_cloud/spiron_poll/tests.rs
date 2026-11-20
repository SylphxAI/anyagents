use super::super::*;
use axum::body::{Body, Bytes};
use axum::extract::Query;
use axum::http::{HeaderMap, StatusCode, header};
use axum::routing::get;
use axum::{Json, Router};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

const RECEIPT: &str = "/v1/message-receipts/task-1";
const EVENTS: &str = "/v1/message-receipts/task-1/events";
const FAILURE: &str = "permanent AI provider error (invalid_value): request rejected\n  details";

fn failed_receipt(status: &str, error: Value, failure: Value) -> Value {
    json!({
        "id": "task-1", "object": "message_receipt", "status": status,
        "output": [], "reply": null, "failure": failure, "error": error
    })
}

async fn poll(app: Router, cursor: Option<&str>) -> CloudTurnPollResult {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let driver = SpironServerlessDriver::new(format!("http://{addr}"), None);
    let result = driver.poll_turn("task-1", cursor).await;
    server.abort();
    result.unwrap()
}

#[tokio::test]
async fn completed_receipt_reads_conversation_data_and_legacy_items() {
    for key in ["data", "items"] {
        let app = Router::new()
            .route(RECEIPT, get(|| async { Json(json!({"status":"completed", "agent_id":"a1"})) }))
            .route("/v1/agents/a1/conversation", get(move || async move {
                Json(json!({(key): [
                    {"task_id":"task-1", "role":"agent", "output":{"type":"text", "text":"matched reply"}},
                    {"task_id":"other", "role":"agent", "text":"unrelated"},
                    {"task_id":"task-1", "role":"user", "text":"original prompt"}
                ]}))
            }));
        let result = poll(app, None).await;
        assert_eq!(result.status, CloudTurnState::Completed);
        assert_eq!(result.result_text.as_deref(), Some("matched reply"));
        assert_eq!(result.error_text, None);
    }
}

#[tokio::test]
async fn message_completed_returns_reply_without_waiting_for_sse_eof() {
    let app = Router::new()
        .route(RECEIPT, get(|| async { Json(json!({"status":"working"})) }))
        .route(EVENTS, get(|headers: HeaderMap, Query(query): Query<BTreeMap<String, String>>| async move {
            assert_eq!(headers["last-event-id"], "1");
            assert_eq!(query["after"], "1");
            let stream = futures::stream::once(async {
                Ok::<_, std::io::Error>(Bytes::from_static(b": keepalive\r\n\r\nid: 2\r\nevent: message.completed\r\ndata: {\"task_id\":\"task-1\",\r\ndata: \"output\":{\"type\":\"text\",\"text\":\"streamed reply\"}}\r\n\r\n"))
            });
            let stream = futures::StreamExt::chain(stream, futures::stream::pending());
            ([(header::CONTENT_TYPE, "text/event-stream; charset=utf-8")], Body::from_stream(stream))
        }));
    let result = poll(app, Some("1")).await;
    assert_eq!(result.status, CloudTurnState::Completed);
    assert_eq!(result.result_text.as_deref(), Some("streamed reply"));
    assert_eq!(result.cursor.as_deref(), Some("2"));
    assert_eq!(result.next_poll_after_ms, None);
}

#[tokio::test]
async fn unwrapped_sse_task_failure_is_terminal_and_preserves_text() {
    for (status, state, error, failure, expected) in [
        (
            "failed",
            CloudTurnState::Failed,
            json!(FAILURE),
            json!({"reason":"baas_turn_failed", "message":"do not use this"}),
            FAILURE,
        ),
        (
            "errored",
            CloudTurnState::Failed,
            Value::Null,
            json!({"reason":"baas_turn_failed", "message":FAILURE}),
            FAILURE,
        ),
        (
            "cancelled",
            CloudTurnState::Cancelled,
            Value::Null,
            json!({"reason":"provider_cancelled"}),
            "provider_cancelled",
        ),
    ] {
        let terminal = failed_receipt(status, error, failure).to_string();
        let body = format!(
            "event: task.updated\ndata: {{\"task_id\":\"task-1\",\"status\":\"submitted\"}}\n\nevent: task.updated\ndata: {{\"task_id\":\"task-1\",\"status\":\"working\"}}\n\nid: terminal\nevent: task.updated\ndata: {terminal}\n\n"
        );
        let app = Router::new()
            .route(RECEIPT, get(|| async { Json(json!({"status":"working"})) }))
            .route(
                EVENTS,
                get(move || async move { ([(header::CONTENT_TYPE, "text/event-stream")], body) }),
            );
        let result = poll(app, None).await;
        assert_eq!(result.status, state);
        assert_eq!(result.error_text.as_deref(), Some(expected));
        assert_eq!(result.result_text, None);
        assert_eq!(result.next_poll_after_ms, None);
        assert_eq!(result.cursor.as_deref(), Some("terminal"));
    }
}

#[tokio::test]
async fn durable_failure_returns_exact_text_without_requesting_events() {
    for (error, failure, expected) in [
        (
            json!(FAILURE),
            json!({"reason":"baas_turn_failed", "message":FAILURE}),
            FAILURE,
        ),
        (
            Value::Null,
            json!({"reason":"baas_turn_failed", "message":FAILURE}),
            FAILURE,
        ),
        (
            Value::Null,
            json!({"reason":"baas_turn_failed"}),
            "baas_turn_failed",
        ),
    ] {
        let receipt = failed_receipt("failed", error, failure);
        let events_called = Arc::new(AtomicBool::new(false));
        let called = Arc::clone(&events_called);
        let app = Router::new()
            .route(RECEIPT, get(move || async move { Json(receipt) }))
            .route(
                EVENTS,
                get(move || async move {
                    called.store(true, Ordering::Relaxed);
                    StatusCode::NOT_FOUND
                }),
            );
        let result = poll(app, Some("before")).await;
        assert!(!events_called.load(Ordering::Relaxed));
        assert_eq!(result.status, CloudTurnState::Failed);
        assert_eq!(result.error_text.as_deref(), Some(expected));
        assert_eq!(result.result_text, None);
        assert_eq!(result.next_poll_after_ms, None);
        assert_eq!(result.cursor.as_deref(), Some("before"));
    }
}
