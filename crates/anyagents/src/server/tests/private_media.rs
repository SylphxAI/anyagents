use anyagents_core::{ExecutionRuntime, NewAgent};
use axum::{Router, body::Body, http::Request, response::Response};
use http_body_util::BodyExt as _;
use serde_json::{Value, json};
use tower::ServiceExt as _;

use super::helpers::tool_call;
use super::*;

async fn request(
    app: &Router,
    method: &str,
    uri: &str,
    token: Option<&str>,
    content_type: &str,
    body: impl Into<Body>,
) -> Response {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", content_type);
    if let Some(token) = token {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    app.clone()
        .oneshot(request.body(body.into()).unwrap())
        .await
        .unwrap()
}

async fn json_response(response: Response, expected: StatusCode) -> Value {
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(status, expected, "{}", String::from_utf8_lossy(&bytes));
    serde_json::from_slice(&bytes).unwrap()
}

async fn mcp_send(app: &Router, token: &str, args: Value) -> Value {
    let response = request(
        app,
        "POST",
        "/v1/webhooks/mcp",
        Some(token),
        "application/json",
        tool_call("send", args).to_string(),
    )
    .await;
    json_response(response, StatusCode::OK).await["result"].clone()
}

#[test]
fn private_media_source_accepts_only_an_opaque_id() {
    use super::super::private_media::source_id;
    let id = format!("{}~report.pdf", anyagents_core::MessageId::generate());
    assert_eq!(
        source_id(&format!("media://{id}")).unwrap(),
        Some(id.as_str())
    );
    assert_eq!(source_id("https://example.com/report.pdf").unwrap(), None);
    for invalid in [
        "media://".to_string(),
        format!("media://{id}/extra"),
        format!("media://{id}?query"),
        format!("media://{id}#fragment"),
        format!("media://{id}%2f"),
    ] {
        assert_eq!(
            source_id(&invalid).unwrap_err().status,
            StatusCode::BAD_REQUEST
        );
    }
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn sandbox_upload_round_trips_to_owner_and_internal_recipient() {
    let _env = env_lock(); // S3 tests and sandbox placement tests set process env.
    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    let owner = state
        .store
        .create_agent(
            NewAgent::new("media-owner", "codex")
                .with_runtime(ExecutionRuntime::Sandbox { provider: None }),
        )
        .unwrap();
    let peer = state
        .store
        .create_agent(NewAgent {
            parent_id: Some(owner.id.clone()),
            ..NewAgent::new("media-peer", "codex")
                .with_runtime(ExecutionRuntime::Sandbox { provider: None })
        })
        .unwrap();
    state.store.set_agent_state(&peer.id, "running").unwrap();
    let owner_token = state
        .store
        .begin_remote_session(&owner.id, 60_000)
        .unwrap()
        .token;
    let peer_token = state
        .store
        .begin_remote_session(&peer.id, 60_000)
        .unwrap()
        .token;
    let app = build_public_router(state.clone());
    let path = "/v1/media?filename=report.pdf";
    let payload = b"%PDF-1.7\nfixture\x00";
    let unauthenticated = app
        .clone()
        .oneshot(
            Request::post(path)
                .header("x-anyagents-agent-id", owner.id.as_str())
                .body(Body::from(payload.as_slice()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);

    let response = request(
        &app,
        "POST",
        path,
        Some(&owner_token),
        "application/pdf",
        payload.as_slice(),
    )
    .await;
    let receipt = json_response(response, StatusCode::CREATED).await;
    assert_eq!(receipt["filename"], "report.pdf");
    assert_eq!(receipt["content_type"], "application/pdf");
    assert_eq!(receipt["bytes"], payload.len());
    let id = receipt["media_id"].as_str().unwrap();
    let source = receipt["source"].as_str().unwrap();
    assert_eq!(source, format!("media://{id}"));
    let href = format!("/v1/media/{}/uploads/{id}", owner.id);
    let download = request(&app, "GET", &href, Some(&owner_token), "", Body::empty()).await;
    assert_eq!(download.status(), StatusCode::OK);
    assert_eq!(download.headers()["cache-control"], "private, no-store");
    assert_eq!(
        download
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .as_ref(),
        payload
    );
    for path in [&href, &format!("/v1/media/{}/uploads/{id}", peer.id)] {
        let foreign = request(&app, "GET", path, Some(&peer_token), "", Body::empty()).await;
        assert_eq!(foreign.status(), StatusCode::NOT_FOUND);
    }

    let foreign = mcp_send(
        &app,
        &peer_token,
        json!({
            "to": ["parent"], "media": {"kind": "document", "source": source}
        }),
    )
    .await;
    assert_eq!(foreign["isError"], true, "{foreign}");
    assert!(
        foreign["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("private media not found"),
        "{foreign}"
    );
    assert!(state.store.inbox(&owner.id, None).unwrap().is_empty());

    let result = mcp_send(&app, &owner_token, json!({
        "to": ["children"], "text": "private report", "media": {"kind": "document", "source": source}
    })).await;
    assert_ne!(result["isError"], true, "{result}");
    let messages = state.store.inbox(&peer.id, None).unwrap();
    assert_eq!(messages.len(), 1);
    let prefix = format!("/v1/media/{}/uploads/", peer.id);
    let (_, copy_id) = messages[0].text.split_once(&prefix).unwrap();
    let copy_href = format!("{prefix}{}", copy_id.trim_end_matches(']').trim());
    assert!(!messages[0].text.contains(source));
    let copy = request(
        &app,
        "GET",
        &copy_href,
        Some(&peer_token),
        "",
        Body::empty(),
    )
    .await;
    assert_eq!(copy.status(), StatusCode::OK, "{}", messages[0].text);
    assert_eq!(
        copy.into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .as_ref(),
        payload
    );
    // A relationship permits delivery, not access to another agent's upload.
    let foreign_copy = request(
        &app,
        "GET",
        &copy_href,
        Some(&owner_token),
        "",
        Body::empty(),
    )
    .await;
    assert_eq!(foreign_copy.status(), StatusCode::NOT_FOUND);

    // Detect invalid and over-limit uploads at the HTTP boundary.
    for (content_type, bytes, status) in [
        ("application/octet-stream", vec![], StatusCode::BAD_REQUEST),
        ("application/octet-stream", vec![0; 25 * 1024 * 1024 + 1], StatusCode::PAYLOAD_TOO_LARGE),
        ("multipart/form-data; boundary=media", b"--media\r\nContent-Disposition: form-data; name=\"other\"\r\n\r\nabc\r\n--media--\r\n".to_vec(), StatusCode::BAD_REQUEST),
    ] {
        let response = request(&app, "POST", "/v1/media", Some(&owner_token), content_type, bytes).await;
        assert_eq!(response.status(), status);
    }
    state.store.revoke_remote_sessions(&owner.id, None).unwrap();
    let revoked = request(
        &app,
        "POST",
        path,
        Some(&owner_token),
        "application/pdf",
        payload.as_slice(),
    )
    .await;
    assert_eq!(revoked.status(), StatusCode::UNAUTHORIZED);
}
