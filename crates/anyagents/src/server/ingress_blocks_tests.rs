use super::*;
use axum::body::{Body, to_bytes};
use axum::http::{Request as HttpRequest, header};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tower::ServiceExt as _;

#[tokio::test]
async fn ingress_blocks_admin_crud_and_agent_scope() {
    let dir = crate::test_tempdir();
    let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
    store
        .hold_capability_key("test-capability-key-ingress-block-fixture")
        .unwrap();
    let agent = store
        .create_agent(anyagents_core::NewAgent::new("recipient", "claude-cloud"))
        .unwrap();
    let peer = store
        .create_agent(anyagents_core::NewAgent::new("other", "claude-cloud"))
        .unwrap();
    let capability = store.issue_capability(&agent.id).unwrap();
    let remote_session = store.begin_remote_session(&agent.id, 60_000).unwrap();
    let operator_token = "test-ingress-operator";
    let app = crate::server::build_router(AppState {
        store,
        health: Default::default(),
        sign_in: Arc::new(Mutex::new(crate::server::types::ProcessRunner)),
        identity: Arc::new(crate::server::ServingIdentity::capture()),
        profiles: Default::default(),
        timer_backend: Arc::new(crate::timer::NoopTimerBackend),
        auth: crate::auth::Mode::Confined {
            operator_digest: Some(crate::auth::operator_digest(operator_token)),
        },
    });
    let admin = format!("/admin/api/agents/{}/ingress-blocks", agent.id);
    let peer_admin = format!("/admin/api/agents/{}/ingress-blocks", peer.id);
    let own = "/v1/ingress-blocks";
    let operator = (crate::operator::HEADER, operator_token);
    let local_agent = (crate::operator::AGENT_HEADER, capability.as_str());
    let remote_agent = (crate::operator::AGENT_HEADER, remote_session.token.as_str());
    let call = |method: &str, path: &str, (key, value): (&str, &str), body: Value| {
        let req = HttpRequest::builder()
            .method(method)
            .uri(path)
            .header(key, value)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        let app = app.clone();
        async move {
            let response = app.oneshot(req).await.unwrap();
            let status = response.status();
            let bytes = to_bytes(response.into_body(), 16_384).await.unwrap();
            (
                status,
                serde_json::from_slice::<Value>(&bytes).unwrap_or(Value::Null),
            )
        }
    };
    let target = json!({"channel":"telegram","scope":{"kind":"sender","sender_id":"87901"}});
    let block = json!({"op":"block_sender","target":target,"reason":"synthetic"});
    let unblock = json!({"op":"unblock_sender","target":target});
    let list = json!({"op":"list_blocks"});

    assert_eq!(
        call("POST", &admin, local_agent, block.clone()).await.0,
        StatusCode::FORBIDDEN
    );
    // Remote agents reach this operation through authenticated MCP, not the
    // private coordinator route used by the local agent tool server.
    assert_eq!(
        call("POST", own, remote_agent, list.clone()).await.0,
        StatusCode::UNAUTHORIZED
    );
    let mut forged = block.clone();
    forged["relationship"] = json!("stranger");
    assert_eq!(
        call("POST", own, local_agent, forged).await.0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    let (status, receipt) = call("POST", &admin, operator, block.clone()).await;
    assert_eq!(status, StatusCode::OK, "{receipt}");
    assert_eq!(receipt["rule"]["blocked_by"], "operator");
    let (status, receipt) = call("POST", own, local_agent, list).await;
    assert_eq!(status, StatusCode::OK, "{receipt}");
    assert_eq!(receipt["rules"][0]["target"], target);
    assert_eq!(
        call("GET", &peer_admin, operator, Value::Null).await.1["rules"],
        json!([])
    );
    assert_eq!(
        call("DELETE", &admin, operator, unblock.clone()).await.1["removed"],
        true
    );
    assert_eq!(
        call("DELETE", &admin, operator, unblock).await.1["removed"],
        false
    );
    assert_eq!(
        call("GET", &admin, operator, Value::Null).await.1["rules"],
        json!([])
    );
    let (status, receipt) = call("POST", own, local_agent, block).await;
    assert_eq!(status, StatusCode::OK, "{receipt}");
    assert_eq!(receipt["rule"]["blocked_by"], format!("agent:{}", agent.id));
}
