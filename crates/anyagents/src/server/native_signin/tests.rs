use axum::http::Request;
use http_body_util::BodyExt as _;
use serde_json::{Value, json};
use tower::ServiceExt as _;

use super::*;
use crate::auth::{InjectedPrincipal, Mode};

fn state(dir: &std::path::Path) -> AppState {
    AppState {
        store: Arc::new(anyagents_core::Store::open(dir.join("state.db")).unwrap()),
        auth: Mode::Confined {
            operator_digest: None,
        },
        health: Default::default(),
        sign_in: Arc::new(Mutex::new(super::super::ProcessRunner)),
        identity: Arc::new(crate::identity::ServingIdentity::capture()),
        profiles: Default::default(),
        timer_backend: Arc::new(crate::timer::NoopTimerBackend),
    }
}

fn post(uri: &str, principal: Principal, body: Value) -> Request<Body> {
    Request::post(uri)
        .header("content-type", "application/json")
        .extension(InjectedPrincipal(principal))
        .body(Body::from(body.to_string()))
        .unwrap()
}

#[tokio::test]
async fn http_stream_is_private_operator_only_and_relays_only_to_the_active_session() {
    let dir = tempfile::tempdir().unwrap();
    let state = state(dir.path());
    let root = "/v1/profiles/native-signin";
    let app = router().with_state(state.clone());
    let denied = app
        .clone()
        .oneshot(post(
            root,
            Principal::Agent {
                agent_id: anyagents_core::AgentId::generate(),
                name: "fixture".into(),
            },
            json!({"harness":"codex"}),
        ))
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);
    let unsupported = app
        .clone()
        .oneshot(post(
            root,
            Principal::Operator,
            json!({"harness":"unsupported"}),
        ))
        .await
        .unwrap();
    assert_eq!(unsupported.status(), StatusCode::BAD_REQUEST);

    let response = start_session(state, |_, _, output, mut input| async move {
        send(&output, "link", "https://auth.openai.com/codex/device").await;
        let request = input.recv().await.unwrap();
        assert_eq!(request.value, "fixture-code");
        request.result.send(Ok(())).unwrap();
        Ok("test/profile".into())
    })
    .unwrap();
    assert_eq!(response.headers()["content-type"], "application/x-ndjson");
    assert_eq!(response.headers()["cache-control"], "no-store, private");
    let mut body = response.into_body();
    let read = |frame: axum::body::Bytes| serde_json::from_slice::<Value>(&frame).unwrap();
    let session = read(body.frame().await.unwrap().unwrap().into_data().unwrap());
    assert_eq!(session["kind"], "session");
    let link = read(body.frame().await.unwrap().unwrap().into_data().unwrap());
    assert_eq!(link["text"], "https://auth.openai.com/codex/device");
    let uri = format!("{root}/{}/input", session["text"].as_str().unwrap());
    let response = app
        .clone()
        .oneshot(post(
            &uri,
            Principal::Operator,
            json!({"value":"fixture-code"}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let done = read(body.frame().await.unwrap().unwrap().into_data().unwrap());
    assert_eq!(done, json!({"kind":"done","text":"test/profile"}));
    let gone = app
        .oneshot(post(
            &uri,
            Principal::Operator,
            json!({"value":"fixture-code"}),
        ))
        .await
        .unwrap();
    assert_eq!(gone.status(), StatusCode::GONE);
}
