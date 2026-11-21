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

#[tokio::test]
async fn scratch_sign_in_surfaces_capacity_failure_without_retrying_or_allocating_a_volume() {
    use crate::sandbox_providers::providers::{
        EnsureSandboxRequest, SandboxProvider, SylphxConfig, SylphxProvider,
    };
    use crate::sandbox_providers::sylphx::tests::spawn_mock_http;

    let dir = tempfile::tempdir().unwrap();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&calls);
    let (endpoint, _server) = spawn_mock_http(move |method, path, body| {
        let body: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
        observed
            .lock()
            .unwrap()
            .push((method.to_owned(), path.to_owned(), body));
        (
            503,
            vec![("Content-Type".into(), "application/json".into())],
            serde_json::to_vec(&json!({
                "code": "NO_CAPACITY",
                "message": "compute unavailable at https://example.invalid/private?token=fixture",
                "retryable": true,
                "retry_after_ms": 5000,
            }))
            .unwrap(),
        )
    });
    let mut config = SylphxConfig::new("orgs/o1/projects/p1/envs/e1");
    config.endpoint = endpoint;
    config.default_volume_size_gib = Some(40);
    let provider: Arc<dyn SandboxProvider> = Arc::new(SylphxProvider::new(config).unwrap());
    let response = start_session(state(dir.path()), |_, _, _, _| async move {
        sandbox::ensure(&provider, EnsureSandboxRequest::for_agent("signin-fixture")).await?;
        panic!("capacity rejection must not start login");
    })
    .unwrap();
    let body = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        response.into_body().collect(),
    )
    .await
    .expect("a capacity rejection must terminate the stream before the retry timer")
    .unwrap()
    .to_bytes();
    let lines: Vec<Value> = std::str::from_utf8(&body)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0]["kind"], "session");
    assert_eq!(
        lines[1],
        json!({"kind":"error","text":"Could not prepare sandbox: HTTP 503: NO_CAPACITY: compute unavailable at [endpoint]"})
    );
    let calls = calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].0, "POST");
    assert_eq!(calls[0].1, "/v1/orgs/o1/projects/p1/envs/e1/leases");
    assert_eq!(calls[0].2["spec"]["ttl"], "600s");
    assert!(calls[0].2["spec"].get("volumes").is_none());
}
