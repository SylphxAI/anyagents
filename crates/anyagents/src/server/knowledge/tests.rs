use std::sync::{Arc, Mutex};

use anyagents_core::{NewAgent, NewRemoteTurn, hash_callback_token};
use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::Request as HttpRequest;
use tower::ServiceExt;

use super::*;
use crate::server::admin_auth::{self, AdminSession};

const OPERATOR: &str = "test-operator";
const ENV: [(&str, &str); 4] = [
    ("ANYAGENTS_ADMIN_OIDC_ISSUER", "https://issuer.example.com"),
    ("ANYAGENTS_ADMIN_OIDC_CLIENT_ID", "test-client"),
    ("ANYAGENTS_ADMIN_EMAILS", "owner@example.com"),
    (
        "ANYAGENTS_ADMIN_SESSION_SECRET",
        "test-fixture-signing-key-for-cookie-only",
    ),
];

struct OidcEnv(Vec<(&'static str, Option<std::ffi::OsString>)>);
impl OidcEnv {
    fn install() -> Self {
        Self(
            ENV.iter()
                .map(|&(key, value)| {
                    let previous = std::env::var_os(key);
                    // The shared test environment lock is held for this test.
                    unsafe { std::env::set_var(key, value) };
                    (key, previous)
                })
                .collect(),
        )
    }
}
impl Drop for OidcEnv {
    fn drop(&mut self) {
        for (key, previous) in &self.0 {
            match previous {
                Some(value) => unsafe { std::env::set_var(key, value) },
                None => unsafe { std::env::remove_var(key) },
            }
        }
    }
}

fn setup(dir: &std::path::Path) -> AppState {
    let store = Arc::new(Store::open(dir.join("state.db")).unwrap());
    store
        .hold_capability_key("fixture-capability-key-not-for-deployment")
        .unwrap();
    AppState {
        store,
        health: Default::default(),
        sign_in: Arc::new(Mutex::new(crate::server::types::ProcessRunner)),
        identity: Arc::new(crate::identity::ServingIdentity::capture()),
        profiles: Default::default(),
        timer_backend: Arc::new(crate::timer::NoopTimerBackend),
        auth: crate::auth::Mode::Confined {
            operator_digest: Some(crate::auth::operator_digest(OPERATOR)),
        },
    }
}

async fn post(
    app: &Router,
    path: &str,
    authority: (&str, &str),
    body: Value,
) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            HttpRequest::post(path)
                .header("content-type", "application/json")
                .header(authority.0, authority.1)
                .header(admin_auth::ADMIN_CSRF_HEADER, "1")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 128 * 1024).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[test]
fn knowledge_http_owner_grants_and_cas() {
    let _lock = crate::server::tests::env_lock();
    let _env = OidcEnv::install();
    let dir = crate::test_tempdir();
    let state = setup(dir.path());
    let mut labeled = NewAgent::new("role-is-not-authority", "test");
    labeled.role = Some("director".into());
    let agent = state.store.create_agent(labeled).unwrap();
    state
        .store
        .set_agent_lifecycle(&agent.id, "standing")
        .unwrap();
    let token = state.store.issue_capability(&agent.id).unwrap();
    state.store.join_group("reviewers", &agent.id).unwrap();
    let store = state.store.clone();
    let router = super::router().with_state(state);
    let now = admin_auth::unix_now_secs();
    let cookie = admin_auth::sign_admin_session(
        &admin_auth::session_signing_key_from_env().unwrap(),
        &AdminSession {
            email: "owner@example.com".into(),
            sub: "test-subject".into(),
            name: None,
            iat: now,
            exp: now + 600,
        },
    )
    .unwrap();
    let cookie = format!("{}={cookie}", admin_auth::ADMIN_SESSION_COOKIE);
    let root = ("cookie", cookie.as_str());
    let agent_auth = (crate::operator::AGENT_HEADER, token.as_str());
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let call = |auth, body| post(&router, "/v1/knowledge", auth, body);
        for namespace in ["org:instructions", "org:memory", "domain:example"] {
            let (status, doc) = call(root, json!({"action":"write", "namespace":namespace, "topic":"topic", "body":"first", "reason":"initial", "expected_version":0})).await;
            assert_eq!(status, StatusCode::OK, "{doc}");
            assert_eq!(doc["version"], 1);
            assert_eq!(call(agent_auth, json!({"action":"read", "namespace":namespace, "topic":"topic"})).await.0, StatusCode::FORBIDDEN);
        }
        let proposal = |namespace| json!({"action":"propose", "namespace":namespace, "topic":"topic", "body":"proposal", "reason":"revision reason", "sim_expected":"expected behavior", "expected_version":1});
        assert_eq!(call(agent_auth, proposal("domain:example")).await.0, StatusCode::FORBIDDEN);
        let (status, draft) = call(agent_auth, proposal("org:memory")).await;
        assert_eq!(status, StatusCode::OK, "{draft}");
        assert_eq!(draft["status"], "pending");
        let read_org = json!({"action":"read", "namespace":"org:memory", "topic":"topic"});
        assert_eq!(call(root, read_org).await.1["body"], "first");
        let promote = |version| json!({"action":"promote", "namespace":"org:memory", "proposal_id":draft["id"], "expected_version":version});
        assert_eq!(call(agent_auth, promote(1)).await.0, StatusCode::FORBIDDEN);
        assert_eq!(call(root, promote(2)).await.0, StatusCode::CONFLICT);
        let (status, promotion) = call(root, promote(1)).await;
        assert_eq!(status, StatusCode::OK, "{promotion}");
        assert_eq!(promotion["doc"]["version"], 2);
        let (_, history) = call(root, json!({"action":"revisions", "namespace":"org:memory", "topic":"topic"})).await;
        assert_eq!(history["revisions"][0]["sim_expected"], "expected behavior");
        let grant = |mode| json!({"action":"grant", "agent_id":agent.id, "namespace_pattern":"org:instructions", "mode":mode});
        assert_eq!(call((crate::operator::HEADER, OPERATOR), grant("read")).await.0, StatusCode::FORBIDDEN);
        assert_eq!(call(agent_auth, grant("read")).await.0, StatusCode::FORBIDDEN);
        let grant_group = json!({"action":"grant", "group_id":"reviewers", "namespace_pattern":"org:instructions", "mode":"read"});
        let mut ambiguous = grant_group.clone();
        ambiguous["agent_id"] = json!(agent.id);
        assert_eq!(call(root, ambiguous).await.0, StatusCode::BAD_REQUEST);
        let (status, read_grant) = call(root, grant_group).await;
        assert_eq!(status, StatusCode::OK, "{read_grant}");
        assert_eq!(read_grant["group_id"], "reviewers");
        assert_eq!(call(root, json!({"action":"grants"})).await.1["grants"][0]["id"], read_grant["id"]);
        assert_eq!(call(root, grant("write")).await.0, StatusCode::OK);
        let write = |version, body| json!({"action":"write", "namespace":"org:instructions", "topic":"topic", "body":body, "reason":"update", "expected_version":version});
        assert_eq!(call(agent_auth, write(1, "second")).await.0, StatusCode::OK);
        let (status, conflict) = call(root, write(1, "must-not-overwrite")).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(conflict["current_version"], 2);
        let read = json!({"action":"read", "namespace":"org:instructions", "topic":"topic"});
        assert_eq!(call(agent_auth, read.clone()).await.1["body"], "second");
        store.leave_group("reviewers", &agent.id).unwrap();
        assert_eq!(call(agent_auth, read).await.0, StatusCode::FORBIDDEN);
        assert_eq!(call(root, json!({"action":"revoke", "grant_id":read_grant["id"]})).await.1["revoked"], true);
        let (status, receipt) = call(agent_auth, json!({"action":"write", "namespace":"org:instructions", "topic":"topic", "title":"new title", "reason":"metadata only", "expected_version":2})).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(receipt, json!({"namespace":"org:instructions", "topic":"topic", "version":3}));
    });
}

#[tokio::test]
async fn knowledge_sandbox_wake_binds_token_and_grant_to_recipient() {
    let dir = crate::test_tempdir();
    let state = setup(dir.path());
    let store = state.store.clone();
    let agent = store
        .create_agent(NewAgent::new("wake-recipient", "test"))
        .unwrap();
    let peer = store
        .create_agent(NewAgent::new("wake-peer", "test"))
        .unwrap();
    let owner = Actor::Owner("owner".into());
    store
        .knowledge_write(
            &owner,
            "org:instructions",
            "rules",
            None,
            &Edit {
                body: Some("first-version".into()),
                reason: "init".into(),
                ..Edit::default()
            },
            false,
        )
        .unwrap();
    let grant = store
        .knowledge_grant(&owner, &agent.id, "org:instructions", Mode::Read)
        .unwrap();
    for (id, agent_id, token) in [
        ("wake-one", agent.id.clone(), "only-one"),
        ("wake-two", peer.id, "only-two"),
    ] {
        store
            .remote_turn_store()
            .create_turn(&NewRemoteTurn {
                turn_id: id.into(),
                agent_id,
                harness_kind: "sandbox".into(),
                provider_kind: None,
                remote_run_id: None,
                callback_token_hash: hash_callback_token(token),
                cursor: None,
                status: "running".into(),
                metadata_json: Some(json!({"message_seq":1})),
                next_poll_at_ms: None,
            })
            .unwrap();
    }
    let router = sandbox_router().with_state(state);
    let call = |token, id| {
        post(
            &router,
            "/v1/internal/guest/knowledge-wake",
            ("authorization", token),
            json!({"turn_id":id}),
        )
    };
    assert_eq!(
        call("Bearer only-one", "wake-two").await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call("Bearer only-two", "wake-two").await.1["context"],
        Value::Null
    );
    assert!(
        call("Bearer only-one", "wake-one").await.1["context"]
            .as_str()
            .unwrap()
            .contains("first-version")
    );
    store.knowledge_revoke(&owner, &grant.id).unwrap();
    assert_eq!(
        call("Bearer only-one", "wake-one").await.1["context"],
        Value::Null
    );
}

#[test]
fn knowledge_listing_budgets_serialized_pages() {
    let dir = crate::test_tempdir();
    let state = setup(dir.path());
    let owner = Actor::Owner("owner".into());
    for i in 0..20 {
        state
            .store
            .knowledge_write(
                &owner,
                "org:memory",
                &format!("item-{i:02}"),
                None,
                &Edit {
                    title: Some("\"".repeat(120)),
                    description: Some("\u{0001}".repeat(320)),
                    body: Some("text".into()),
                    reason: "init".into(),
                    ..Edit::default()
                },
                false,
            )
            .unwrap();
    }
    let mut offset = 0;
    loop {
        let page = execute(
            &state.store,
            &owner,
            Request::List {
                scope: Some("org".into()),
                limit: Some(20),
                offset: Some(offset),
                prefix: None,
            },
        )
        .unwrap();
        assert!(page.to_string().len() < 8192);
        for row in page["items"].as_array().unwrap() {
            assert_eq!(row["topic"], format!("item-{offset:02}"));
            assert!(row.get("body").is_none());
            offset += 1;
        }
        match page["next_offset"].as_u64() {
            Some(next) => assert_eq!(next as usize, offset),
            None => break,
        }
    }
    assert_eq!(offset, 20);
}

#[tokio::test]
async fn knowledge_mcp_dispatch_returns_only_authorized_citations() {
    let dir = crate::test_tempdir();
    let state = setup(dir.path());
    let agent = state
        .store
        .create_agent(NewAgent::new("knowledge-mcp", "test"))
        .unwrap();
    let owner = Actor::Owner("owner".into());
    for ns in ["org:memory", "domain:example"] {
        state
            .store
            .knowledge_write(
                &owner,
                ns,
                "guide",
                None,
                &Edit {
                    body: Some("部署守則".into()),
                    reason: "fixture".into(),
                    ..Edit::default()
                },
                false,
            )
            .unwrap();
    }
    let group = GroupId::parse("readers").unwrap();
    state.store.join_group(group.as_str(), &agent.id).unwrap();
    state
        .store
        .knowledge_grant_group(&owner, &group, "org:memory", Mode::Read)
        .unwrap();
    let id = agent.id.clone();
    let store = state.store.clone();
    let tools = crate::mcp::test_agent_over_router(super::router().with_state(state), agent);
    let search = json!({"action":"search", "scope":"all", "query":"部署守則"});
    let result = tools.test_dispatch_memory(search.clone()).await.unwrap();
    assert_eq!(result["hits"].as_array().unwrap().len(), 1);
    assert_eq!(result["hits"][0]["namespace"], "org:memory");
    assert_eq!(result["hits"][0]["topic"], "guide");
    assert_eq!(result["hits"][0]["version"], 1);
    assert!(
        tools
            .test_dispatch_memory(
                json!({"action":"read", "namespace":"domain:example", "topic":"guide"})
            )
            .await
            .is_err()
    );
    store.leave_group(group.as_str(), &id).unwrap();
    assert_eq!(
        tools.test_dispatch_memory(search).await.unwrap()["hits"],
        json!([])
    );
    let mut draft = json!({"action":"propose", "namespace":"org:memory", "topic":"guide", "expected_version":1, "body":"draft update", "reason":"fixture"});
    assert_eq!(
        tools.test_dispatch_memory(draft.clone()).await.unwrap()["status"],
        "pending"
    );
    draft["action"] = json!("write");
    assert!(tools.test_dispatch_memory(draft).await.is_err());
    assert_eq!(
        store
            .knowledge_read(&owner, "org:memory", "guide")
            .unwrap()
            .version,
        1
    );
}
