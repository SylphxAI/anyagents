use std::collections::BTreeMap;
use std::ffi::OsString;
use std::sync::{Arc, Mutex};

use anyagents_core::{
    MASTER_KEY_ENV, NewAgent, NewRemoteTurn, SettleRemoteTurn, Store, generate_callback_token,
    generate_turn_id, hash_callback_token, now_ms,
};
use anyagents_supervisor::HealthBoard;
use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt as _;
use serde_json::{Value, json};
use tower::ServiceExt as _;

use super::merge_sandbox_exec_env;
use crate::server::{AppState, admin_auth, build_router, build_webhook_router, serverless};

struct EnvRestore(Vec<(&'static str, Option<OsString>)>);

impl EnvRestore {
    fn capture(keys: &[&'static str]) -> Self {
        Self(keys.iter().map(|&k| (k, std::env::var_os(k))).collect())
    }
}

impl Drop for EnvRestore {
    fn drop(&mut self) {
        for (k, v) in &self.0 {
            match v {
                Some(val) => unsafe { std::env::set_var(k, val) },
                None => unsafe { std::env::remove_var(k) },
            }
        }
    }
}

const TEST_OPERATOR_TOKEN: &str = "op-secrets-test-token-0123456789abcdef";
const TEST_MASTER_KEY: &str = "test-master-key-0123456789abcdef0123456789abcdef";

fn make_confined_app() -> (tempfile::TempDir, Arc<Store>, Router) {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
    store
        .hold_capability_key("test-capability-key-0123456789abcdef")
        .unwrap();
    let state = AppState {
        store: Arc::clone(&store),
        health: HealthBoard::default(),
        sign_in: Arc::new(Mutex::new(crate::server::types::ProcessRunner)),
        auth: crate::auth::Mode::Confined {
            operator_digest: Some(crate::auth::operator_digest(TEST_OPERATOR_TOKEN)),
        },
        identity: Arc::new(crate::identity::ServingIdentity::capture()),
        profiles: Default::default(),
        timer_backend: Arc::new(crate::timer::NoopTimerBackend),
    };
    let app = build_router(state);
    (dir, store, app)
}

async fn send_json(
    app: &Router,
    method: &str,
    uri: &str,
    auth_header: Option<(&str, &str)>,
    body: Option<Value>,
) -> (StatusCode, String, Value) {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some((k, v)) = auth_header {
        builder = builder.header(k, v);
    }
    let req_body = if let Some(val) = body {
        builder = builder.header("content-type", "application/json");
        Body::from(serde_json::to_vec(&val).unwrap())
    } else {
        Body::empty()
    };
    let resp = app
        .clone()
        .oneshot(builder.body(req_body).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let raw = String::from_utf8_lossy(&bytes).to_string();
    let parsed = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, raw, parsed)
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn sandbox_target_api_is_operator_only_and_keeps_billing_secrets_off_read_surfaces() {
    let _env = crate::server::tests::env_lock();
    let _restore = EnvRestore::capture(&[MASTER_KEY_ENV]);
    // SAFETY: shared test environment lock is held.
    unsafe { std::env::set_var(MASTER_KEY_ENV, TEST_MASTER_KEY) };
    let (dir, store, app) = make_confined_app();
    let agent = store
        .create_agent(NewAgent::new("fixture", "codex"))
        .unwrap();
    let capability = store.issue_capability(&agent.id).unwrap();
    let operator = Some((crate::operator::HEADER, TEST_OPERATOR_TOKEN));
    let agent_auth = Some((crate::operator::AGENT_HEADER, capability.as_str()));
    let token = "fixture-billing-bearer";
    let credential_path = "/v1/sandbox-credentials/example";
    let target_path = "/v1/agents/fixture/sandbox-target";
    let credential = json!({"token": token, "parent": "orgs/sample/projects/sample/envs/test"});
    for (method, path, body) in [
        ("PUT", credential_path, Some(credential.clone())),
        ("DELETE", credential_path, None),
        (
            "PUT",
            "/v1/sandbox-profiles/example",
            Some(json!({"credential":"example"})),
        ),
        ("PUT", target_path, Some(json!({"credential":"example"}))),
        ("GET", target_path, None),
    ] {
        let (status, raw, _) = send_json(&app, method, path, agent_auth, body).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{method} {path}: {raw}");
        assert!(!raw.contains(token));
    }
    for (path, body) in [
        (credential_path, credential),
        (
            "/v1/sandbox-profiles/example",
            json!({"credential":"example"}),
        ),
        (target_path, json!({"profile":"example"})),
    ] {
        let (status, raw, _) = send_json(&app, "PUT", path, operator, Some(body)).await;
        assert_eq!(status, StatusCode::NO_CONTENT, "{path}: {raw}");
        assert!(raw.is_empty());
    }
    let (status, raw, body) = send_json(&app, "GET", target_path, operator, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({"credential":"example", "source":"profile"}));
    assert!(!raw.contains(token));
    let (status, raw, _) = send_json(&app, "GET", "/v1/agents/fixture", operator, None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(!raw.contains(token));
    for name in ["state.db", "state.db-wal"] {
        if let Ok(bytes) = std::fs::read(dir.path().join(name)) {
            assert!(
                !bytes
                    .windows(token.len())
                    .any(|window| window == token.as_bytes())
            );
        }
    }
    assert!(
        store
            .resolve_agent_secret_env(&agent.id)
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn write_only_secret_crud_encryption_at_rest_and_admin_access() {
    let _env = crate::server::tests::env_lock();
    let _restore = EnvRestore::capture(&[MASTER_KEY_ENV]);
    let (dir, store, app) = make_confined_app();

    let op = Some((crate::operator::HEADER, TEST_OPERATOR_TOKEN));
    let root_peer = store
        .create_agent(NewAgent::new("root-peer", "claude-code"))
        .unwrap();
    let root_cap = store.issue_capability(&root_peer.id).unwrap();

    let non_admin = store
        .create_agent(NewAgent::new("regular-agent", "claude-code"))
        .unwrap();
    let regular_cap = store.issue_capability(&non_admin.id).unwrap();

    // 1. Fail closed when ANYAGENTS_MASTER_KEY is unset.
    unsafe { std::env::remove_var(MASTER_KEY_ENV) };
    let (st, _, _) = send_json(
        &app,
        "PUT",
        "/v1/secrets/ci-github",
        op,
        Some(json!({ "env": { "GH_TOKEN": "canary_gh_value_failclosed_12345" } })),
    )
    .await;
    assert_eq!(st, StatusCode::INTERNAL_SERVER_ERROR);

    // 2. With ANYAGENTS_MASTER_KEY set, operator upserts write-only secret.
    unsafe { std::env::set_var(MASTER_KEY_ENV, TEST_MASTER_KEY) };
    let secret_val_a = "canary_gh_secret_value_99112233";
    let secret_val_b = "canary_deploy_key_value_44556677";
    let (st, raw, body) = send_json(
        &app,
        "PUT",
        "/v1/secrets/ci-github",
        op,
        Some(json!({
            "env": {
                "GH_TOKEN": secret_val_a,
                "DEPLOY_KEY": secret_val_b,
            }
        })),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "upsert failed: {raw}");
    assert_eq!(body["name"], "ci-github");
    assert_eq!(body["env_keys"], json!(["DEPLOY_KEY", "GH_TOKEN"]));
    assert!(
        !raw.contains(secret_val_a),
        "response leaked secret value A"
    );
    assert!(
        !raw.contains(secret_val_b),
        "response leaked secret value B"
    );

    // 3. Verify encryption at rest directly in SQLite.
    let raw_conn = rusqlite::Connection::open(dir.path().join("state.db")).unwrap();
    let raw_ct: String = raw_conn
        .query_row(
            "SELECT env_entries_ciphertext FROM deployment_secrets WHERE name = 'ci-github'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(raw_ct.starts_with("aaenc1:"));
    assert!(!raw_ct.contains(secret_val_a));
    assert!(!raw_ct.contains(secret_val_b));
    assert!(!raw_ct.contains("GH_TOKEN"));

    // 4. Reserved internal coordinator keys are rejected.
    for reserved in [
        "ANYAGENTS_MASTER_KEY",
        "ANYAGENTS_OPERATOR_TOKEN",
        "ANYAGENTS_CAPABILITY_KEY",
        "ANYAGENTS_DATABASE_URL",
        "ANYAGENTS_DIRECT_DATABASE_URL",
        "ANYAGENTS_TICK_TOKEN",
        "ANYAGENTS_ADMIN_SESSION_SECRET",
        "ANYAGENTS_ADMIN_OIDC_CLIENT_SECRET",
        "ANYAGENTS_S3_SECRET_ACCESS_KEY",
    ] {
        let (st, _, _) = send_json(
            &app,
            "PUT",
            "/v1/secrets/bad-reserved",
            op,
            Some(json!({ "env": { reserved: "nope" } })),
        )
        .await;
        assert_eq!(st, StatusCode::BAD_REQUEST, "should reject {reserved}");
    }

    // 5. GET /v1/secrets returns metadata ONLY (never values).
    let (st, raw, body) = send_json(&app, "GET", "/v1/secrets", op, None).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(body["secrets"].as_array().unwrap().len(), 1);
    assert!(!raw.contains(secret_val_a));
    assert!(!raw.contains(secret_val_b));

    // 6. Non-admin agent is denied on all /v1/secrets endpoints.
    let reg_hdr = Some((crate::operator::AGENT_HEADER, regular_cap.as_str()));
    assert_eq!(
        send_json(&app, "GET", "/v1/secrets", reg_hdr, None).await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        send_json(
            &app,
            "PUT",
            "/v1/secrets/nope",
            reg_hdr,
            Some(json!({ "env": { "FOO": "bar" } })),
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        send_json(&app, "DELETE", "/v1/secrets/ci-github", reg_hdr, None)
            .await
            .0,
        StatusCode::FORBIDDEN
    );

    // 7. Even a root agent has ZERO admin privilege on /v1/secrets (strictly operator-only).
    let root_hdr = Some((crate::operator::AGENT_HEADER, root_cap.as_str()));
    assert_eq!(
        send_json(&app, "GET", "/v1/secrets", root_hdr, None)
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        send_json(&app, "GET", "/v1/secrets/ci-github", root_hdr, None)
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        send_json(
            &app,
            "PUT",
            "/v1/secrets/root-denied",
            root_hdr,
            Some(json!({ "env": { "STRIPE_KEY": "canary_root_denied_000111" } })),
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        send_json(&app, "DELETE", "/v1/secrets/ci-github", root_hdr, None)
            .await
            .0,
        StatusCode::FORBIDDEN
    );

    // 8. Operator detail GET /v1/secrets/{name} is strictly write-only (metadata only, NEVER raw values).
    let (st, raw, detail) = send_json(&app, "GET", "/v1/secrets/ci-github", op, None).await;
    assert_eq!(st, StatusCode::OK, "operator detail failed: {raw}");
    assert_eq!(detail["name"], json!("ci-github"));
    assert_eq!(detail["env_keys"], json!(["DEPLOY_KEY", "GH_TOKEN"]));
    assert!(!raw.contains("canary_gh_"));
    assert!(!raw.contains("canary_deploy_"));
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn capability_delegation_attenuation_self_mutation_deny_and_cascade_delete() {
    let _env = crate::server::tests::env_lock();
    let _restore = EnvRestore::capture(&[
        MASTER_KEY_ENV,
        "ANYAGENTS_FAKE_MODE",
        "HOME",
        "ANYAGENTS_OWNER_WORKDIR",
    ]);
    let (dir, store, app) = make_confined_app();
    let home = dir.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    unsafe {
        std::env::set_var(MASTER_KEY_ENV, TEST_MASTER_KEY);
        std::env::set_var("ANYAGENTS_FAKE_MODE", "1");
        std::env::set_var("HOME", &home);
        std::env::remove_var("ANYAGENTS_OWNER_WORKDIR");
    }

    let op = Some((crate::operator::HEADER, TEST_OPERATOR_TOKEN));

    // Create three deployment secrets s1, s2, s3.
    for (name, key, val) in [
        ("s1", "S1_KEY", "val_s1_secret_11111111"),
        ("s2", "S2_KEY", "val_s2_secret_22222222"),
        ("s3", "S3_KEY", "val_s3_secret_33333333"),
    ] {
        let (st, raw, _) = send_json(
            &app,
            "PUT",
            &format!("/v1/secrets/{name}"),
            op,
            Some(json!({ "env": { key: val } })),
        )
        .await;
        assert_eq!(st, StatusCode::OK, "upsert {name}: {raw}");
    }

    // Create parent root agent and grant [s1, s2] via operator.
    let parent = store
        .create_agent(NewAgent::new("parent", "claude-code"))
        .unwrap();
    let parent_cap = store.issue_capability(&parent.id).unwrap();
    let parent_hdr = Some((crate::operator::AGENT_HEADER, parent_cap.as_str()));

    // Non-existent secret name rejected (400).
    let (st, _, _) = send_json(
        &app,
        "PUT",
        "/v1/agents/parent/secrets",
        op,
        Some(json!({ "secrets": ["missing-secret"] })),
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);

    let (st, raw, body) = send_json(
        &app,
        "PUT",
        "/v1/agents/parent/secrets",
        op,
        Some(json!({ "secrets": ["s2", "s1"] })),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{raw}");
    assert_eq!(body["secrets"], json!(["s1", "s2"]));

    // 1. Newly created child defaults to [] when `secrets` is omitted.
    let (st, raw, child_view) = send_json(
        &app,
        "POST",
        "/v1/agents",
        parent_hdr,
        Some(json!({ "name": "child", "harness": "claude-code" })),
    )
    .await;
    assert_eq!(st, StatusCode::CREATED, "{raw}");
    assert_eq!(child_view["secrets"], json!([]));

    // 2. Parent cannot create a child with a secret it does NOT hold (s3) -> 403, no orphan agent.
    let (st, _, _) = send_json(
        &app,
        "POST",
        "/v1/agents",
        parent_hdr,
        Some(json!({ "name": "child-denied", "harness": "claude-code", "secrets": ["s3"] })),
    )
    .await;
    assert_eq!(st, StatusCode::FORBIDDEN);
    assert!(store.agent_by_name("child-denied").is_err());

    // 3. Parent CAN create sibling child with subset ["s1"] at creation time.
    let (st, raw, sibling_view) = send_json(
        &app,
        "POST",
        "/v1/agents",
        parent_hdr,
        Some(json!({ "name": "sibling", "harness": "claude-code", "secrets": ["s1"] })),
    )
    .await;
    assert_eq!(st, StatusCode::CREATED, "{raw}");
    assert_eq!(sibling_view["secrets"], json!(["s1"]));

    // 4. Parent grants subset ["s1"] to direct child via PUT and PATCH.
    let (st, raw, body) = send_json(
        &app,
        "PUT",
        "/v1/agents/child/secrets",
        parent_hdr,
        Some(json!({ "secrets": ["s1"] })),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{raw}");
    assert_eq!(body["secrets"], json!(["s1"]));

    // Parent tries to grant s3 (which it does NOT hold) to child -> 403.
    let (st, _, _) = send_json(
        &app,
        "PUT",
        "/v1/agents/child/secrets",
        parent_hdr,
        Some(json!({ "secrets": ["s1", "s3"] })),
    )
    .await;
    assert_eq!(st, StatusCode::FORBIDDEN);

    // Parent updates child via PATCH /v1/agents/child -> OK for subset, 403 for unheld secret.
    let (st, raw, body) = send_json(
        &app,
        "PATCH",
        "/v1/agents/child",
        parent_hdr,
        Some(json!({ "secrets": ["s1", "s2"] })),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{raw}");
    assert_eq!(body["secrets"], json!(["s1", "s2"]));

    let (st, _, _) = send_json(
        &app,
        "PATCH",
        "/v1/agents/child",
        parent_hdr,
        Some(json!({ "secrets": ["s1", "s3"] })),
    )
    .await;
    assert_eq!(st, StatusCode::FORBIDDEN);

    // Attenuate child back to ["s1"] only.
    let (st, _, body) = send_json(
        &app,
        "PATCH",
        "/v1/agents/child",
        parent_hdr,
        Some(json!({ "secrets": ["s1"] })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(body["secrets"], json!(["s1"]));

    let child = store.agent_by_name("child").unwrap();
    let child_cap = store.issue_capability(&child.id).unwrap();
    let child_hdr = Some((crate::operator::AGENT_HEADER, child_cap.as_str()));

    let sibling = store.agent_by_name("sibling").unwrap();
    let sibling_cap = store.issue_capability(&sibling.id).unwrap();
    let sibling_hdr = Some((crate::operator::AGENT_HEADER, sibling_cap.as_str()));

    // 5. Self-mutation ALWAYS denied for non-admin agent (even subset or empty list!).
    for (method, path, payload) in [
        (
            "PUT",
            "/v1/agents/child/secrets",
            json!({ "secrets": ["s1"] }),
        ),
        ("PUT", "/v1/agents/child/secrets", json!({ "secrets": [] })),
        ("PUT", "/v1/agents/self/secrets", json!({ "secrets": [] })),
        ("PATCH", "/v1/agents/child", json!({ "secrets": [] })),
        ("PATCH", "/v1/agents/self", json!({ "secrets": ["s1"] })),
    ] {
        let (st, raw, _) = send_json(&app, method, path, child_hdr, Some(payload)).await;
        assert_eq!(
            st,
            StatusCode::FORBIDDEN,
            "self-mutation should be 403 for {method} {path}: {raw}"
        );
    }

    // 6. Child cannot grant to sibling or parent (403).
    assert_eq!(
        send_json(
            &app,
            "PUT",
            "/v1/agents/sibling/secrets",
            child_hdr,
            Some(json!({ "secrets": ["s1"] })),
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        send_json(
            &app,
            "PUT",
            "/v1/agents/parent/secrets",
            child_hdr,
            Some(json!({ "secrets": ["s1"] })),
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );

    // 7. Child CAN grant subset of its own held secrets (["s1"]) to grandchild, NOT s2!
    let (st, raw, gc_view) = send_json(
        &app,
        "POST",
        "/v1/agents",
        child_hdr,
        Some(json!({ "name": "grandchild", "harness": "claude-code", "secrets": ["s1"] })),
    )
    .await;
    assert_eq!(st, StatusCode::CREATED, "{raw}");
    assert_eq!(gc_view["secrets"], json!(["s1"]));

    assert_eq!(
        send_json(
            &app,
            "PUT",
            "/v1/agents/grandchild/secrets",
            child_hdr,
            Some(json!({ "secrets": ["s2"] })),
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );

    // Ancestor (grandparent holding s1, s2) CAN update grandchild's secrets within its own held
    // set, and CANNOT grant unheld s3 (403); then restore ["s1"] for cascade-delete check below.
    let gp = "/v1/agents/grandchild/secrets";
    let (st, raw, body) = send_json(
        &app,
        "PUT",
        gp,
        parent_hdr,
        Some(json!({ "secrets": ["s1", "s2"] })),
    )
    .await;
    assert_eq!(
        (st, &body["secrets"]),
        (StatusCode::OK, &json!(["s1", "s2"])),
        "{raw}"
    );
    assert_eq!(
        send_json(
            &app,
            "PUT",
            gp,
            parent_hdr,
            Some(json!({ "secrets": ["s3"] }))
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        send_json(
            &app,
            "PUT",
            gp,
            parent_hdr,
            Some(json!({ "secrets": ["s1"] }))
        )
        .await
        .0,
        StatusCode::OK
    );

    // 8. GET /v1/agents/{name}/secrets visibility: operator/self/ancestor OK, sibling 403.
    for hdr in [op, parent_hdr, child_hdr] {
        let (st, raw, body) = send_json(&app, "GET", "/v1/agents/child/secrets", hdr, None).await;
        assert_eq!(st, StatusCode::OK, "{raw}");
        assert_eq!(body["secrets"], json!(["s1"]));
        assert!(!raw.contains("val_s1_secret_11111111"));
    }
    assert_eq!(
        send_json(&app, "GET", "/v1/agents/child/secrets", sibling_hdr, None)
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    // AgentView exposes secrets to self/ancestor/operator, hides from sibling.
    let (st, _, view_as_child) = send_json(&app, "GET", "/v1/agents/child", child_hdr, None).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(view_as_child["secrets"], json!(["s1"]));

    let (st, _, view_as_sibling) =
        send_json(&app, "GET", "/v1/agents/child", sibling_hdr, None).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(view_as_sibling["secrets"], json!([]));

    // 9. Deleting secret s1 cascades removal across parent, child, sibling, grandchild.
    let (st, _, _) = send_json(&app, "DELETE", "/v1/secrets/s1", op, None).await;
    assert_eq!(st, StatusCode::OK);

    assert_eq!(
        send_json(&app, "GET", "/v1/agents/parent/secrets", op, None)
            .await
            .2["secrets"],
        json!(["s2"])
    );
    assert_eq!(
        send_json(&app, "GET", "/v1/agents/child/secrets", op, None)
            .await
            .2["secrets"],
        json!([])
    );
    assert_eq!(
        send_json(&app, "GET", "/v1/agents/grandchild/secrets", op, None)
            .await
            .2["secrets"],
        json!([])
    );
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn sandbox_env_resolution_and_turn_output_masking_scrub_granted_secrets() {
    let _env = crate::server::tests::env_lock();
    let _restore = EnvRestore::capture(&[MASTER_KEY_ENV]);
    unsafe { std::env::set_var(MASTER_KEY_ENV, TEST_MASTER_KEY) };

    let (_dir, store, _app) = make_confined_app();
    let agent = store
        .create_agent(NewAgent::new("runner", "claude-code"))
        .unwrap();

    let canary = "canary_opaque_sandbox_secret_77441199";
    let mut bundle = BTreeMap::new();
    bundle.insert("CUSTOM_DEPLOY_TOKEN".to_string(), canary.to_string());
    store
        .upsert_deployment_secret("deploy-creds", &bundle)
        .unwrap();
    store
        .set_agent_secrets(&agent.id, &["deploy-creds".to_string()], Some("operator"))
        .unwrap();

    // 1. Sandbox env resolution merges decrypted secret without overriding system ANYAGENTS_* vars.
    let mut env = BTreeMap::new();
    env.insert(
        "ANYAGENTS_MCP_URL".to_string(),
        "https://coord.example.com/mcp".to_string(),
    );
    merge_sandbox_exec_env(&store, &agent, &mut env).unwrap();
    assert_eq!(
        env.get("CUSTOM_DEPLOY_TOKEN").map(String::as_str),
        Some(canary)
    );
    assert_eq!(
        env.get("ANYAGENTS_MCP_URL").map(String::as_str),
        Some("https://coord.example.com/mcp")
    );

    // 2. Process-wide masking automatically scrubs registered secret values.
    let raw_line = format!("running deploy with {canary} done");
    let masked_line = anyagents_core::masking::mask_known_secrets(&raw_line);
    assert_eq!(masked_line, "running deploy with [redacted] done");
    assert!(!masked_line.contains(canary));

    // 3. Settling a remote turn scrubs granted secrets from persisted turn & transcript blob.
    let turn_id = generate_turn_id();
    let token = generate_callback_token();
    store
        .remote_turn_store()
        .create_turn(&NewRemoteTurn {
            turn_id: turn_id.clone(),
            agent_id: agent.id.clone(),
            harness_kind: "sandbox".to_string(),
            provider_kind: Some("local".to_string()),
            remote_run_id: None,
            callback_token_hash: hash_callback_token(&token),
            cursor: None,
            status: "running".to_string(),
            metadata_json: None,
            next_poll_at_ms: None,
        })
        .unwrap();

    let settled = serverless::settle_remote_turn_and_deliver(
        &store,
        &turn_id,
        SettleRemoteTurn {
            status: "completed".to_string(),
            result_text: Some(format!("deployed with token {canary} ok")),
            error_text: None,
            usage_json: Some(json!({ "note": format!("leak {canary}") })),
            cursor: None,
            settled_at_ms: now_ms(),
        },
        None,
        0,
    )
    .unwrap()
    .unwrap();

    let result_text = settled.turn.result_text.as_deref().unwrap_or("");
    assert_eq!(result_text, "deployed with token [redacted] ok");
    assert!(!result_text.contains(canary));

    let transcript = store
        .blob_store()
        .get_agent_file(&agent.id, "sessions", &format!("turns/{turn_id}.json"))
        .unwrap()
        .expect("transcript persisted");
    let transcript_str = String::from_utf8_lossy(&transcript.content);
    assert!(!transcript_str.contains(canary));
    assert!(transcript_str.contains("[redacted]"));
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn admin_ui_serves_secrets_tab_and_enforces_csrf_on_secret_mutations() {
    let _env = crate::server::tests::env_lock();
    let _restore = EnvRestore::capture(&[
        MASTER_KEY_ENV,
        "ANYAGENTS_SERVERLESS",
        "ANYAGENTS_OPERATOR_TOKEN",
        "ANYAGENTS_OPERATOR_TOKEN_FILE",
        "ANYAGENTS_ADMIN_OIDC_ISSUER",
        "ANYAGENTS_ADMIN_OIDC_CLIENT_ID",
        "ANYAGENTS_ADMIN_EMAILS",
        "ANYAGENTS_ADMIN_SESSION_SECRET",
        "ANYAGENTS_PUBLIC_URL",
    ]);
    unsafe {
        std::env::set_var(MASTER_KEY_ENV, TEST_MASTER_KEY);
        std::env::set_var("ANYAGENTS_SERVERLESS", "true");
        std::env::remove_var("ANYAGENTS_OPERATOR_TOKEN");
        std::env::remove_var("ANYAGENTS_OPERATOR_TOKEN_FILE");
        std::env::set_var("ANYAGENTS_ADMIN_OIDC_ISSUER", "https://idp.example.com");
        std::env::set_var("ANYAGENTS_ADMIN_OIDC_CLIENT_ID", "console-client");
        std::env::set_var("ANYAGENTS_ADMIN_EMAILS", "ops@example.com");
        std::env::set_var(
            "ANYAGENTS_ADMIN_SESSION_SECRET",
            "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
        );
        std::env::set_var("ANYAGENTS_PUBLIC_URL", "https://console.example.com");
    }

    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
    let agent = store
        .create_agent(NewAgent::new("console-worker", "claude-code"))
        .unwrap();
    let auth = crate::auth::Mode::from_env(dir.path());
    let app = build_webhook_router(AppState {
        store: Arc::clone(&store),
        health: HealthBoard::default(),
        sign_in: Arc::new(Mutex::new(crate::server::types::ProcessRunner)),
        auth,
        identity: Arc::new(crate::identity::ServingIdentity::capture()),
        profiles: Default::default(),
        timer_backend: Arc::new(crate::timer::NoopTimerBackend),
    });

    // 1. /admin redirects to /, and authenticated / HTML includes Secrets tab and loads
    //    /assets/secrets.js with zero inline scripts (legacy /admin/assets/secrets.js also serves).
    let key = admin_auth::session_signing_key_from_env().unwrap();
    let now_s = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let cookie_val = admin_auth::sign_admin_session(
        &key,
        &admin_auth::AdminSession {
            email: "ops@example.com".to_string(),
            name: Some("Ops".to_string()),
            sub: "sub-ops".to_string(),
            iat: now_s,
            exp: now_s + 3600,
        },
    )
    .unwrap();
    let cookie_hdr = format!("{}={cookie_val}", admin_auth::ADMIN_SESSION_COOKIE);

    let admin_redir = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/admin")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(admin_redir.status(), StatusCode::FOUND);
    assert_eq!(
        admin_redir
            .headers()
            .get(axum::http::header::LOCATION)
            .and_then(|v| v.to_str().ok()),
        Some("/")
    );

    let html_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/")
                .header(axum::http::header::COOKIE, &cookie_hdr)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(html_resp.status(), StatusCode::OK);
    let html = String::from_utf8_lossy(&html_resp.into_body().collect().await.unwrap().to_bytes())
        .to_string();
    assert!(html.contains(r#"data-tab="secrets""#));
    assert!(html.contains(r#"src="/assets/secrets.js""#));
    assert!(!html.contains("<script>"));

    for js_path in ["/assets/secrets.js", "/admin/assets/secrets.js"] {
        let js_resp = app
            .clone()
            .oneshot(Request::builder().uri(js_path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(js_resp.status(), StatusCode::OK, "{js_path}");
        assert!(
            js_resp.headers().contains_key("content-security-policy"),
            "{js_path}"
        );
    }

    // 2. OIDC cookie mutations require X-Anyagents-Admin-Csrf: 1.

    let no_csrf = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/v1/secrets/ui-secret")
                .header(axum::http::header::COOKIE, &cookie_hdr)
                .header("content-type", "application/json")
                .body(Body::from(r#"{"env":{"FOO":"canary_ui_secret_123456"}}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(no_csrf.status(), StatusCode::FORBIDDEN);

    let with_csrf = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/v1/secrets/ui-secret")
                .header(axum::http::header::COOKIE, &cookie_hdr)
                .header(admin_auth::ADMIN_CSRF_HEADER, "1")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"env":{"FOO":"canary_ui_secret_123456"}}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(with_csrf.status(), StatusCode::OK);
    let body_text =
        String::from_utf8_lossy(&with_csrf.into_body().collect().await.unwrap().to_bytes())
            .to_string();
    assert!(!body_text.contains("canary_ui_secret_123456"));

    let grant_ok = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/agents/{}/secrets", agent.name))
                .header(axum::http::header::COOKIE, &cookie_hdr)
                .header(admin_auth::ADMIN_CSRF_HEADER, "1")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"secrets":["ui-secret"]}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(grant_ok.status(), StatusCode::OK);
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn parent_grants_secrets_to_existing_paused_child_and_enforces_subset_before_wake() {
    let _env = crate::server::tests::env_lock();
    let _r = EnvRestore::capture(&[MASTER_KEY_ENV, "ANYAGENTS_FAKE_MODE", "HOME"]);
    let (dir, store, app) = make_confined_app();
    let home = dir.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    unsafe {
        std::env::set_var(MASTER_KEY_ENV, TEST_MASTER_KEY);
        std::env::set_var("ANYAGENTS_FAKE_MODE", "1");
        std::env::set_var("HOME", &home);
    }
    let op = Some((crate::operator::HEADER, TEST_OPERATOR_TOKEN));
    for (p, b) in [
        (
            "/v1/secrets/gh-token",
            json!({ "env": { "GH_TOKEN": "canary_gh_11" } }),
        ),
        (
            "/v1/secrets/deploy-key",
            json!({ "env": { "DEPLOY_KEY": "canary_dk_22" } }),
        ),
    ] {
        assert_eq!(
            send_json(&app, "PUT", p, op, Some(b)).await.0,
            StatusCode::OK
        );
    }
    let mk = |n: &str, p: Option<&anyagents_core::AgentId>, paused: bool, grant: bool| {
        let mut a = NewAgent::new(n, "claude-code");
        a.parent_id = p.cloned();
        let ag = store.create_agent(a).unwrap();
        if paused {
            store.set_agent_state(&ag.id, "paused").unwrap();
        }
        if grant {
            store
                .set_agent_secrets(&ag.id, &["gh-token".into()], Some("operator"))
                .unwrap();
        }
        (ag.id.clone(), store.issue_capability(&ag.id).unwrap())
    };
    let (pid, pcap) = mk("lead", None, false, true);
    let (cid, _) = mk("paused-child", Some(&pid), true, false);
    let _ = mk("paused-gc", Some(&cid), true, false);
    let (_, scap) = mk("sibling-worker", Some(&pid), false, true);
    let (_, ocap) = mk("unrelated-root", None, false, true);
    fn hdr(c: &str) -> Option<(&'static str, &str)> {
        Some((crate::operator::AGENT_HEADER, c))
    }
    let (ph, sh, oh) = (hdr(&pcap), hdr(&scap), hdr(&ocap));
    let (ok, both) = (
        json!({ "secrets": ["gh-token"] }),
        json!({ "secrets": ["gh-token", "deploy-key"] }),
    );
    let (cp, ca) = ("/v1/agents/paused-child/secrets", "/v1/agents/paused-child");
    for (m, p) in [("PUT", cp), ("PATCH", "/v1/agents/paused-gc")] {
        let (st, raw, b) = send_json(&app, m, p, ph, Some(ok.clone())).await;
        assert_eq!(st, StatusCode::OK, "{raw}");
        assert_eq!(b["secrets"], json!(["gh-token"]));
    }
    for (h, b) in [(ph, &both), (sh, &ok), (oh, &ok)] {
        for (m, p) in [("PUT", cp), ("PATCH", ca)] {
            let r = send_json(&app, m, p, h, Some(b.clone())).await;
            assert_eq!(r.0, StatusCode::FORBIDDEN);
        }
    }
    let gc_bad = send_json(&app, "PUT", "/v1/agents/paused-gc/secrets", ph, Some(both)).await;
    assert_eq!(gc_bad.0, StatusCode::FORBIDDEN);
    let w = send_json(
        &app,
        "POST",
        "/v1/agents/paused-child/wake",
        ph,
        Some(json!({})),
    )
    .await;
    assert_eq!(w.0, StatusCode::OK);
    assert_eq!(
        (&w.2["state"], &w.2["secrets"]),
        (&json!("running"), &json!(["gh-token"]))
    );
}
