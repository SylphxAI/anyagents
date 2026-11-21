use super::*;
use anyagents_core::{GroupAction, GroupActor, NewAgent};
use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, Method, StatusCode, Uri},
};
use std::sync::Mutex;

const MASTER: &str = "mock sandbox credential encryption key";
const ALPHA: &str = "fixture-alpha-bearer";
const BETA: &str = "fixture-beta-bearer";
const GLOBAL: &str = "fixture-default-bearer";

#[derive(Clone)]
struct Mock {
    parent: String,
    token: &'static str,
    requests: Arc<Mutex<Vec<(Method, String)>>>,
}

async fn respond(
    State(mock): State<Mock>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> (StatusCode, Json<Value>) {
    let prefix = format!("/v1/{}", mock.parent);
    let path = uri.path();
    if headers.get("authorization").and_then(|v| v.to_str().ok())
        != Some(&format!("Bearer {}", mock.token))
    {
        return (StatusCode::UNAUTHORIZED, Json(json!({})));
    }
    let Some(resource) = path.strip_prefix(&prefix) else {
        return (StatusCode::NOT_FOUND, Json(json!({})));
    };
    mock.requests
        .lock()
        .unwrap()
        .push((method.clone(), resource.into()));
    let body = match (method.as_str(), resource) {
        ("GET", "/leases") => json!({"leases": []}),
        ("GET", path) if path.starts_with("/volumes/") => {
            return (StatusCode::NOT_FOUND, Json(json!({})));
        }
        ("POST", "/volumes") => json!({"id": "volume"}),
        ("POST", "/leases") => json!({"id": "lease", "state": "READY"}),
        ("POST", "/leases/lease:exec") => {
            json!({"exit_code": 0, "stdout_base64": "ZXhpc3RzOjEyMzo3"})
        }
        ("POST", "/leases/lease:release") | ("DELETE", "/volumes/volume") => json!({}),
        ("POST", "/leases/lease:readFile") => {
            return (StatusCode::BAD_GATEWAY, Json(json!({"message":mock.token})));
        }
        _ => return (StatusCode::NOT_FOUND, Json(json!({}))),
    };
    (StatusCode::OK, Json(body))
}

async fn mock(scope: &str, token: &'static str) -> (String, Mock, tokio::task::JoinHandle<()>) {
    let mock = Mock {
        parent: format!("orgs/{scope}/projects/example/envs/test"),
        token,
        requests: Default::default(),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let app = Router::new().fallback(respond).with_state(mock.clone());
    (
        endpoint,
        mock,
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() }),
    )
}

#[test]
fn sandbox_target_billing_is_inherited_fenced_and_redacted() {
    // Spawn only this test with an isolated master key, without mutating the
    // environment used by concurrent credential and provider fixtures.
    const WORKER: &str = "ANYAGENTS_SANDBOX_TARGET_TEST_WORKER";
    if std::env::var_os(WORKER).is_none() {
        let out = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "sandbox_exec::target::tests::sandbox_target_billing_is_inherited_fenced_and_redacted"])
            .env(WORKER, "1")
            .env("ANYAGENTS_MASTER_KEY", MASTER)
            .env_remove("ANYAGENTS_DATABASE_URL")
            .env_remove("DATABASE_URL")
            .output().unwrap();
        assert!(
            out.status.success()
                && String::from_utf8_lossy(&out.stdout).contains("1 passed; 0 failed"),
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        return;
    }
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let store = Store::open(dir.path().join("state.db")).unwrap();
            let (a_url, a, a_server) = mock("alpha", ALPHA).await;
            let (b_url, b, b_server) = mock("beta", BETA).await;
            let (g_url, g, g_server) = mock("default", GLOBAL).await;
            std::fs::write(
                dir.path().join("sandbox-provider.json"),
                json!({
                    "provider":"sylphx", "endpoint":g_url, "parent":g.parent, "token":GLOBAL
                })
                .to_string(),
            )
            .unwrap();
            let root = store.create_agent(NewAgent::new("root", "codex")).unwrap();
            let mut new = NewAgent::new("child", "codex")
                .with_runtime(anyagents_core::ExecutionRuntime::Sandbox { provider: None });
            new.parent_id = Some(root.id.clone());
            let child = store.create_agent(new).unwrap();
            let other = store.create_agent(NewAgent::new("other", "codex")).unwrap();
            let unbound = store
                .create_agent(NewAgent::new("unbound", "codex"))
                .unwrap();
            for (name, url, endpoint) in [("alpha", &a_url, &a), ("beta", &b_url, &b)] {
                store
                    .put_sandbox_credential(
                        name,
                        &SandboxCredential {
                            token: endpoint.token.into(),
                            base_url: Some(url.clone()),
                            parent: endpoint.parent.clone(),
                        },
                    )
                    .unwrap();
            }
            store.set_sandbox_profile("team", "alpha").unwrap();
            store
                .set_agent_sandbox_target(&root.id, Some("team"), None)
                .unwrap();
            store
                .set_agent_sandbox_target(&other.id, None, Some("beta"))
                .unwrap();
            let reference = store.sandbox_credential_ref(&child.id).unwrap().unwrap();
            assert_eq!(reference.name, "alpha");
            assert_eq!(
                reference.source,
                anyagents_core::sandbox_targets::SandboxCredentialSource::Inherited
            );
            assert!(store.sandbox_credential_ref(&unbound.id).unwrap().is_none());

            store
                .group_action_as(
                    &GroupActor::Operator,
                    "billing",
                    GroupAction::Join,
                    std::slice::from_ref(&root.id),
                    None,
                )
                .unwrap();
            store
                .set_group_sandbox_profile("billing", Some("team"))
                .unwrap();
            assert!(
                store
                    .group_action_as(
                        &GroupActor::Agent(other.id.clone()),
                        "billing",
                        GroupAction::Join,
                        std::slice::from_ref(&other.id),
                        None
                    )
                    .is_err()
            );
            assert!(
                store
                    .group_action_as(
                        &GroupActor::Agent(root.id.clone()),
                        "billing",
                        GroupAction::Leave,
                        std::slice::from_ref(&root.id),
                        None
                    )
                    .is_err()
            );

            for agent in [&child, &other, &unbound] {
                let config = anyagents_core::agent_config_dir(store.data_dir(), &agent.id);
                let binding =
                    ensure_sandbox_for_agent(&store, agent, &config, None, &BTreeMap::new())
                        .unwrap()
                        .unwrap();
                let registry = resolve_registry_for_binding(&store, &binding).unwrap();
                let handle = sandbox_handle_from_binding(&binding).unwrap();
                let provider = registry.active();
                let output = provider
                    .exec(
                        &handle,
                        &crate::sandbox_providers::providers::ExecRequest::argv(["test-command"]),
                    )
                    .await
                    .unwrap();
                assert_eq!(output.stdout, "exists:123:7");
                if agent.id != unbound.id {
                    assert!(!config.join("sandbox-sylphx.json").exists());
                    let client = provider.scoped_lease_client().unwrap();
                    let resource = binding.lease_resource.as_deref().unwrap();
                    let error = client.read_file(resource, "/file").unwrap_err();
                    if agent.id == child.id {
                        let watch = crate::schedule::create_agent_schedule_with_mode(
                            &store,
                            &anyagents_supervisor::HealthBoard::default(),
                            &child,
                            None,
                            None,
                            Some("file:fixture"),
                            "watch fixture",
                            false,
                        )
                        .unwrap();
                        assert_eq!(
                            store
                                .get_schedule(&watch.id)
                                .unwrap()
                                .unwrap()
                                .watch_baseline
                                .as_deref(),
                            Some("sandbox:exists:123:7")
                        );
                    }
                    assert!(
                        error.contains("502") && !error.contains(ALPHA) && !error.contains(BETA)
                    );
                    assert!(
                        !binding.spec_json.contains(ALPHA) && !binding.spec_json.contains(BETA)
                    );
                    assert!(release_sandbox_for_agent(&store, &agent.id, &config).unwrap());
                    destroy_sandbox_for_agent(&store, &agent.id, &config).unwrap();
                }
            }
            for endpoint in [&a, &b] {
                let requests = endpoint.requests.lock().unwrap();
                for (method, path) in [
                    (Method::POST, "/volumes"),
                    (Method::POST, "/leases"),
                    (Method::POST, "/leases/lease:exec"),
                    (Method::POST, "/leases/lease:release"),
                    (Method::DELETE, "/volumes/volume"),
                    (Method::POST, "/leases/lease:readFile"),
                ] {
                    assert!(
                        requests.contains(&(method.clone(), path.into())),
                        "missing {method} {path}"
                    );
                }
            }
            assert!(
                g.requests
                    .lock()
                    .unwrap()
                    .contains(&(Method::POST, "/leases".into()))
            );
            let config = anyagents_core::agent_config_dir(store.data_dir(), &child.id);
            let globals_before = g.requests.lock().unwrap().len();
            store.delete_sandbox_credential("alpha").unwrap();
            let err = ensure_sandbox_for_agent(&store, &child, &config, None, &BTreeMap::new())
                .unwrap_err();
            assert_eq!(err, "bound sandbox credential is missing or unreadable");
            assert_eq!(g.requests.lock().unwrap().len(), globals_before);
            for server in [a_server, b_server, g_server] {
                server.abort();
            }
        });
}
