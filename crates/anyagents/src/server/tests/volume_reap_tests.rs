use super::super::agents_lifecycle::{
    VerdictRequest, archive_agent_route, delete_agent_route, verdict_route,
};
use super::super::*;
use super::helpers::archetype_validation_state;
use anyagents_core::stateless::SandboxBinding;
use anyagents_core::{Agent, NewAgent, stateless::NewRemoteTurn};
use axum::Json;
use axum::extract::Path;
use axum::http::{Method, StatusCode, Uri};
use base64::Engine as _;
use serde_json::{Value, json};

const PARENT: &str = "orgs/test/projects/test/envs/test";
const VOLUME: &str = "orgs/test/projects/test/envs/test/volumes/workspace";
const LEASE: &str = "orgs/test/projects/test/envs/test/leases/lease";

#[derive(Clone)]
struct SandboxMock {
    calls: Arc<Mutex<Vec<String>>>,
    probe: (StatusCode, Value),
    volume_delete: Arc<Mutex<StatusCode>>,
}

async fn sandbox(
    State(mock): State<SandboxMock>,
    method: Method,
    uri: Uri,
    body: axum::body::Bytes,
) -> (StatusCode, Json<Value>) {
    let op = match (method.as_str(), uri.path().strip_prefix("/v1/")) {
        ("POST", Some(path)) if path == format!("{LEASE}:exec") => "probe",
        ("POST", Some(path)) if path == format!("{LEASE}:release") => "release",
        ("DELETE", Some(VOLUME)) => "delete",
        _ => "unexpected",
    };
    mock.calls.lock().unwrap().push(op.into());
    let (status, response) = match op {
        "probe" => {
            let request: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(request["command"][0], "/bin/sh");
            assert_eq!(request["working_directory"], "/workspace");
            mock.probe
        }
        "release" => (StatusCode::OK, json!({})),
        "delete" => (
            *mock.volume_delete.lock().unwrap(),
            json!({"message": "volume busy"}),
        ),
        _ => (
            StatusCode::BAD_REQUEST,
            json!({"message": "unexpected request"}),
        ),
    };
    (status, Json(response))
}

struct Fixture {
    _dir: tempfile::TempDir,
    state: AppState,
    agent: Agent,
    mock: SandboxMock,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl Fixture {
    async fn new(
        lifecycle: &str,
        live: bool,
        probe: (StatusCode, Value),
        delete: StatusCode,
    ) -> Self {
        let mock = SandboxMock {
            calls: Default::default(),
            probe,
            volume_delete: Arc::new(Mutex::new(delete)),
        };
        let app = Router::new().fallback(sandbox).with_state(mock.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let dir = tempfile::tempdir().unwrap();
        let state = archetype_validation_state(dir.path());
        let parent = state
            .store
            .create_agent(NewAgent::new("parent", "opencode"))
            .unwrap();
        let mut child = NewAgent::new("worker", "opencode");
        child.parent_id = Some(parent.id);
        let child = state.store.create_agent(child).unwrap();
        let agent = state
            .store
            .set_agent_lifecycle(&child.id, lifecycle)
            .unwrap();
        let mut binding = SandboxBinding::new(agent.id.clone(), "sylphx");
        binding.volume_id = "workspace".into();
        binding.volume_resource = VOLUME.into();
        binding.remote_workdir = "/workspace".into();
        binding.spec_json = json!({"endpoint": endpoint, "parent": PARENT,
            "metadata": {"lease_resource": format!("{LEASE}-stale"), "volume_resource": "stale"}})
        .to_string();
        if live {
            binding.lease_state = "ready".into();
            binding.lease_id = Some("lease".into());
            binding.lease_resource = Some(LEASE.into());
            binding.data_plane_resource = Some(LEASE.into());
        }
        state
            .store
            .sandbox_binding_store()
            .upsert(&binding)
            .unwrap();
        Self {
            _dir: dir,
            state,
            agent,
            mock,
            server,
        }
    }

    async fn archive(&self) -> Value {
        self.state_result(
            archive_agent_route(
                Principal::Operator,
                State(self.state.clone()),
                Path(self.agent.name.clone()),
            )
            .await,
        )
    }

    async fn delete(&self) -> Value {
        self.state_result(
            delete_agent_route(
                Principal::Operator,
                State(self.state.clone()),
                Path(self.agent.name.clone()),
            )
            .await,
        )
    }

    async fn verdict(&self, verdict: &str) -> Value {
        self.state_result(
            verdict_route(
                Principal::Operator,
                State(self.state.clone()),
                Path(self.agent.name.clone()),
                Json(VerdictRequest {
                    verdict: verdict.into(),
                    reason: None,
                }),
            )
            .await,
        )
    }

    fn state_result<T: serde::Serialize>(&self, result: Result<Json<T>, ApiError>) -> Value {
        serde_json::to_value(result.unwrap_or_else(|e| panic!("{}", e.message)).0).unwrap()
    }

    fn calls(&self, expected: &[&str]) {
        assert_eq!(&*self.mock.calls.lock().unwrap(), expected);
    }

    fn binding(&self) -> SandboxBinding {
        self.state
            .store
            .sandbox_binding(&self.agent.id)
            .unwrap()
            .unwrap()
    }
}

fn probe(exit_code: i64, stdout: &str) -> (StatusCode, Value) {
    (
        StatusCode::OK,
        json!({"exit_code": exit_code, "stdout_base64": base64::engine::general_purpose::STANDARD.encode(stdout), "stderr": ""}),
    )
}

#[tokio::test]
async fn delete_releases_and_reaps_standing_and_task_including_404() {
    for (lifecycle, status) in [
        ("standing", StatusCode::OK),
        ("task", StatusCode::NOT_FOUND),
    ] {
        let f = Fixture::new(lifecycle, true, probe(1, ""), status).await;
        let result = f.delete().await;
        assert_eq!(result["deleted"], true, "{result}");
        assert_eq!(result["remote_volume_reaped"], true);
        assert!(f.state.store.agent(&f.agent.id).is_err());
        f.calls(&["release", "delete"]);
    }
}

#[tokio::test]
async fn standing_archive_releases_compute_but_preserves_volume() {
    let f = Fixture::new(
        "standing",
        true,
        probe(0, "clean and pushed"),
        StatusCode::OK,
    )
    .await;
    let result = f.archive().await;
    assert_eq!(result["state"], "archived");
    assert_eq!(result["remote_volume_reaped"], false);
    assert_eq!(result["remote_volume_reap_reason"], "standing agent");
    f.calls(&["release"]);
    assert_eq!(f.binding().lease_state, "detached");
    assert_eq!(f.binding().volume_resource, VOLUME);
}

#[tokio::test]
async fn every_terminal_verdict_reaps_detached_volume_and_can_retry_via_archive() {
    for verdict in ["accepted", "rejected", "abandoned"] {
        let f = Fixture::new("task", false, probe(1, ""), StatusCode::NOT_FOUND).await;
        let result = f.verdict(verdict).await;
        assert_eq!(result["kind"], verdict);
        assert_eq!(result["remote_volume_reaped"], true, "{result}");
        assert_eq!(f.state.store.agent(&f.agent.id).unwrap().state, "archived");
        f.calls(&["delete"]);
        assert_eq!(f.archive().await["remote_volume_reaped"], true);
        f.calls(&["delete", "delete"]);
        f.state
            .store
            .set_agent_state(&f.agent.id, "running")
            .unwrap();
        assert_eq!(
            f.archive().await["remote_volume_reaped"],
            false,
            "historical verdict must not authorize the next archive"
        );
        f.calls(&["delete", "delete"]);
    }
}

#[tokio::test]
async fn live_unjudged_task_probes_before_releasing_and_only_reaps_on_proof() {
    for (status, stdout, expected) in [
        (0, "clean and pushed\n", true),
        (0, "no repository\n", true),
        (1, "", false),
        (0, "unexpected output", false),
    ] {
        let f = Fixture::new("task", true, probe(status, stdout), StatusCode::OK).await;
        let result = f.archive().await;
        assert_eq!(result["remote_volume_reaped"], expected, "{result}");
        if expected {
            f.calls(&["probe", "release", "delete"]);
        } else {
            f.calls(&["probe", "release"]);
        }
    }
}

#[tokio::test]
async fn detached_unjudged_task_is_not_probed_or_deleted() {
    let f = Fixture::new("task", false, probe(0, "no repository"), StatusCode::OK).await;
    let result = f.archive().await;
    assert_eq!(result["remote_volume_reaped"], false);
    assert!(
        result["remote_volume_reap_reason"]
            .as_str()
            .unwrap()
            .contains("detached"),
        "{result}"
    );
    assert_eq!(f.binding().volume_resource, VOLUME);
    f.calls(&[]);
}

#[tokio::test]
async fn active_writer_disqualifies_probe() {
    let f = Fixture::new("task", true, probe(0, "no repository"), StatusCode::OK).await;
    f.state
        .store
        .remote_turn_store()
        .create_turn(&NewRemoteTurn {
            turn_id: "turn-test".into(),
            agent_id: f.agent.id.clone(),
            harness_kind: "sandbox".into(),
            provider_kind: Some("sylphx".into()),
            remote_run_id: None,
            callback_token_hash: "test".into(),
            cursor: None,
            status: "running".into(),
            metadata_json: None,
            next_poll_at_ms: None,
        })
        .unwrap();
    assert_eq!(f.archive().await["remote_volume_reaped"], false);
    f.calls(&["release"]);
}

#[tokio::test]
async fn non_404_volume_failure_keeps_archived_identity_and_binding_for_retry() {
    let f = Fixture::new("task", true, probe(1, ""), StatusCode::SERVICE_UNAVAILABLE).await;
    let result = f.verdict("accepted").await;
    assert_eq!(result["remote_volume_reaped"], false);
    assert!(
        result["remote_volume_reap_reason"]
            .as_str()
            .unwrap()
            .contains("503"),
        "{result}"
    );
    assert_eq!(f.binding().lease_state, "detached");
    assert_eq!(f.binding().volume_resource, VOLUME);
    f.calls(&["release", "delete"]);
    let result = f.delete().await;
    assert_eq!(result["deleted"], false);
    assert_eq!(result["remote_volume_reaped"], false);
    assert_eq!(f.state.store.agent(&f.agent.id).unwrap().state, "archived");
    *f.mock.volume_delete.lock().unwrap() = StatusCode::NOT_FOUND;
    assert_eq!(f.archive().await["remote_volume_reaped"], true);
    assert_eq!(f.delete().await["deleted"], true);
    f.calls(&["release", "delete", "delete", "delete", "delete"]);
}
