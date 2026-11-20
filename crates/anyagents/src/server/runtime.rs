//! Orthogonal `harness × runtime` switching (`POST /v1/agents/{name}/runtime`)
//! and serverless cloud turn budget/metadata helpers (ADR 0010).

use anyagents_core::{Agent, ExecutionRuntime, Store};
use axum::Json;
use axum::extract::{Path as AxumPath, State};
use axum::http::{HeaderMap, StatusCode};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::auth::Principal;
use crate::create;

use super::serverless::ServerlessDispatchInput;
use super::types::{
    ApiError, AppState, is_self_or_ancestor, refuse_host_placement_over_http,
    require_operator_audited, require_remote_callback_for_runtime, spawn_store, via_http_mcp,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RuntimeSwitchRequest {
    #[serde(default)]
    pub(super) runtime: Option<String>,
    #[serde(default)]
    pub(super) harness: Option<String>,
    #[serde(default)]
    pub(super) model: Option<String>,
    #[serde(default)]
    pub(super) profile: Option<String>,
    #[serde(default)]
    pub(super) budget_cap_micros: Option<u64>,
    #[serde(default)]
    pub(super) clear_budget: Option<bool>,
}

/// `POST /v1/agents/{name}/runtime` — switch an agent's execution runtime
/// (`local`, `sandbox`, `sandbox:<provider>`, `cloud`), optional harness/model/profile,
/// and optional remote spend budget cap (`budget_cap_micros`) fail-closed.
pub(super) async fn runtime_route(
    principal: Principal,
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    headers: HeaderMap,
    Json(body): Json<RuntimeSwitchRequest>,
) -> Result<Json<Value>, ApiError> {
    let via_http = via_http_mcp(&headers);
    spawn_store(move || {
        let found = state
            .store
            .agent_by_name(&name)
            .map_err(ApiError::from_store)?;
        let actor = match &principal {
            Principal::Agent {
                agent_id,
                name: caller_name,
            } if agent_id != &found.id && is_self_or_ancestor(&state.store, agent_id, &found) => {
                format!("agent:{caller_name}")
            }
            _ => {
                require_operator_audited(&state.store, &principal, "agents.runtime", &found.name)?
            }
        };

        let raw_harness = body
            .harness
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| found.canonical_harness());
        let resolved = anyagents_harness::resolve_harness_and_runtime(
            &raw_harness,
            body.runtime.as_deref(),
            found.runtime.clone(),
        )
        .map_err(|err| ApiError::new(StatusCode::BAD_REQUEST, err.to_string()))?;

        if crate::sandbox_providers::SandboxMode::from_env().is_remote_required()
            && resolved.runtime == ExecutionRuntime::Local
        {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "runtime `local` is refused when ANYAGENTS_SANDBOX_MODE=remote_required; use `sandbox` or `cloud`",
            ));
        }

        if body.runtime.is_some() || resolved.runtime != found.runtime {
            if resolved.runtime.is_off_host() {
                require_remote_callback_for_runtime(state.store.data_dir(), &resolved.runtime)?;
            }
            refuse_host_placement_over_http(
                via_http,
                state.store.data_dir(),
                &resolved.harness,
                Some(&resolved.runtime.as_canonical_string()),
            )?;
        }

        let budget_update = if body.clear_budget.unwrap_or(false) {
            Some(None)
        } else {
            body.budget_cap_micros
                .map(|cap| (cap > 0).then_some(cap))
        };

        let out = create::configure_agent_runtime_with_options(
            &state.store,
            state.store.data_dir(),
            &actor,
            &found.name,
            Some(&resolved.harness),
            Some(&resolved.runtime.as_canonical_string()),
            body.model.as_deref(),
            body.profile.as_deref(),
            budget_update,
        )
        .map_err(ApiError::from_store)?;
        state.profiles.invalidate(&state.store);
        Ok(Json(out))
    })
    .await
}

/// Fail-closed hard spend budget guard executed before dispatching a remote/cloud turn.
pub(super) fn check_remote_budget_cap(store: &Store, agent: &Agent) -> Result<(), String> {
    let Some(cap_micros) = agent.remote_budget_micros.filter(|&c| c > 0) else {
        return Ok(());
    };
    let records = store
        .list_usage(Some(&agent.id))
        .map_err(|e| format!("failed to query agent spend for budget enforcement: {e}"))?;
    let spent_micros: u64 = records
        .iter()
        .filter_map(|r| r.reported_cost_micros.or(r.estimated_cost_micros))
        .filter(|&v| v > 0)
        .fold(0u64, |acc, v| acc.saturating_add(v as u64));
    if spent_micros >= cap_micros {
        return Err(format!(
            "remote spend budget exceeded for agent `{}`: spent ${:.4} >= cap ${:.4} ({spent_micros}/{cap_micros} micro-USD)",
            agent.name,
            spent_micros as f64 / 1_000_000.0,
            cap_micros as f64 / 1_000_000.0,
        ));
    }
    Ok(())
}

/// Build structured turn metadata persisted on `pending_remote_turns` for cloud turns.
pub(super) fn build_cloud_turn_metadata(
    input: &ServerlessDispatchInput,
    cloud_endpoint: &str,
    saved_remote_agent: Option<&str>,
) -> Value {
    let mut meta = serde_json::Map::new();
    meta.insert("cloud_endpoint".into(), json!(cloud_endpoint));
    meta.insert("agent_harness".into(), json!(input.agent.harness));
    meta.insert(
        "agent_runtime".into(),
        json!(input.agent.runtime.as_canonical_string()),
    );
    if let Some(cap) = input.agent.remote_budget_micros {
        meta.insert("remote_budget_micros".into(), json!(cap));
    }
    if let Some(ref model) = input.agent.model {
        meta.insert("agent_model".into(), json!(model));
    }
    if let Some(rid) = saved_remote_agent {
        meta.insert("remote_agent_id".into(), json!(rid));
    }
    if let Some(seq) = input.message_seq {
        meta.insert("message_seq".into(), json!(seq));
    }
    if let Some(ref opener) = input.opener_id {
        meta.insert("opener_id".into(), json!(opener));
    }
    if let Some(ref rt) = input.reply_target {
        meta.insert(
            "reply_target".into(),
            serde_json::to_value(rt).unwrap_or(Value::Null),
        );
    }
    Value::Object(meta)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use anyagents_core::{BillingMode, NewAgent, NewUsage};
    use anyagents_supervisor::HealthBoard;
    use axum::http::header::HeaderName;

    use super::*;
    use crate::api::CreateAgentRequest;
    use crate::sandbox_providers::providers::{
        EnsureSandboxRequest, ExecOutput, ExecRequest, ProviderHealth, ProviderKind,
        SandboxCapabilities, SandboxHandle, SandboxProvider as AsyncSandboxProvider,
    };
    use crate::server::agents::create_agent_route;
    use crate::server::serverless::dispatch_serverless_turn;
    use crate::server::types::ProcessRunner;

    fn test_state(dir: &std::path::Path) -> AppState {
        let store = Arc::new(Store::open(dir.join("state.db")).unwrap());
        store
            .hold_capability_key("0123456789abcdef0123456789abcdef")
            .unwrap();
        AppState {
            store,
            health: HealthBoard::default(),
            sign_in: Arc::new(Mutex::new(ProcessRunner)),
            auth: crate::auth::Mode::Trusted {
                data_dir: dir.to_path_buf(),
            },
            identity: Arc::new(crate::identity::ServingIdentity::capture()),
            profiles: Default::default(),
        }
    }

    struct ZeroSandboxSpyProvider {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl AsyncSandboxProvider for ZeroSandboxSpyProvider {
        fn kind(&self) -> ProviderKind {
            ProviderKind::Http
        }

        fn capabilities(&self) -> SandboxCapabilities {
            SandboxCapabilities::default()
        }

        async fn ensure(&self, _req: &EnsureSandboxRequest) -> Result<SandboxHandle, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            panic!("cloud turn must allocate 0 sandboxes");
        }

        async fn exec(
            &self,
            _handle: &SandboxHandle,
            _req: &ExecRequest,
        ) -> Result<ExecOutput, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            panic!("cloud turn must execute 0 sandbox commands");
        }

        async fn sync_bundle_tar(
            &self,
            _handle: &SandboxHandle,
            _dest_dir: &str,
            _tar_gz: &[u8],
        ) -> Result<(), String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            panic!("cloud turn must not sync sandbox bundles");
        }

        async fn release(&self, _handle: &SandboxHandle) -> Result<(), String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn destroy(&self, _handle: &SandboxHandle) -> Result<(), String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn health(&self) -> Result<ProviderHealth, String> {
            Ok(ProviderHealth {
                provider: ProviderKind::Http,
                healthy: true,
                message: "spy".into(),
                latency_ms: Some(0),
            })
        }
    }

    #[test]
    fn create_agent_route_normalizes_cloud_selectors_and_rejects_agy_cloud() {
        let _env = crate::server::tests::env_lock();
        let dir = crate::test_tempdir();
        std::fs::write(
            dir.path().join(crate::remote::CONFIG_FILE),
            "[server]\npublic_url = \"https://agents.example.com\"\n",
        )
        .unwrap();
        let state = test_state(dir.path());

        let cases = [
            (
                "sp-orthogonal",
                json!({"name": "sp-orthogonal", "harness": "spiron", "runtime": "cloud", "budget_cap_micros": 2_500_000}),
                Some(2_500_000),
            ),
            (
                "sp-at-cloud",
                json!({"name": "sp-at-cloud", "harness": "spiron@cloud"}),
                None,
            ),
            (
                "sp-legacy-cloud",
                json!({"name": "sp-legacy-cloud", "harness": "spiron-cloud"}),
                None,
            ),
        ];

        tokio::runtime::Runtime::new().unwrap().block_on(async {
            for (name, payload, want_budget) in cases {
                let req: CreateAgentRequest = serde_json::from_value(payload).unwrap();
                let (status, Json(view)) = create_agent_route(
                    Principal::Operator,
                    State(state.clone()),
                    HeaderMap::new(),
                    Json(req),
                )
                .await
                .unwrap_or_else(|e| panic!("{name}: {}: {}", e.status, e.message));
                assert_eq!(status, StatusCode::CREATED);
                assert_eq!(view.harness, "spiron", "{name}");
                assert_eq!(view.runtime, "cloud", "{name}");
                assert_eq!(view.remote_budget_micros, want_budget, "{name}");

                let stored = state.store.agent_by_name(name).unwrap();
                assert_eq!(stored.harness, "spiron", "{name}");
                assert_eq!(stored.runtime, ExecutionRuntime::Cloud, "{name}");
                assert_eq!(stored.remote_budget_micros, want_budget, "{name}");
            }

            // Unsupported `agy` + `cloud` fails closed with HTTP 400.
            let bad_req: CreateAgentRequest = serde_json::from_value(json!({
                "name": "bad-agy-cloud",
                "harness": "agy",
                "runtime": "cloud"
            }))
            .unwrap();
            let err = create_agent_route(
                Principal::Operator,
                State(state.clone()),
                HeaderMap::new(),
                Json(bad_req),
            )
            .await
            .unwrap_err();
            assert_eq!(err.status, StatusCode::BAD_REQUEST);
            assert!(state.store.agent_by_name("bad-agy-cloud").is_err());
        });
    }

    #[test]
    fn runtime_route_switches_runtime_and_budget_with_guards() {
        let _env = crate::server::tests::env_lock();
        let dir = crate::test_tempdir();
        let state = test_state(dir.path());
        let spiron = state
            .store
            .create_agent(NewAgent::new("rt-spiron", "spiron"))
            .unwrap();
        let agy = state
            .store
            .create_agent(NewAgent::new("rt-agy", "agy"))
            .unwrap();

        tokio::runtime::Runtime::new().unwrap().block_on(async {
            // Missing `public_url` rejects switching to `cloud`.
            let err = runtime_route(
                Principal::Operator,
                State(state.clone()),
                AxumPath(spiron.name.clone()),
                HeaderMap::new(),
                Json(serde_json::from_value(json!({"runtime": "cloud"})).unwrap()),
            )
            .await
            .unwrap_err();
            assert_eq!(err.status, StatusCode::BAD_REQUEST);

            std::fs::write(
                dir.path().join(crate::remote::CONFIG_FILE),
                "[server]\npublic_url = \"https://agents.example.com\"\n",
            )
            .unwrap();

            // Switch `spiron` local -> cloud with budget cap.
            let Json(switched) = runtime_route(
                Principal::Operator,
                State(state.clone()),
                AxumPath(spiron.name.clone()),
                HeaderMap::new(),
                Json(
                    serde_json::from_value(
                        json!({"runtime": "cloud", "budget_cap_micros": 1_500_000}),
                    )
                    .unwrap(),
                ),
            )
            .await
            .unwrap();
            assert_eq!(switched["harness"], "spiron");
            assert_eq!(switched["runtime"], "cloud");
            assert_eq!(switched["remote_budget_micros"], 1_500_000);

            // Switch `agy` -> cloud fails closed with HTTP 400.
            let bad = runtime_route(
                Principal::Operator,
                State(state.clone()),
                AxumPath(agy.name.clone()),
                HeaderMap::new(),
                Json(serde_json::from_value(json!({"runtime": "cloud"})).unwrap()),
            )
            .await
            .unwrap_err();
            assert_eq!(bad.status, StatusCode::BAD_REQUEST);

            // Switching to `local` over HTTP MCP fails closed with HTTP 403.
            let mut http_headers = HeaderMap::new();
            http_headers.insert(
                HeaderName::from_static(crate::mcp::HTTP_MCP_HEADER),
                axum::http::HeaderValue::from_static("1"),
            );
            let forbidden = runtime_route(
                Principal::Operator,
                State(state.clone()),
                AxumPath(spiron.name.clone()),
                http_headers,
                Json(serde_json::from_value(json!({"runtime": "local"})).unwrap()),
            )
            .await
            .unwrap_err();
            assert_eq!(forbidden.status, StatusCode::FORBIDDEN);
        });
    }

    #[test]
    fn serverless_spiron_cloud_dispatches_with_zero_sandboxes_and_enforces_budget() {
        let _env = crate::server::tests::env_lock();
        let dir = crate::test_tempdir();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let spy_calls = Arc::new(AtomicUsize::new(0));
        let spy_provider: Arc<dyn AsyncSandboxProvider> = Arc::new(ZeroSandboxSpyProvider {
            calls: Arc::clone(&spy_calls),
        });

        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let mock_spiron = axum::Router::new()
                .route(
                    "/v1/agents",
                    axum::routing::post(|| async { Json(json!({ "id": "spiron_agt_orth_1" })) }),
                )
                .route(
                    "/v1/agents/{aid}/messages",
                    axum::routing::post(
                        |axum::extract::Path(aid): axum::extract::Path<String>| async move {
                            Json(json!({
                                "id": "rcpt_orth_cloud_1",
                                "agent_id": aid,
                                "status": "submitted"
                            }))
                        },
                    ),
                );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            tokio::spawn(async move {
                let _ = axum::serve(listener, mock_spiron).await;
            });

            let agent = store
                .create_agent(
                    NewAgent::new("orth-spiron-cloud", "spiron")
                        .with_runtime(ExecutionRuntime::Cloud)
                        .with_budget_micros(500_000),
                )
                .unwrap();
            assert_eq!(agent.harness, "spiron");
            assert_eq!(agent.runtime, ExecutionRuntime::Cloud);

            let mut req = ServerlessDispatchInput::for_agent(agent.clone(), "Ship cloud turn");
            req.callback_base_url = Some("https://coord.example".into());
            req.cloud_endpoint = Some(format!("http://{addr}"));
            req.sandbox_provider = Some(Arc::clone(&spy_provider));

            let dispatched = dispatch_serverless_turn(Arc::clone(&store), req)
                .await
                .expect("orthogonal (spiron, cloud) must dispatch to SpironServerlessDriver");
            assert_eq!(
                spy_calls.load(Ordering::SeqCst),
                0,
                "cloud turn must allocate 0 sandboxes"
            );
            assert_eq!(dispatched.turn.harness_kind, "spiron");
            assert_eq!(
                dispatched.turn.remote_run_id.as_deref(),
                Some("rcpt_orth_cloud_1")
            );
            let meta = dispatched.turn.metadata_json.as_ref().unwrap();
            assert_eq!(meta["agent_runtime"], "cloud");
            assert_eq!(meta["remote_budget_micros"], 500_000);

            // Once cumulative spend reaches the cap, dispatch fails closed before calling driver or sandbox.
            store
                .record_usage(NewUsage {
                    agent_id: agent.id.clone(),
                    harness: "spiron".into(),
                    model: None,
                    input_tokens: 100,
                    output_tokens: 50,
                    cache_read_tokens: 0,
                    cache_write_tokens: 0,
                    reported_cost_micros: Some(500_000),
                    estimated_cost_micros: None,
                    billing_mode: BillingMode::ApiKey,
                    quota_state: None,
                    day: "2026-11-20".into(),
                })
                .unwrap();
            let mut over_req = ServerlessDispatchInput::for_agent(agent, "Should fail budget");
            over_req.callback_base_url = Some("https://coord.example".into());
            over_req.cloud_endpoint = Some(format!("http://{addr}"));
            over_req.sandbox_provider = Some(spy_provider);
            let err = dispatch_serverless_turn(store, over_req).await.unwrap_err();
            assert!(err.contains("remote spend budget exceeded"), "err={err}");
            assert_eq!(spy_calls.load(Ordering::SeqCst), 0);
        });
    }
}
