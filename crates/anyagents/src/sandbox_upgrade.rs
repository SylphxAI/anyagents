//! Sylphx compute recycling. The same process lock excludes local vendors and
//! serverless admission while deciding whether an existing lease can be released.
use anyagents_core::stateless::SandboxBinding;
use anyagents_core::{AgentId, Error, Store};
use serde::Serialize;

use crate::sandbox_exec::{release_sandbox_binding_with_provider, resolve_registry_for_binding};
use crate::sandbox_providers::image_version::{LeaseVersion, desired_ref};
use crate::sandbox_providers::providers::{
    ImageSpec, ProviderKind, ProviderRegistry, SandboxProvider as AsyncSandboxProvider,
};
use crate::sandbox_providers::sylphx::DEFAULT_TEMPLATE;

const NS: &str = "markers";
pub(crate) const DRAIN: &str = "sandbox-drain-lease";
/// Guest heartbeats fire every 5s while live; an older `inflight_children > 0`
/// snapshot with no active turn is stale (e.g. a completed or dead guest) and
/// must not block turn-boundary lease recycling indefinitely.
const INFLIGHT_HEARTBEAT_FRESHNESS_MS: i64 = 60_000;

fn desired_image(store: &Store, binding: Option<&SandboxBinding>) -> String {
    desired_image_ref(
        configured_image_spec(store).as_ref(),
        binding.and_then(|b| b.image.as_deref()),
        binding.and_then(|b| b.template.as_deref()),
    )
}

pub(crate) fn configured_image_spec(store: &Store) -> Option<ImageSpec> {
    ProviderRegistry::from_env_or_config(Some(store.data_dir()), None)
        .ok()
        .map(|registry| registry.default_image_spec().clone())
}

pub(crate) fn desired_image_ref(
    configured: Option<&ImageSpec>,
    image: Option<&str>,
    template: Option<&str>,
) -> String {
    let (image, template) = match configured {
        Some(spec) => (spec.image.as_deref(), spec.template.as_deref()),
        None => (image, template),
    };
    desired_ref(image, template.unwrap_or(DEFAULT_TEMPLATE))
}

fn version(binding: &SandboxBinding) -> LeaseVersion {
    version_from_spec(&binding.spec_json)
}

pub(crate) fn version_from_spec(spec_json: &str) -> LeaseVersion {
    serde_json::from_str::<serde_json::Value>(spec_json)
        .ok()
        .and_then(|spec| {
            serde_json::from_str(spec.pointer("/metadata/lease_version")?.as_str()?).ok()
        })
        .unwrap_or_default()
}

pub(crate) fn lease_key(binding: &SandboxBinding) -> Option<&str> {
    binding
        .lease_resource
        .as_deref()
        .or(binding.lease_id.as_deref())
}

pub(crate) fn requested(store: &Store, binding: &SandboxBinding) -> Result<bool, Error> {
    let Some(key) = lease_key(binding) else {
        return Ok(false);
    };
    Ok(store
        .blob_store()
        .get_agent_file(&binding.agent_id, NS, DRAIN)?
        .is_some_and(|blob| blob.content == key.as_bytes()))
}

pub(crate) fn has_active_turn(store: &Store, agent: &AgentId) -> Result<bool, Error> {
    Ok(store.has_open_turn(agent)?
        || !store
            .remote_turn_store()
            .list_active_for_agent(agent)?
            .is_empty())
}

pub(crate) fn heartbeat_has_live_children(
    inflight_children: i64,
    heartbeat_at_ms: Option<i64>,
    now_ms: i64,
) -> bool {
    inflight_children > 0
        && heartbeat_at_ms
            .is_some_and(|ts| now_ms.saturating_sub(ts) <= INFLIGHT_HEARTBEAT_FRESHNESS_MS)
}

fn heartbeat_reports_live_children(hb: &anyagents_core::RuntimeHeartbeat, now: i64) -> bool {
    heartbeat_has_live_children(hb.inflight_children.into(), hb.heartbeat_at_ms, now)
}

pub(crate) fn busy(store: &Store, agent: &AgentId) -> Result<bool, Error> {
    if has_active_turn(store, agent)? {
        return Ok(true);
    }
    let now = anyagents_core::now_ms();
    Ok(store
        .runtime_heartbeat(agent)?
        .is_some_and(|hb| heartbeat_reports_live_children(&hb, now)))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Action {
    RecycledNow,
    DrainScheduled,
    AlreadyUpToDate,
    ColdNextTurnLatest,
}

/// Caller holds the agent process lock AND has not persisted the new turn yet.
pub(crate) fn recycle_at_boundary(
    store: &Store,
    agent: &AgentId,
    provider: Option<&dyn AsyncSandboxProvider>,
) -> Result<Action, String> {
    let Some(mut binding) = store.sandbox_binding(agent).map_err(|e| e.to_string())? else {
        return Ok(Action::ColdNextTurnLatest);
    };
    if lease_key(&binding).is_none() {
        return Ok(Action::ColdNextTurnLatest);
    }
    if binding.provider != "sylphx" || provider.is_some_and(|p| p.kind() != ProviderKind::Sylphx) {
        return Ok(Action::AlreadyUpToDate);
    }
    let desired = desired_image(store, Some(&binding));
    if !requested(store, &binding).map_err(|e| e.to_string())? {
        let recorded = version(&binding);
        if recorded.matches(&desired) {
            return Ok(Action::AlreadyUpToDate);
        }
        // Bindings from older coordinators lack observations. Ask the provider
        // before recycling one that might already match the configured image.
        if recorded.image.is_none() && recorded.digest().is_none() {
            let (client, parent) = crate::sandbox_exec::sylphx_client_for_binding(store, &binding)?;
            if let Some(lease) =
                client.get_lease(&parent, lease_key(&binding).unwrap_or_default())?
            {
                persist_observation(store, &mut binding, &lease)?;
                if !lease.is_terminal() && lease.version.matches(&desired) {
                    return Ok(Action::AlreadyUpToDate);
                }
            }
        }
    }
    if busy(store, agent).map_err(|e| e.to_string())? {
        let key = lease_key(&binding).unwrap_or_default();
        store
            .blob_store()
            .put_agent_file(agent, NS, DRAIN, key.as_bytes(), false)
            .map_err(|e| e.to_string())?;
        return Ok(Action::DrainScheduled);
    }
    let resolved;
    let provider = match provider {
        Some(provider) => provider,
        None => {
            resolved = resolve_registry_for_binding(store, &binding)?.active();
            resolved.as_ref()
        }
    };
    let config_dir = anyagents_core::agent_config_dir(store.data_dir(), agent);
    release_sandbox_binding_with_provider(store, &binding, provider, Some(&config_dir))?;
    // Clear any stale guest child-process count from the released lease so
    // subsequent version checks and turns see a clean idle runtime state.
    let now = anyagents_core::now_ms();
    let _ = store.update_runtime_heartbeat(
        agent,
        &anyagents_core::RuntimeHeartbeat {
            owner_replica_id: Some(store.replica_id().to_string()),
            turn_state: "idle".to_string(),
            retry_at_ms: None,
            heartbeat_at_ms: Some(now),
            idle_since_ms: Some(now),
            inflight_children: 0,
        },
    );
    // The marker is scoped to the released lease identity, so a failed delete
    // cannot accidentally request an upgrade on a newly attached lease.
    let _ = store.blob_store().delete_agent_file(agent, NS, DRAIN);
    Ok(Action::RecycledNow)
}

pub(crate) fn request_drain(store: &Store, agent: &AgentId) -> Result<Action, String> {
    match store.try_lock_agent(agent) {
        Ok(_guard) => recycle_at_boundary(store, agent, None),
        Err(Error::AgentBusy) => {
            let Some(binding) = store.sandbox_binding(agent).map_err(|e| e.to_string())? else {
                return Ok(Action::ColdNextTurnLatest);
            };
            let Some(key) = lease_key(&binding) else {
                return Ok(Action::ColdNextTurnLatest);
            };
            if binding.provider != "sylphx"
                || version(&binding).matches(&desired_image(store, Some(&binding)))
            {
                return Ok(Action::AlreadyUpToDate);
            }
            store
                .blob_store()
                .put_agent_file(agent, NS, DRAIN, key.as_bytes(), false)
                .map_err(|e| e.to_string())?;
            // The worker may have ended just as the request arrived.
            match store.try_lock_agent(agent) {
                Ok(_guard) => recycle_at_boundary(store, agent, None),
                Err(Error::AgentBusy) => Ok(Action::DrainScheduled),
                Err(e) => Err(e.to_string()),
            }
        }
        Err(e) => Err(e.to_string()),
    }
}

pub(crate) fn reconcile_ready_bindings(store: &Store) -> Result<(), Error> {
    for binding in store.sandbox_binding_store().list_active_ready()? {
        if binding.provider == "sylphx"
            && (requested(store, &binding)?
                || !version(&binding).matches(&desired_image(store, Some(&binding))))
        {
            reconcile_candidate(store, &binding.agent_id);
        }
    }
    Ok(())
}

fn reconcile_candidate(store: &Store, agent: &AgentId) {
    if let Ok(_guard) = store.try_lock_agent(agent)
        && let Err(err) = recycle_at_boundary(store, agent, None)
    {
        eprintln!("anyagents-sandbox: turn-boundary drain failed for {agent}: {err}");
    }
}

/// The renewal timer must not use Ensure: an expired or stale lease must not
/// trigger guest replacement or layout initialization while a turn owns it.
pub(crate) fn renew_active_binding(store: &Store, agent: &AgentId) -> Result<bool, String> {
    let _guard = store.try_lock_agent(agent).map_err(|e| e.to_string())?;
    let Some(mut binding) = store.sandbox_binding(agent).map_err(|e| e.to_string())? else {
        return Ok(false);
    };
    if binding.provider != "sylphx" || binding.lease_state != "ready" {
        return Ok(false);
    }
    if requested(store, &binding).map_err(|e| e.to_string())? {
        recycle_at_boundary(store, agent, None)?;
        return Ok(false);
    }
    let Some(key) = lease_key(&binding).map(str::to_owned) else {
        return Ok(false);
    };
    let (client, parent) = crate::sandbox_exec::sylphx_client_for_binding(store, &binding)?;
    let Some(lease) = client.get_lease(&parent, &key)? else {
        return Ok(false);
    };
    let desired = desired_image(store, Some(&binding));
    let mut renew = None;
    if !lease.is_terminal() && lease.version.matches(&desired) {
        renew = Some(client.renew_matching_lease(&lease, &desired, None)?);
    }
    let observed = renew.as_ref().unwrap_or(&lease);
    persist_observation(store, &mut binding, observed)?;
    if !observed.version.matches(&desired) {
        recycle_at_boundary(store, agent, None)?;
    }
    Ok(renew.is_some())
}

// A running standalone shim owns its stream. Renew the current lease only;
// never recycle it on this path, nor use a stale token or a local mirror.
pub(crate) fn renew_scoped_lease(store: &Store, agent: &AgentId) -> Result<(), String> {
    let Some(binding) = store.sandbox_binding(agent).map_err(|e| e.to_string())? else {
        return Ok(());
    };
    let Some(key) = lease_key(&binding) else {
        return Ok(());
    };
    let (client, parent) = crate::sandbox_exec::sylphx_client_for_binding(store, &binding)?;
    if let Some(lease) = client.get_lease(&parent, key)?
        && !lease.is_terminal()
    {
        client.renew_matching_lease(&lease, &desired_image(store, Some(&binding)), None)?;
    }
    Ok(())
}

fn persist_observation(
    store: &Store,
    binding: &mut SandboxBinding,
    observed: &crate::sandbox_providers::sylphx::SylphxLease,
) -> Result<(), String> {
    let mut spec: serde_json::Value =
        serde_json::from_str(&binding.spec_json).map_err(|e| e.to_string())?;
    if spec["metadata"].is_null() {
        spec["metadata"] = serde_json::json!({});
    }
    spec["metadata"]["lease_version"] = serde_json::Value::String(
        serde_json::to_string(&observed.version).map_err(|e| e.to_string())?,
    );
    binding.spec_json = spec.to_string();
    binding.lease_expires_at_ms = observed
        .expires_at
        .as_deref()
        .and_then(|v| v.parse::<jiff::Timestamp>().ok())
        .map(|ts| ts.as_millisecond())
        .or(binding.lease_expires_at_ms);
    store
        .sandbox_binding_store()
        .upsert(binding)
        .map(|_| ())
        .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyagents_core::{NewAgent, NewRemoteTurn, SettleRemoteTurn};
    use axum::{Json, Router, extract::Request, http::StatusCode, response::IntoResponse};
    use serde_json::json;
    use std::sync::{Arc, Mutex};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sandbox_lease_upgrades_preserve_busy_turn_and_volume() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let observed_calls = Arc::clone(&calls);
        let desired = format!("sha256:{}", "a".repeat(64));
        let observed_digest = desired.clone();
        let app = Router::new().fallback(move |req: Request| {
            let calls = Arc::clone(&observed_calls);
            let matching = observed_digest.clone();
            async move {
                let path = req.uri().path();
                calls.lock().unwrap().push(format!("{} {path}", req.method()));
                let lease = path.rsplit('/').next().unwrap().split(':').next().unwrap();
                let image_digest = if lease == "current" { matching } else { format!("sha256:{}", "b".repeat(64)) };
                match (req.method().as_str(), lease) {
                    ("GET", "current" | "idle" | "busy") => Json(json!({
                        "name": format!("orgs/o/projects/p/envs/e/leases/{lease}"),
                        "status": {"state":"READY", "imageDigest": image_digest, "harnessVersion":"1.2.3"}
                    })).into_response(),
                    ("POST", "idle" | "busy") if path.ends_with(":release") => StatusCode::NO_CONTENT.into_response(),
                    ("POST", "current") if path.ends_with(":renew") => Json(json!({
                        "name": "orgs/o/projects/p/envs/e/leases/current",
                        "status": {"state":"READY", "expiresAt":"2099-01-01T00:00:00Z"}
                    })).into_response(),
                    _ => StatusCode::BAD_REQUEST.into_response(),
                }
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        tokio::task::spawn_blocking(move || {
            let _env = crate::server::tests::env_lock();
            let dir = tempfile::tempdir().unwrap();
            let store = Store::open(dir.path().join("agents.db")).unwrap();
            let parent = "orgs/o/projects/p/envs/e";
            std::fs::write(
                store.data_dir().join("sandbox-provider.json"),
                json!({
                    "provider":"sylphx", "endpoint":endpoint, "parent":parent, "image":desired
                })
                .to_string(),
            )
            .unwrap();
            let bind = |name: &str| {
                let agent = store.create_agent(NewAgent::new(name, "codex")).unwrap();
                let binding: SandboxBinding = serde_json::from_value(json!({
                    "agent_id":agent.id, "provider":"sylphx", "image":desired,
                    "spec_json":json!({"endpoint":endpoint,"parent":parent}).to_string(),
                    "volume_id":format!("volume-{name}"), "lease_id":name, "lease_state":"ready",
                    "lease_resource":format!("{parent}/leases/{name}")
                }))
                .unwrap();
                store.sandbox_binding_store().upsert(&binding).unwrap();
                agent.id
            };
            let idle = bind("idle");
            let working = bind("busy");
            let current = bind("current");
            let remote = store.remote_turn_store();
            let active = remote
                .create_turn(&NewRemoteTurn {
                    agent_id: working.clone(),
                    turn_id: "turn-busy".into(),
                    harness_kind: "sandbox".into(),
                    provider_kind: Some("sylphx".into()),
                    remote_run_id: None,
                    callback_token_hash: "fixture".into(),
                    cursor: None,
                    status: "running".into(),
                    metadata_json: None,
                    next_poll_at_ms: None,
                })
                .unwrap();
            assert_eq!(request_drain(&store, &idle).unwrap(), Action::RecycledNow);
            assert_eq!(
                request_drain(&store, &idle).unwrap(),
                Action::ColdNextTurnLatest
            );
            assert!(!renew_active_binding(&store, &working).unwrap());
            assert_eq!(
                request_drain(&store, &working).unwrap(),
                Action::DrainScheduled
            );
            assert_eq!(
                request_drain(&store, &working).unwrap(),
                Action::DrainScheduled
            );
            assert_eq!(
                store
                    .sandbox_binding(&working)
                    .unwrap()
                    .unwrap()
                    .lease_id
                    .as_deref(),
                Some("busy")
            );
            assert_eq!(
                calls
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|c| c.starts_with("POST"))
                    .cloned()
                    .collect::<Vec<_>>(),
                [format!("POST /v1/{parent}/leases/idle:release")]
            );
            let payload = SettleRemoteTurn {
                status: "completed".into(),
                result_text: None,
                error_text: None,
                usage_json: None,
                cursor: None,
                settled_at_ms: anyagents_core::now_ms(),
            };
            crate::server::serverless::settle_remote_turn_and_deliver(
                &store,
                &active.turn_id,
                payload,
                None,
                0,
            )
            .unwrap();
            let drained = store.sandbox_binding(&working).unwrap().unwrap();
            assert!(drained.lease_id.is_none());
            assert_eq!(drained.volume_id, "volume-busy");
            assert!(!requested(&store, &drained).unwrap());
            assert!(renew_active_binding(&store, &current).unwrap());
            let matched = store.sandbox_binding(&current).unwrap().unwrap();
            assert_eq!(
                version(&matched).digest().as_deref(),
                Some(desired.as_str())
            );
            assert_eq!(version(&matched).harness_version.as_deref(), Some("1.2.3"));
            assert_eq!(
                request_drain(&store, &current).unwrap(),
                Action::AlreadyUpToDate
            );
            assert_eq!(
                calls
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|c| c.starts_with("POST"))
                    .cloned()
                    .collect::<Vec<_>>(),
                [
                    format!("POST /v1/{parent}/leases/idle:release"),
                    format!("POST /v1/{parent}/leases/busy:release"),
                    format!("POST /v1/{parent}/leases/current:renew"),
                ]
            );
        })
        .await
        .unwrap();
        server.abort();
    }

    #[test]
    fn stale_inflight_children_heartbeat_does_not_block_recycle() {
        let now = 1_795_222_000_000;
        let fresh = anyagents_core::RuntimeHeartbeat {
            owner_replica_id: None,
            turn_state: "idle".to_string(),
            retry_at_ms: None,
            heartbeat_at_ms: Some(now - 5_000),
            idle_since_ms: None,
            inflight_children: 2,
        };
        assert!(heartbeat_reports_live_children(&fresh, now));

        let stale = anyagents_core::RuntimeHeartbeat {
            heartbeat_at_ms: Some(now - INFLIGHT_HEARTBEAT_FRESHNESS_MS - 1_000),
            ..fresh.clone()
        };
        assert!(!heartbeat_reports_live_children(&stale, now));

        let missing_ts = anyagents_core::RuntimeHeartbeat {
            heartbeat_at_ms: None,
            ..fresh.clone()
        };
        assert!(!heartbeat_reports_live_children(&missing_ts, now));

        let zero_children = anyagents_core::RuntimeHeartbeat {
            inflight_children: 0,
            ..fresh
        };
        assert!(!heartbeat_reports_live_children(&zero_children, now));
    }
}
