//! Warm-hold and compute release policy; persistent volumes are preserved.

use std::path::Path;
use std::sync::Arc;

use anyagents_core::stateless::SandboxBinding;
use anyagents_core::{AgentId, Error, RuntimeHeartbeat, RuntimeState, Store};

use super::{
    AsyncSandboxProvider, ProviderKind, SylphxLeaseConfig, block_on_provider,
    resolve_registry_for_binding, sandbox_handle_from_binding, takeover_pin,
};

/// Outcome of one singleton 2-tier idle-TTL sandbox sweep pass.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct IdleSweepReport {
    pub inspected: usize,
    pub released: Vec<AgentId>,
    pub skipped_busy: Vec<AgentId>,
    pub failed: Vec<(AgentId, String)>,
}

fn clear_local_sylphx_lease_only(config_dir: &Path) {
    if let Some(mut cfg) = SylphxLeaseConfig::from_config_dir(config_dir) {
        cfg.lease_id = None;
        cfg.lease_resource = None;
        cfg.data_plane_resource = None;
        cfg.envd_url = None;
        cfg.envd_token = None;
        cfg.expires_at = None;
        let _ = cfg.save_to_config_dir(config_dir);
    }
}

/// Release compute for a persisted [`SandboxBinding`] using a concrete provider,
/// preserving `/home/user/agent-data` persistent volumes and clearing only the
/// ephemeral lease fields in `agent_sandbox_bindings`.
pub(crate) fn release_sandbox_binding_with_provider(
    store: &Store,
    binding: &SandboxBinding,
    provider: &dyn AsyncSandboxProvider,
    config_dir: Option<&Path>,
) -> Result<bool, String> {
    let Some(handle) = sandbox_handle_from_binding(binding) else {
        return Ok(false);
    };
    if handle.sandbox_id.trim().is_empty() && binding.lease_resource.is_none() {
        let _ = store.sandbox_binding_store().clear_lease(&binding.agent_id);
        return Ok(false);
    }
    let _ = store.sandbox_binding_store().update_lease_state(
        &binding.agent_id,
        "releasing",
        binding.idle_since_ms,
    );
    if let Err(err) = block_on_provider(provider.release(&handle)) {
        // Restore prior state so the next sweep retries cleanly.
        let _ = store.sandbox_binding_store().update_lease_state(
            &binding.agent_id,
            &binding.lease_state,
            binding.idle_since_ms,
        );
        return Err(err);
    }
    store
        .sandbox_binding_store()
        .clear_lease(&binding.agent_id)
        .map_err(|e| e.to_string())?;
    if let Some(cfg_dir) = config_dir
        && provider.credential_target().is_none()
    {
        clear_local_sylphx_lease_only(cfg_dir);
    }
    Ok(true)
}

/// Release compute for `agent_id` (on explicit pause, archive, or batch idle),
/// preserving persistent volumes. Caller holds the agent lifecycle lock.
pub(crate) fn release_sandbox_for_agent(
    store: &Store,
    agent_id: &AgentId,
    config_dir: &Path,
) -> Result<bool, String> {
    takeover_pin::end_locked(store, agent_id, None).map_err(|e| e.to_string())?;
    if let Some(binding) = store.sandbox_binding(agent_id).map_err(|e| e.to_string())? {
        if binding.lease_id.is_none() && binding.lease_resource.is_none() {
            let _ = store.sandbox_binding_store().clear_lease(agent_id);
            return Ok(false);
        }
        if binding.provider != "legacy_http" && binding.provider != "local" {
            let registry = resolve_registry_for_binding(store, &binding)?;
            return release_sandbox_binding_with_provider(
                store,
                &binding,
                registry.active().as_ref(),
                Some(config_dir),
            );
        }
    }
    if store
        .sandbox_credential_ref(agent_id)
        .map_err(|e| e.to_string())?
        .is_some()
    {
        return Err("cannot release a legacy sandbox with an explicit sandbox credential".into());
    }
    crate::sandbox_providers::release_sandbox_for_config_dir(config_dir)?;
    let _ = store.sandbox_binding_store().clear_lease(agent_id);
    Ok(true)
}

/// Handle supervisor idle notification with 2-tier warm-hold policy:
/// - Never releases while `inflight_children > 0`.
/// - When `idle_ttl_ms > 0`, marks `idle_since_ms = Some(now)` on both
///   `agent_runtime` and `agent_sandbox_bindings` and leaves the compute lease
///   warm until the singleton PG sweeper releases it after `idle_ttl_ms`.
/// - When `idle_ttl_ms == 0` (batch policy), releases compute immediately while
///   preserving the persistent volume.
pub(crate) fn handle_agent_idle_release(store: &Store, agent_id: &AgentId, config_dir: &Path) {
    let Ok(_guard) = store.try_lock_agent(agent_id) else {
        return;
    };
    match crate::sandbox_upgrade::recycle_at_boundary(store, agent_id, None) {
        Ok(
            crate::sandbox_upgrade::Action::RecycledNow
            | crate::sandbox_upgrade::Action::DrainScheduled,
        ) => return,
        Err(err) => {
            eprintln!("anyagents-sandbox: turn-boundary drain failed for {agent_id}: {err}");
            return;
        }
        _ => {}
    }
    if !matches!(crate::sandbox_upgrade::busy(store, agent_id), Ok(false)) {
        return;
    }
    let Ok(_lifecycle) = store.try_lock_agent_lifecycle(agent_id) else {
        return;
    };
    let now = anyagents_core::now_ms();
    if let Ok(Some(hb)) = store.runtime_heartbeat(agent_id)
        && hb.inflight_children > 0
    {
        let _ = store
            .sandbox_binding_store()
            .update_lease_state(agent_id, "ready", None);
        return;
    }

    let Ok(binding) = store.sandbox_binding(agent_id) else {
        return;
    };
    if let Some(binding) = binding {
        if binding.lease_state != "ready" {
            return;
        }
        if binding.idle_ttl_ms > 0
            || !matches!(takeover_pin::holds(store, &binding, now), Ok(false))
        {
            let idle_since = binding.idle_since_ms.unwrap_or(now);
            let _ = store.sandbox_binding_store().update_lease_state(
                agent_id,
                "ready",
                Some(idle_since),
            );
            let prev = store.runtime_heartbeat(agent_id).ok().flatten();
            let _ = store.update_runtime_heartbeat(
                agent_id,
                &RuntimeHeartbeat {
                    owner_replica_id: Some(store.replica_id().to_string()),
                    turn_state: "idle".to_string(),
                    retry_at_ms: prev.as_ref().and_then(|p| p.retry_at_ms),
                    heartbeat_at_ms: Some(now),
                    idle_since_ms: Some(idle_since),
                    inflight_children: 0,
                },
            );
            return;
        }
        if let Err(err) = release_sandbox_for_agent(store, agent_id, config_dir) {
            eprintln!("anyagents-sandbox: immediate batch idle release failed: {err}");
        }
        return;
    }

    if let Err(err) = crate::sandbox_providers::release_sandbox_for_config_dir(config_dir) {
        eprintln!("anyagents-sandbox: idle release failed: {err}");
    }
}

/// Singleton 2-tier idle-TTL sandbox sweeper (invoked under `maint:singleton`).
///
/// Releases idle compute leases whose `idle_since_ms + idle_ttl_ms <= now_ms`
/// ONLY when:
/// - `agent_runtime.inflight_children == 0`
/// - `agent_runtime.turn_state == "idle"` (and no fresher busy heartbeat)
/// - no unfinished turn and no undelivered mailbox message exist
/// - runtime is not actively `starting`
///
/// Always calls `provider.release(&handle)` (NEVER `destroy()`) so the persistent
/// `/home/user/agent-data` volume survives compute release intact.
pub(crate) fn sweep_idle_sandboxes_with_resolver<F>(
    store: &Store,
    data_dir: &Path,
    now_ms: i64,
    mut resolve_provider: F,
) -> Result<IdleSweepReport, Error>
where
    F: FnMut(&SandboxBinding) -> Result<Option<Arc<dyn AsyncSandboxProvider>>, String>,
{
    let mut report = IdleSweepReport::default();
    let due = store.sandbox_binding_store().list_idle_due(now_ms)?;
    for candidate in due {
        report.inspected += 1;
        let _guard = match store.try_lock_agent(&candidate.agent_id) {
            Ok(guard) => guard,
            Err(Error::AgentBusy) => {
                report.skipped_busy.push(candidate.agent_id);
                continue;
            }
            Err(err) => return Err(err),
        };
        let _lifecycle = match store.try_lock_agent_lifecycle(&candidate.agent_id) {
            Ok(guard) => guard,
            Err(Error::AgentBusy) => {
                report.skipped_busy.push(candidate.agent_id);
                continue;
            }
            Err(err) => return Err(err),
        };
        let Some(binding) = store.sandbox_binding(&candidate.agent_id)? else {
            continue;
        };
        let Some(idle_since) = binding.idle_since_ms else {
            continue;
        };
        if binding.lease_state != "ready" || idle_since.saturating_add(binding.idle_ttl_ms) > now_ms
        {
            continue;
        }

        if !matches!(takeover_pin::holds(store, &binding, now_ms), Ok(false)) {
            report.skipped_busy.push(binding.agent_id);
            continue;
        }

        // Fail-closed causal guards: never release compute while remote children,
        // active turns, pending wakes, or startup transitions exist.
        let busy_from_hb = match store.runtime_heartbeat(&binding.agent_id) {
            Ok(Some(hb)) => {
                hb.inflight_children > 0
                    || (!hb.turn_state.trim().is_empty() && hb.turn_state != "idle")
                    || (hb.idle_since_ms.is_none()
                        && hb.heartbeat_at_ms.is_some_and(|t| t >= idle_since))
            }
            Ok(None) => false,
            Err(_) => true,
        };
        let has_unfinished_turn = match store.unfinished_turn(&binding.agent_id) {
            Ok(turn) => turn.is_some(),
            Err(_) => true,
        };
        let has_undelivered = match store.next_undelivered(&binding.agent_id) {
            Ok(msg) => msg.is_some(),
            Err(_) => true,
        };
        let is_starting = match store.agent_runtime(&binding.agent_id) {
            Ok(Some(rt)) => rt.state == RuntimeState::Starting,
            Ok(None) => false,
            Err(_) => true,
        };
        let has_active_remote_turn = match store
            .remote_turn_store()
            .list_active_for_agent(&binding.agent_id)
        {
            Ok(turns) => !turns.is_empty(),
            Err(_) => true,
        };

        if busy_from_hb
            || has_unfinished_turn
            || has_undelivered
            || is_starting
            || has_active_remote_turn
        {
            let _ =
                store
                    .sandbox_binding_store()
                    .update_lease_state(&binding.agent_id, "ready", None);
            report.skipped_busy.push(binding.agent_id);
            continue;
        }

        let cfg_dir = anyagents_core::agent_config_dir(data_dir, &binding.agent_id);
        match resolve_provider(&binding) {
            Ok(Some(provider)) => {
                match release_sandbox_binding_with_provider(
                    store,
                    &binding,
                    provider.as_ref(),
                    Some(&cfg_dir),
                ) {
                    Ok(true) => report.released.push(binding.agent_id),
                    Ok(false) => {}
                    Err(err) => report.failed.push((binding.agent_id, err)),
                }
            }
            Ok(None) => {
                // Legacy file-backed provider (e.g. `legacy_http`).
                match crate::sandbox_providers::release_sandbox_for_config_dir(&cfg_dir) {
                    Ok(()) => {
                        let _ = store.sandbox_binding_store().clear_lease(&binding.agent_id);
                        report.released.push(binding.agent_id);
                    }
                    Err(err) => report.failed.push((binding.agent_id, err)),
                }
            }
            Err(err) => report.failed.push((binding.agent_id, err)),
        }
    }
    Ok(report)
}

/// Run the singleton 2-tier idle-TTL sweeper using [`ProviderRegistry`].
pub(crate) fn sweep_idle_sandboxes(
    store: &Store,
    data_dir: &Path,
    now_ms: i64,
) -> Result<IdleSweepReport, Error> {
    crate::sandbox_upgrade::reconcile_ready_bindings(store)?;
    sweep_idle_sandboxes_with_resolver(store, data_dir, now_ms, |binding| {
        if binding.provider == "legacy_http" || binding.provider == "local" {
            return Ok(None);
        }
        let reg = resolve_registry_for_binding(store, binding)?;
        if reg.active_kind() == ProviderKind::Local {
            return Ok(None);
        }
        Ok(Some(reg.active()))
    })
}
