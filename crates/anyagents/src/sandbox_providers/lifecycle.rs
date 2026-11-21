//! Remote workspace teardown under the agent lifecycle lock. Pause/idle do not
//! use this path: they only release compute and always keep persistent storage.

use std::path::Path;

use anyagents_core::stateless::SandboxBinding;
use anyagents_core::{Agent, Store, agent_config_dir, now_ms};

use super::providers::{ExecOutput, ExecRequest, ProviderKind, SandboxHandle};
use super::sylphx::{SYLPHX_STATE_FILE, SylphxLeaseConfig};
use crate::api::RemoteVolumeReap;
use crate::sandbox_exec::{
    block_on_provider, release_sandbox_binding_with_provider, resolve_registry_for_binding,
    sandbox_handle_from_binding, validate_legacy_cleanup_target,
};

const PROBE: &str = include_str!("lifecycle/workspace_probe.sh");

#[derive(Clone, Copy)]
enum Action {
    Delete,
    Archive { can_probe: bool },
}

/// `can_probe` requires custody and no pending writer at archive admission.
pub(crate) fn archive_volume(
    store: &Store,
    agent: &Agent,
    can_probe: bool,
) -> Option<RemoteVolumeReap> {
    volume_result(store, agent, Action::Archive { can_probe })
}

pub(crate) fn delete_volume(store: &Store, agent: &Agent) -> Option<RemoteVolumeReap> {
    volume_result(store, agent, Action::Delete)
}

fn volume_result(store: &Store, agent: &Agent, action: Action) -> Option<RemoteVolumeReap> {
    match cleanup(store, agent, action) {
        Ok(receipt) => receipt,
        Err(err) => {
            let detail = anyagents_core::masking::mask_known_secrets(&err);
            eprintln!(
                "anyagents-sandbox: volume cleanup failed for {}: {detail}",
                agent.id
            );
            Some(receipt(false, detail.as_ref()))
        }
    }
}

fn receipt(reaped: bool, reason: &str) -> RemoteVolumeReap {
    RemoteVolumeReap {
        remote_volume_reaped: reaped,
        remote_volume_reap_reason: reason.into(),
    }
}

#[derive(Clone, Copy)]
enum Policy {
    Reap,
    Preserve(&'static str),
    Verify,
}

fn cleanup(
    store: &Store,
    agent: &Agent,
    action: Action,
) -> Result<Option<RemoteVolumeReap>, String> {
    let cfg = agent_config_dir(store.data_dir(), &agent.id);
    let binding = store
        .sandbox_binding(&agent.id)
        .map_err(|e| format!("cannot read sandbox binding: {e}"))?;
    if binding.as_ref().is_some_and(|b| b.provider == "local") {
        return Ok(None);
    }
    let legacy_path = cfg.join(SYLPHX_STATE_FILE);
    let legacy = if binding.as_ref().is_none_or(|b| b.provider == "sylphx") {
        let legacy = SylphxLeaseConfig::from_config_dir(&cfg);
        if legacy.is_none() && binding.is_none() {
            require_absent(&legacy_path)?;
        }
        legacy
    } else {
        None
    };
    let policy = match action {
        Action::Delete => Policy::Reap,
        Action::Archive { .. } if agent.lifecycle() != "task" => Policy::Preserve("standing agent"),
        Action::Archive { .. }
            if store
                .has_terminal_archive_verdict(&agent.id)
                .map_err(|e| e.to_string())? =>
        {
            Policy::Reap
        }
        Action::Archive { can_probe: true } => Policy::Verify,
        Action::Archive { can_probe: false } => {
            Policy::Preserve("workspace could not be verified while a writer may still be active")
        }
    };
    if let Some(binding) = binding.as_ref().filter(|b| b.provider != "legacy_http") {
        return with_binding(store, &cfg, binding, legacy.as_ref(), policy).map(Some);
    }
    if let Some(legacy) = legacy {
        validate_legacy_cleanup_target(store, &agent.id)?;
        return with_legacy_volume(&cfg, &legacy, policy).map(Some);
    }
    if super::sandbox_config_from_config_dir(&cfg).is_none() {
        require_absent(&cfg.join("sandbox.json"))?;
        return if binding.is_some() {
            Err("legacy sandbox binding has no usable sandbox configuration".into())
        } else {
            Ok(None)
        };
    }
    validate_legacy_cleanup_target(store, &agent.id)?;
    if matches!(action, Action::Delete) {
        super::destroy_sandbox_for_config_dir(&cfg)
            .map_err(|e| format!("sandbox destroy failed: {e}"))?;
        Ok(Some(receipt(
            true,
            "remote sandbox deleted or already absent",
        )))
    } else {
        super::release_sandbox_for_config_dir(&cfg)
            .map_err(|e| format!("sandbox release failed: {e}"))?;
        Ok(Some(receipt(
            false,
            "legacy sandbox cannot verify and delete its volume independently",
        )))
    }
}

fn require_absent(path: &Path) -> Result<(), String> {
    match std::fs::symlink_metadata(path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        _ => Err("cannot read persisted sandbox configuration".into()),
    }
}

fn with_binding(
    store: &Store,
    cfg: &Path,
    binding: &SandboxBinding,
    legacy: Option<&SylphxLeaseConfig>,
    policy: Policy,
) -> Result<RemoteVolumeReap, String> {
    let mut handle =
        sandbox_handle_from_binding(binding).ok_or("unknown persisted sandbox provider")?;
    let registry = resolve_registry_for_binding(store, binding)?;
    if registry.active_kind() != handle.provider {
        return Err("persisted sandbox provider does not match the resolved provider".into());
    }
    let provider = registry.active();
    let has_lease =
        !handle.sandbox_id.trim().is_empty() || handle.metadata.contains_key("lease_resource");
    let ready = has_lease
        && binding.lease_state == "ready"
        && binding
            .lease_expires_at_ms
            .is_none_or(|expiry| expiry > now_ms());
    let decision = decide(policy, ready, || {
        block_on_provider(provider.exec(&handle, &probe_request(&handle.workdir)))
    });
    let token = legacy.and_then(|c| c.token.as_deref());
    let error = |step: &str, err: String| redact_step(step, &err, token);
    let has_volume = handle.volume_id.is_some() || handle.metadata.contains_key("volume_resource");
    let independent = matches!(
        handle.provider,
        ProviderKind::Sylphx | ProviderKind::Modal | ProviderKind::Docker
    );
    if !independent
        && decision.remote_volume_reaped
        && (has_lease || handle.provider != ProviderKind::Daytona)
    {
        // These providers address destruction by sandbox ID. Do not clear it
        // until destroy succeeds; on an already detached binding we cannot
        // honestly report that its volume was deleted.
        if !has_lease && has_volume {
            return Err(
                "provider cannot delete this detached volume without its sandbox ID".into(),
            );
        }
        if has_lease {
            block_on_provider(provider.destroy(&handle))
                .map_err(|e| error("sandbox destroy", e))?;
            store
                .sandbox_binding_store()
                .clear_lease(&binding.agent_id)
                .map_err(|e| e.to_string())?;
        }
        return Ok(decision);
    }
    if has_lease {
        release_sandbox_binding_with_provider(store, binding, provider.as_ref(), Some(cfg))
            .map_err(|e| error("sandbox release", e))?;
    }
    if decision.remote_volume_reaped && has_volume {
        detach_handle(&mut handle);
        if handle.provider == ProviderKind::Docker
            && (handle
                .volume_id
                .as_deref()
                .is_some_and(|v| v.starts_with('/'))
                || handle
                    .metadata
                    .get("bind_mount")
                    .is_some_and(|v| v == "true"))
        {
            return Ok(receipt(
                matches!(policy, Policy::Reap),
                "host bind mount retained; no provider-managed volume to delete",
            ));
        }
        block_on_provider(provider.destroy(&handle)).map_err(|e| error("volume delete", e))?;
    }
    Ok(decision)
}

fn detach_handle(handle: &mut SandboxHandle) {
    handle.sandbox_id.clear();
    handle.metadata.remove("lease_resource");
    handle.metadata.remove("data_plane_resource");
}

fn with_legacy_volume(
    cfg: &Path,
    legacy: &SylphxLeaseConfig,
    policy: Policy,
) -> Result<RemoteVolumeReap, String> {
    let lease = legacy
        .lease_resource
        .as_deref()
        .filter(|r| !r.trim().is_empty());
    let data = legacy.active_data_resource().ok();
    let ready = lease.is_some()
        && data.is_some()
        && legacy.expires_at.as_deref().is_none_or(|s| {
            s.parse::<jiff::Timestamp>()
                .is_ok_and(|t| t.as_millisecond() > now_ms())
        });
    let decision = decide(policy, ready, || {
        let req = probe_request(&legacy.workdir);
        legacy.client().exec(
            data.ok_or("no live data plane")?,
            &req.command,
            req.cwd.as_deref(),
            &req.env,
            req.timeout_ms,
        )
    });
    super::release_sandbox_for_config_dir(cfg)
        .map_err(|e| redact_step("sandbox release", &e, legacy.token.as_deref()))?;
    if decision.remote_volume_reaped
        && let Some(volume) = [&legacy.volume_resource, &legacy.volume_id]
            .into_iter()
            .find(|v| !v.trim().is_empty())
    {
        legacy
            .client()
            .delete_volume(&legacy.parent, volume)
            .map_err(|e| redact_step("volume delete", &e, legacy.token.as_deref()))?;
    }
    Ok(decision)
}

fn redact_step(step: &str, error: &str, token: Option<&str>) -> String {
    let secrets: Vec<_> = token.into_iter().collect();
    let safe = anyagents_core::masking::mask_with_secrets(error, &secrets);
    format!(
        "{step} failed: {}",
        safe.chars().take(400).collect::<String>()
    )
}

fn probe_request(workdir: &str) -> ExecRequest {
    let workdir = if workdir == "/" {
        workdir
    } else {
        workdir.trim_end_matches('/')
    };
    ExecRequest::argv(["/bin/sh", "-c", PROBE, "workspace-probe", workdir])
        .with_cwd(workdir)
        .with_timeout_ms(10_000)
}

fn decide(
    policy: Policy,
    ready: bool,
    probe: impl FnOnce() -> Result<ExecOutput, String>,
) -> RemoteVolumeReap {
    match policy {
        Policy::Reap => receipt(true, "remote volume deleted or already absent"),
        Policy::Preserve(reason) => receipt(false, reason),
        Policy::Verify if !ready => receipt(
            false,
            "lease is detached or not ready; workspace was not verified",
        ),
        Policy::Verify => match probe() {
            Ok(out)
                if out.exit_code == 0
                    && matches!(out.stdout.trim(), "clean and pushed" | "no repository") =>
            {
                receipt(true, out.stdout.trim())
            }
            _ => receipt(
                false,
                "workspace could not be verified clean and pushed (dirty, unpublished, or probe unavailable)",
            ),
        },
    }
}

#[cfg(test)]
mod tests;
