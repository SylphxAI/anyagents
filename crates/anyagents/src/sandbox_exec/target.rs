//! Resolve the coordinator-only target for an agent and fence durable resources
//! to the target that created them. Provider instances are request-scoped, so
//! credential deletion and rotation do not leave a shared stale client cache.
use super::*;
use anyagents_core::sandbox_targets::SandboxCredential;

pub(crate) fn validate_credential(input: &SandboxCredential) -> Result<(), String> {
    let token = &input.token;
    if token.is_empty()
        || token.len() > 4096
        || !token
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"-._~+/=".contains(&c))
    {
        return Err("invalid sandbox bearer credential".into());
    }
    let fields: Vec<_> = input.parent.split('/').collect();
    if !matches!(fields.as_slice(), ["orgs", _, "projects", _, "envs", _])
        || [1, 3, 5].into_iter().any(|i| {
            fields[i].is_empty()
                || !fields[i]
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || b"_-".contains(&c))
        })
    {
        return Err("invalid sandbox parent resource".into());
    }
    if let Some(url) = &input.base_url {
        validate_endpoint(url)?;
    }
    Ok(())
}

fn validate_endpoint(raw: &str) -> Result<(), String> {
    let invalid = || {
        "sandbox endpoint must be an HTTPS base URL without user info, query or fragment"
            .to_string()
    };
    let url = url::Url::parse(raw).map_err(|_| invalid())?;
    let loopback = matches!(url.host_str(), Some("127.0.0.1" | "[::1]"));
    if raw != raw.trim()
        || (url.scheme() != "https" && !(url.scheme() == "http" && loopback))
        || url.host_str().is_none()
        || url.authority().contains('@')
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(invalid());
    }
    Ok(())
}

pub(crate) fn resolve_registry_for_binding(
    store: &Store,
    binding: &SandboxBinding,
) -> Result<ProviderRegistry, String> {
    resolve_scoped(store, &binding.agent_id, Some(binding))?.map_or_else(
        || {
            ProviderRegistry::from_env_or_config(
                Some(store.data_dir()),
                Some(&overrides_from_binding(binding)),
            )
        },
        Ok,
    )
}

pub(crate) fn resolve_registry_for_agent(
    store: &Store,
    id: &AgentId,
) -> Result<Option<ProviderRegistry>, String> {
    let binding = store.sandbox_binding(id).map_err(|e| e.to_string())?;
    registry_for_agent(store, id, binding.as_ref())
}

pub(super) fn registry_for_agent(
    store: &Store,
    id: &AgentId,
    binding: Option<&SandboxBinding>,
) -> Result<Option<ProviderRegistry>, String> {
    if let Some(reg) = resolve_scoped(store, id, binding)? {
        return Ok(Some(reg));
    }
    if let Some(reg) = resolve_remote_provider_registry(Some(store.data_dir()))? {
        return Ok(Some(reg));
    }
    binding
        .filter(|b| b.provider != "legacy_http" && b.provider != "local")
        .map(|b| {
            ProviderRegistry::from_env_or_config(
                Some(store.data_dir()),
                Some(&overrides_from_binding(b)),
            )
        })
        .transpose()
}

fn resolve_scoped(
    store: &Store,
    id: &AgentId,
    binding: Option<&SandboxBinding>,
) -> Result<Option<ProviderRegistry>, String> {
    let reference = store
        .sandbox_credential_ref(id)
        .map_err(|e| e.to_string())?;
    if let Some(reference) = reference {
        let credential = store
            .read_sandbox_credential(&reference.name)
            .map_err(|e| e.to_string())?;
        validate_credential(&credential)?;
        let config = anyagents_core::agent_config_dir(store.data_dir(), id);
        if binding.is_none()
            && (config.join("sandbox-sylphx.json").exists() || config.join("sandbox.json").exists())
        {
            return Err(
                "restore the previous sandbox target before operating on legacy resources".into(),
            );
        }
        let mut overrides = binding.map(overrides_from_binding).unwrap_or_default();
        overrides.endpoint = Some(
            credential
                .base_url
                .unwrap_or_else(|| "https://api.sylphx.com".into()),
        );
        overrides.parent = Some(credential.parent);
        overrides.token = Some(credential.token);
        overrides.provider = Some(ProviderKind::Sylphx.as_str().into());
        let registry =
            ProviderRegistry::for_scoped_target(store.data_dir(), &overrides, &reference.name)?;
        fence(binding, registry.active().credential_target().as_ref())?;
        Ok(Some(registry))
    } else {
        fence(binding, None)?;
        Ok(None)
    }
}

/// The credential reference alone is not enough: a key rotated in place may
/// not change the server or resource parent for an existing volume.
fn fence(binding: Option<&SandboxBinding>, target: Option<&Value>) -> Result<(), String> {
    let Some(binding) = binding else {
        return Ok(());
    };
    let spec = binding_spec_value(binding);
    let pinned = spec.get("credential_target");
    if (pinned.is_some()
        || !binding.volume_id.is_empty()
        || !binding.volume_resource.is_empty()
        || binding.lease_id.is_some()
        || binding.lease_resource.is_some())
        && (pinned != target
            || (target.is_some() && binding.provider != ProviderKind::Sylphx.as_str()))
    {
        return Err("sandbox credential target changed; restore the previous target before operating on this lease or volume".into());
    }
    Ok(())
}

pub(super) fn verify_provider_target(
    store: &Store,
    agent: &AgentId,
    binding: Option<&SandboxBinding>,
    provider: &dyn AsyncSandboxProvider,
) -> Result<(), String> {
    let actual = provider.credential_target();
    let expected = resolve_scoped(store, agent, binding)?.map(|r| r.active());
    if actual != expected.as_ref().and_then(|p| p.credential_target())
        || provider.scoped_lease_client().and_then(|c| c.token)
            != expected.and_then(|p| p.scoped_lease_client()?.token)
    {
        return Err("sandbox credential target changed; retry with the current target".into());
    }
    Ok(())
}

pub(crate) fn destroy_sandbox_for_agent(
    store: &Store,
    id: &AgentId,
    config: &Path,
) -> Result<(), String> {
    let binding = store.sandbox_binding(id).map_err(|e| e.to_string())?;
    if let Some(binding) = binding.as_ref()
        && binding_spec_value(binding)
            .get("credential_target")
            .is_some()
    {
        let registry = resolve_registry_for_binding(store, binding)?;
        let handle = sandbox_handle_from_binding(binding).ok_or("invalid sandbox lease binding")?;
        block_on_provider(registry.active().destroy(&handle))?;
        store
            .sandbox_binding_store()
            .delete(id)
            .map_err(|e| e.to_string())?;
        return Ok(());
    }
    if store
        .sandbox_credential_ref(id)
        .map_err(|e| e.to_string())?
        .is_none()
    {
        crate::sandbox_providers::destroy_sandbox_for_config_dir(config)?;
    } else if binding.is_some()
        || config.join("sandbox-sylphx.json").exists()
        || config.join("sandbox.json").exists()
    {
        return Err("restore the original sandbox target before deleting its resources".into());
    }
    Ok(())
}

pub(crate) fn validate_scoped_endpoint(raw: &str) -> Result<(), String> {
    validate_endpoint(raw)
}

pub(super) fn binding_spec_value(binding: &SandboxBinding) -> Value {
    serde_json::from_str::<Value>(&binding.spec_json).unwrap_or_else(|_| json!({}))
}

fn overrides_from_binding(binding: &SandboxBinding) -> SandboxRegistryConfig {
    let spec = binding_spec_value(binding);
    let spec_str = |key: &str| {
        spec.get(key)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
    };
    SandboxRegistryConfig {
        provider: (!binding.provider.trim().is_empty()).then(|| binding.provider.clone()),
        endpoint: spec_str("endpoint"),
        image: binding.image.clone(),
        template: binding.template.clone(),
        shape: spec_str("shape"),
        parent: spec_str("parent"),
        volume_size_gib: binding.volume_size_gib.and_then(|v| u32::try_from(v).ok()),
        idle_ttl_ms: (binding.idle_ttl_ms >= 0).then_some(binding.idle_ttl_ms as u64),
        max_lease_ttl_ms: spec.get("max_lease_ttl_ms").and_then(Value::as_u64),
        mount_path: spec_str("mount_path"),
        workdir: (!binding.remote_workdir.trim().is_empty())
            .then(|| binding.remote_workdir.clone()),
        ..SandboxRegistryConfig::default()
    }
}

#[cfg(test)]
mod tests;
