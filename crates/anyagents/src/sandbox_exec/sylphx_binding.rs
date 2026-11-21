use super::*;

pub(super) fn sylphx_lease_from_handle(
    handle: &SandboxHandle,
    registry: &ProviderRegistry,
    binding: Option<&SandboxBinding>,
    data_dir: Option<&Path>,
    config_dir: &Path,
) -> Result<SylphxLeaseConfig, String> {
    let spec = binding.map(binding_spec_value).unwrap_or_else(|| json!({}));
    let existing = SylphxLeaseConfig::from_config_dir(config_dir);
    let sylphx_cfg = SylphxProviderConfig::resolve(data_dir).ok().flatten();
    let scoped = registry.active().scoped_lease_client();
    let endpoint = scoped
        .as_ref()
        .map(|c| c.endpoint.clone())
        .or_else(|| {
            spec.get("endpoint")
                .and_then(Value::as_str)
                .filter(|s| !s.trim().is_empty())
                .map(str::to_string)
        })
        .or_else(|| sylphx_cfg.as_ref().map(|c| c.endpoint.clone()))
        .or_else(|| existing.as_ref().map(|c| c.endpoint.clone()))
        .or_else(|| env_non_empty("ANYAGENTS_SANDBOX_ENDPOINT"))
        .or_else(|| env_non_empty("SYLPHX_SANDBOX_URL"))
        .or_else(|| env_non_empty("ANYAGENTS_SYLPHX_ENDPOINT"))
        .unwrap_or_else(|| "https://api.sylphx.com".to_string());
    let token = if let Some(client) = scoped {
        client.token
    } else {
        sylphx_cfg
            .as_ref()
            .and_then(|c| c.token.clone())
            .or_else(|| existing.as_ref().and_then(|c| c.token.clone()))
            .or_else(|| env_non_empty("ANYAGENTS_SANDBOX_TOKEN"))
            .or_else(|| env_non_empty("SYLPHX_API_KEY"))
            .or_else(|| env_non_empty("ANYAGENTS_SYLPHX_TOKEN"))
    };
    let parent = handle
        .metadata
        .get("parent")
        .cloned()
        .or_else(|| {
            spec.get("parent")
                .and_then(Value::as_str)
                .filter(|s| !s.trim().is_empty())
                .map(str::to_string)
        })
        .or_else(|| sylphx_cfg.as_ref().map(|c| c.parent.clone()))
        .or_else(|| existing.as_ref().map(|c| c.parent.clone()))
        .or_else(|| env_non_empty("ANYAGENTS_SYLPHX_PARENT"))
        .ok_or_else(|| "Sylphx sandbox handle missing `parent` resource".to_string())?;
    let template = binding
        .and_then(|b| b.template.clone())
        .or_else(|| registry.default_image_spec().template.clone())
        .or_else(|| sylphx_cfg.as_ref().map(|c| c.template.clone()))
        .or_else(|| existing.as_ref().map(|c| c.template.clone()))
        .unwrap_or_else(|| crate::sandbox_providers::sylphx::DEFAULT_TEMPLATE.to_string());
    let shape = spec
        .get("shape")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .map(str::to_string)
        .or_else(|| sylphx_cfg.as_ref().map(|c| c.shape.clone()))
        .or_else(|| existing.as_ref().map(|c| c.shape.clone()))
        .unwrap_or_else(|| crate::sandbox_providers::sylphx::DEFAULT_SHAPE.to_string());
    let volume_id = handle
        .volume_id
        .clone()
        .or_else(|| binding.map(|b| b.volume_id.clone()))
        .unwrap_or_default();
    let volume_resource = handle
        .metadata
        .get("volume_resource")
        .cloned()
        .or_else(|| binding.map(|b| b.volume_resource.clone()))
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| format!("{parent}/volumes/{volume_id}"));
    let lease_resource = handle
        .metadata
        .get("lease_resource")
        .cloned()
        .or_else(|| binding.and_then(|b| b.lease_resource.clone()))
        .or_else(|| Some(format!("{parent}/leases/{}", handle.sandbox_id)));
    let data_plane_resource = handle
        .metadata
        .get("data_plane_resource")
        .cloned()
        .or_else(|| binding.and_then(|b| b.data_plane_resource.clone()))
        .or_else(|| lease_resource.clone());
    Ok(SylphxLeaseConfig {
        scoped: registry.active().credential_target().is_some(),
        endpoint,
        token,
        parent,
        template,
        shape,
        volume_size_gib: binding
            .and_then(|b| b.volume_size_gib)
            .and_then(|v| u64::try_from(v).ok()),
        workdir: handle.workdir.clone(),
        mount_path: handle.mount_path.clone(),
        volume_id,
        volume_resource,
        lease_id: (!handle.sandbox_id.is_empty()).then(|| handle.sandbox_id.clone()),
        lease_resource,
        data_plane_resource,
        envd_url: handle.transport_url.clone(),
        envd_token: handle.transport_token.clone(),
        expires_at: handle.expires_at.clone(),
    })
}

pub(crate) fn client_for_binding(
    store: &Store,
    binding: &SandboxBinding,
) -> Result<
    (
        crate::sandbox_providers::sylphx::SylphxSandboxClient,
        String,
    ),
    String,
> {
    let handle =
        sandbox_handle_from_binding(binding).ok_or_else(|| "invalid Sylphx binding".to_string())?;
    let registry = resolve_registry_for_binding(store, binding)?;
    let config_dir = anyagents_core::agent_config_dir(store.data_dir(), &binding.agent_id);
    let cfg = sylphx_lease_from_handle(
        &handle,
        &registry,
        Some(binding),
        Some(store.data_dir()),
        &config_dir,
    )?;
    Ok((cfg.client(), cfg.parent))
}
