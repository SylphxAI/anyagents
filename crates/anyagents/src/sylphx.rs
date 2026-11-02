//! Thin coordinator integration wrapper for `anyagents-harness-sylphx`.
//!
//! All `SylphxHarness` loop, ACI, CLM `context_edit`, Zero-Import `code_mode`,
//! Raw-RAG `memory_recall`, Cloud Sandbox, and RFC 8628 auth implementation
//! lives in the dedicated `crates/anyagents-harness-sylphx` adapter crate.

use std::path::Path;

pub use anyagents_harness_sylphx::{
    CLOUD_WORKSPACE_DIR, DEFAULT_SYLPHX_MODEL, SylphxHarness, read_sylphx_key_from_dir,
    release_persisted_sandbox_lease, resolve_sylphx_cloud_access_key, run_device_flow_login,
    run_stdio_adapter,
};

pub fn write_sylphx_config(
    config_dir: &Path,
    model: Option<&str>,
    mcp_command: &Path,
    mcp_args: &[String],
) -> Result<(), anyagents_core::Error> {
    anyagents_harness_sylphx::write_sylphx_config(config_dir, model, mcp_command, mcp_args)?;
    let resolved_model = model
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(DEFAULT_SYLPHX_MODEL);
    let _ = crate::opencode_config::write(config_dir, Some(resolved_model), mcp_command, mcp_args);
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn create_agent_and_subagent_with_sylphx_harness_succeeds() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("data");
        std::fs::create_dir_all(&data).unwrap();
        let store = anyagents_core::Store::open(data.join("state.db")).unwrap();

        let root = crate::create::create(
            &store,
            &data,
            crate::create::CreateRequest {
                name: "sylphx-lead".to_string(),
                role: Some("Sylphx Lead".to_string()),
                harness: "sylphx-agent".to_string(),
                model: Some("sylphx/openai/gpt-6.1-sol#xhigh".to_string()),
                profile: None,
                repo: None,
                branch: None,
                base: None,
                workdir: None,
                workdir_default: None,
                instructions: Some("Lead instructions".to_string()),
                parent_id: None,
            },
        )
        .unwrap();
        assert_eq!(root.harness, "sylphx");
        let root_cfg = anyagents_core::agent_config_dir(&data, &root.id);
        assert!(root_cfg.join("sylphx-agent.json").is_file());

        let sub = crate::create::create(
            &store,
            &data,
            crate::create::CreateRequest {
                name: "sylphx-worker-1".to_string(),
                role: Some("Sylphx Sub-Agent".to_string()),
                harness: "sylphx".to_string(),
                model: Some("sylphx/openai/gpt-6.1-sol#medium".to_string()),
                profile: None,
                repo: None,
                branch: None,
                base: None,
                workdir: None,
                workdir_default: None,
                instructions: Some("Worker instructions".to_string()),
                parent_id: Some(root.id.clone()),
            },
        )
        .unwrap();
        assert_eq!(sub.harness, "sylphx");
        assert_eq!(sub.parent_id.as_ref(), Some(&root.id));
        let sub_cfg = anyagents_core::agent_config_dir(&data, &sub.id);
        assert!(sub_cfg.join("sylphx-agent.json").is_file());
    }
}
