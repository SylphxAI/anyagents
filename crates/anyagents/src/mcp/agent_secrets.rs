//! MCP helpers for `agents` / `subagents` create, spawn, update (`PATCH /v1/agents/{name}`),
//! and secret grant management (`GET`/`PUT /v1/agents/{name}/secrets`).

use crate::api::{AgentSecretsResponse, AgentView, CreateAgentRequest, PatchAgentRequest};

use super::args::{AgentsArgs, SpawnArgs, SubagentsArgs};
use super::client::Client;

pub(super) fn spawn_args_from_agents(args: AgentsArgs) -> SpawnArgs {
    SpawnArgs {
        name: args.name,
        role: args.role,
        harness: args.harness,
        runtime: args.runtime,
        budget_cap_micros: args.budget_cap_micros,
        model: args.model,
        profile: args.profile,
        repo: args.repo,
        branch: args.branch,
        base: args.base,
        workdir: args.workdir,
        description: args.description,
        instructions: args.instructions,
        scope: args.scope,
        lifecycle: args.lifecycle,
        public: args.public,
        group: args.group,
        prompt: args.prompt,
        browser_profiles: args.browser_profiles,
        secrets: args.secrets,
    }
}

pub(super) fn spawn_args_from_subagents(args: SubagentsArgs) -> SpawnArgs {
    SpawnArgs {
        name: args.agent,
        role: args.role,
        harness: args.harness,
        runtime: args.runtime,
        budget_cap_micros: args.budget_cap_micros,
        model: args.model,
        profile: args.profile,
        repo: args.repo,
        branch: args.branch,
        base: args.base,
        workdir: args.workdir,
        description: args.description,
        instructions: args.instructions,
        scope: args.scope,
        lifecycle: args.lifecycle,
        public: args.public,
        group: args.group,
        prompt: args.prompt,
        browser_profiles: args.browser_profiles,
        secrets: args.secrets,
    }
}

pub(super) fn create_request_from_agents(
    self_agent_id: &str,
    args: AgentsArgs,
) -> Result<CreateAgentRequest, String> {
    let name = args
        .name
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "`agents create` requires `name`".to_string())?
        .to_string();
    let harness = args
        .harness
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("")
        .to_string();
    let parent = match args
        .parent
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        Some(p) if p.eq_ignore_ascii_case("self") => Some(self_agent_id.to_string()),
        Some(p) => Some(p.to_string()),
        None => Some("root".to_string()),
    };
    Ok(CreateAgentRequest {
        name,
        role: args.role,
        harness: Some(harness),
        runtime: args.runtime,
        budget_cap_micros: args.budget_cap_micros,
        model: args.model,
        profile: args.profile,
        repo: args.repo,
        branch: args.branch,
        base: args.base,
        workdir: args.workdir,
        seat_user: None,
        seat_uid: None,
        description: args.description,
        instructions: args.instructions,
        parent,
        scope: args.scope,
        lifecycle: args.lifecycle,
        public: args.public,
        group: args.group,
        prompt: args.prompt,
        browser_profiles: args.browser_profiles,
        secrets: args.secrets,
    })
}

pub(super) fn create_request_from_spawn(
    self_agent_id: &str,
    default_name: String,
    args: SpawnArgs,
) -> CreateAgentRequest {
    let name = args
        .name
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .unwrap_or(default_name);
    let harness = args
        .harness
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("")
        .to_string();
    CreateAgentRequest {
        name,
        role: args.role,
        harness: Some(harness),
        runtime: args.runtime,
        budget_cap_micros: args.budget_cap_micros,
        model: args.model,
        profile: args.profile,
        repo: args.repo,
        branch: args.branch,
        base: args.base,
        workdir: args.workdir,
        seat_user: None,
        seat_uid: None,
        description: args.description,
        instructions: args.instructions,
        parent: Some(self_agent_id.to_string()),
        scope: args.scope,
        lifecycle: args.lifecycle,
        public: args.public,
        group: args.group,
        prompt: args.prompt,
        browser_profiles: args.browser_profiles,
        secrets: args.secrets,
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn switch_agent_harness_or_runtime(
    client: &Client,
    tool_label: &str,
    target_field: &str,
    target: Option<&str>,
    harness: Option<&str>,
    runtime: Option<&str>,
    budget_cap_micros: Option<u64>,
    model: Option<String>,
    profile: Option<String>,
) -> Result<serde_json::Value, String> {
    let target = target
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| format!("`{tool_label}` requires `{target_field}`"))?;
    let clean = target.strip_prefix("agent:").unwrap_or(target);
    let harness = harness.map(str::trim).filter(|s| !s.is_empty());
    let runtime = runtime.map(str::trim).filter(|s| !s.is_empty());
    if harness.is_none() && runtime.is_none() && budget_cap_micros.is_none() {
        return Err(format!(
            "`{tool_label}` requires `harness`, `runtime`, or `budget_cap_micros`"
        ));
    }
    let (suffix, body) = if runtime.is_some() || budget_cap_micros.is_some() || harness.is_none() {
        (
            "runtime",
            serde_json::json!({
                "harness": harness,
                "runtime": runtime,
                "budget_cap_micros": budget_cap_micros,
                "model": model,
                "profile": profile,
            }),
        )
    } else {
        (
            "harness",
            serde_json::json!({
                "harness": harness.unwrap_or_default(),
                "model": model,
                "profile": profile,
            }),
        )
    };
    client
        .get(
            &format!("/v1/agents/{clean}/{suffix}"),
            Some(serde_json::to_vec(&body).map_err(|e| e.to_string())?),
        )
        .await
}

pub(super) async fn patch_agent(
    client: &Client,
    target: Option<&str>,
    req: PatchAgentRequest,
) -> Result<serde_json::Value, String> {
    let target = target
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "`agents update` requires `name` (or `agent`)".to_string())?;
    let clean = target.strip_prefix("agent:").unwrap_or(target);
    let body = serde_json::to_vec(&req).map_err(|e| e.to_string())?;
    let updated: AgentView = client
        .request_method("PATCH", &format!("/v1/agents/{clean}"), Some(body))
        .await?;
    serde_json::to_value(updated).map_err(|e| e.to_string())
}

pub(super) async fn agent_secrets(
    client: &Client,
    target: Option<&str>,
    secrets: Option<Vec<String>>,
) -> Result<serde_json::Value, String> {
    let target = target
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("self");
    let clean = target.strip_prefix("agent:").unwrap_or(target);
    let resp: AgentSecretsResponse = if let Some(secrets) = secrets {
        let body = serde_json::to_vec(&serde_json::json!({ "secrets": secrets }))
            .map_err(|e| e.to_string())?;
        client
            .request_method("PUT", &format!("/v1/agents/{clean}/secrets"), Some(body))
            .await?
    } else {
        client
            .get(&format!("/v1/agents/{clean}/secrets"), None)
            .await?
    };
    serde_json::to_value(resp).map_err(|e| e.to_string())
}
