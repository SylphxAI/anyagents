//! MCP `agents` helpers for `create` and `update` with persistent browser
//! profile grants (`browser_profiles`), enforcing the coordinator's exact
//! `child ⊆ parent` Capability Attenuation Law.

use super::*;

impl AgentMcp {
    pub(super) async fn set_agent_browser_profiles(
        &self,
        target: &str,
        profiles: Vec<String>,
    ) -> Result<serde_json::Value, String> {
        let clean = target
            .trim()
            .strip_prefix("agent:")
            .unwrap_or(target.trim());
        let body = serde_json::json!({ "browser_profiles": profiles });
        self.client
            .request_method(
                "PUT",
                &format!("/v1/agents/{clean}/browser-profiles"),
                Some(serde_json::to_vec(&body).map_err(|e| e.to_string())?),
            )
            .await
    }

    pub(super) async fn get_agent_browser_profiles(
        &self,
        target: &str,
    ) -> Result<serde_json::Value, String> {
        let clean = target
            .trim()
            .strip_prefix("agent:")
            .unwrap_or(target.trim());
        self.client
            .get(&format!("/v1/agents/{clean}/browser-profiles"), None)
            .await
    }

    pub(super) async fn handle_agents_create(
        &self,
        args: AgentsArgs,
    ) -> Result<rmcp::handler::server::wrapper::Json<serde_json::Value>, String> {
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
        let parent_val = match args
            .parent
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            Some(p) if p.eq_ignore_ascii_case("self") => Some(self.agent.id.to_string()),
            Some(p) => Some(p.to_string()),
            None => Some("root".to_string()),
        };
        let body = CreateAgentRequest {
            name,
            role: args.role,
            harness: Some(harness),
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
            parent: parent_val,
            scope: args.scope,
            lifecycle: args.lifecycle,
            public: args.public,
            group: args.group,
            prompt: args.prompt,
            browser_profiles: args.browser_profiles,
        };
        let created: AgentView = self
            .client
            .get(
                "/v1/agents",
                Some(serde_json::to_vec(&body).map_err(|err| err.to_string())?),
            )
            .await?;
        Ok(rmcp::handler::server::wrapper::Json(
            serde_json::to_value(created).map_err(|e| e.to_string())?,
        ))
    }

    pub(super) async fn handle_agents_update(
        &self,
        action: &str,
        args: AgentsArgs,
    ) -> Result<rmcp::handler::server::wrapper::Json<serde_json::Value>, String> {
        let target = args
            .name
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| s.strip_prefix("agent:").unwrap_or(s).to_string())
            .unwrap_or_else(|| self.agent.name.clone());
        let resolved_target = if target.eq_ignore_ascii_case("self") {
            self.agent.name.clone()
        } else {
            target
        };

        let has_desc_mutation = args.description.is_some()
            || args.instructions.is_some()
            || args.role.is_some()
            || args.scope.is_some()
            || args.lifecycle.is_some()
            || args.public.is_some()
            || args.clear.unwrap_or(false);

        if action == "browser_profiles" && args.browser_profiles.is_none() && !has_desc_mutation {
            let current = self.get_agent_browser_profiles(&resolved_target).await?;
            return Ok(rmcp::handler::server::wrapper::Json(current));
        }

        if has_desc_mutation {
            return self
                .describe(Parameters(DescribeArgs {
                    agent: Some(resolved_target),
                    description: args.description,
                    instructions: args.instructions,
                    role: args.role,
                    scope: args.scope,
                    lifecycle: args.lifecycle,
                    public: args.public,
                    clear: args.clear,
                    browser_profiles: args.browser_profiles,
                }))
                .await;
        }

        if let Some(profiles) = args.browser_profiles {
            let updated = self
                .set_agent_browser_profiles(&resolved_target, profiles)
                .await?;
            return Ok(rmcp::handler::server::wrapper::Json(updated));
        }

        Err(
            "`agents update` requires at least one field to update (`browser_profiles`, `description`, `instructions`, `role`, `scope`, `lifecycle`, `public`, or `clear`)"
                .to_string(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agents_args_exposes_and_deserializes_browser_profiles() {
        let schema = serde_json::to_value(schemars::schema_for!(AgentsArgs)).unwrap();
        assert!(
            schema["properties"].get("browser_profiles").is_some(),
            "AgentsArgs schema must expose browser_profiles"
        );
        let create_args: AgentsArgs = serde_json::from_value(serde_json::json!({
            "action": "create",
            "name": "worker-a",
            "browser_profiles": ["profile-a", "profile-b"]
        }))
        .unwrap();
        assert_eq!(
            create_args.browser_profiles,
            Some(vec!["profile-a".to_string(), "profile-b".to_string()])
        );
        let update_args: AgentsArgs = serde_json::from_value(serde_json::json!({
            "action": "update",
            "name": "worker-a",
            "browser_profiles": ["profile-a"]
        }))
        .unwrap();
        assert_eq!(
            update_args.browser_profiles,
            Some(vec!["profile-a".to_string()])
        );
    }
}
