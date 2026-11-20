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

    pub(super) async fn handle_agents_browser_profiles(
        &self,
        target: Option<&str>,
        profiles: Option<Vec<String>>,
    ) -> Result<rmcp::handler::server::wrapper::Json<serde_json::Value>, String> {
        let target = target
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| s.strip_prefix("agent:").unwrap_or(s).to_string())
            .unwrap_or_else(|| self.agent.name.clone());
        let resolved_target = if target.eq_ignore_ascii_case("self") {
            self.agent.name.clone()
        } else {
            target
        };

        let result = if let Some(profiles) = profiles {
            self.set_agent_browser_profiles(&resolved_target, profiles)
                .await?
        } else {
            self.get_agent_browser_profiles(&resolved_target).await?
        };
        Ok(rmcp::handler::server::wrapper::Json(result))
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
