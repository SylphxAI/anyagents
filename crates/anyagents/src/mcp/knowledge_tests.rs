use super::*;

pub(crate) fn test_agent_over_router(
    router: axum::Router,
    agent: anyagents_core::Agent,
) -> AgentMcp {
    let principal = crate::auth::Principal::Agent {
        agent_id: agent.id.clone(),
        name: agent.name.clone(),
    };
    AgentMcp {
        client: Client::new(PathBuf::new(), PathBuf::new(), String::new())
            .over_http_router(router, principal),
        agent: AgentRef {
            id: agent.id,
            name: agent.name,
            workdir: None,
        },
    }
}

impl AgentMcp {
    pub(crate) async fn test_dispatch_memory(
        &self,
        args: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        self.dispatch_any_tool("memory", args).await
    }
}
