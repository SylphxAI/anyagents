//! Standalone `rmcp` stdio MCP server exposing `web_search` and `web_fetch`.

use std::sync::Arc;

use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{
    CacheScope, CallToolRequestParams, CallToolResponse, CallToolResult, Implementation,
    ListToolsResult, PaginatedRequestParams, ProtocolVersion, ResultType, ServerCapabilities,
    ServerConfig, Tool,
};
use rmcp::service::RequestContext;
use rmcp::{ErrorData, RoleServer, ServerHandler, tool, tool_router};
use serde_json::json;

use crate::config::WebServerConfig;
use crate::fetch::{WebFetchRequest, execute_fetch};
use crate::search::{WebSearchRequest, execute_search};

/// Standalone MCP server for web search and SSRF-protected web fetching.
#[derive(Clone)]
pub struct WebMcpServer {
    config: Arc<WebServerConfig>,
}

impl WebMcpServer {
    /// Construct a new server backed by `config`.
    pub fn new(config: WebServerConfig) -> Self {
        Self {
            config: Arc::new(config),
        }
    }

    /// Inspect all registered MCP tool definitions (useful in tests and introspection).
    pub fn list_tool_definitions() -> Vec<Tool> {
        Self::tool_router().list_all()
    }
}

fn web_error(code: &str, message: impl Into<String>) -> CallToolResult {
    CallToolResult::structured_error(json!({
        "code": code,
        "error": message.into(),
    }))
}

#[tool_router]
impl WebMcpServer {
    #[tool(
        description = "Search the public web. Uses env-configured search provider API keys (`BRAVE_API_KEY`, `EXA_API_KEY`, `TAVILY_API_KEY`, `PARALLEL_API_KEY`, `FIRECRAWL_API_KEY`, `PERPLEXITY_API_KEY`) with bounded failover, and falls back automatically to DuckDuckGo HTML search when no API keys are configured.",
        annotations(read_only_hint = true, open_world_hint = true)
    )]
    pub async fn web_search(
        &self,
        Parameters(args): Parameters<WebSearchRequest>,
    ) -> CallToolResult {
        if args.query.trim().is_empty() {
            return web_error("INVALID_INPUT", "`web_search` requires a non-empty `query`");
        }
        let config = Arc::clone(&self.config);
        let outcome = tokio::task::spawn_blocking(move || {
            let keys = config.load_keys();
            execute_search(
                &keys,
                &args,
                config.default_timeout_ms,
                config.effective_user_agent(),
            )
        })
        .await;

        match outcome {
            Ok(Ok(value)) => CallToolResult::structured(value),
            Ok(Err(msg)) => web_error("WEB_SEARCH_ERROR", msg),
            Err(join_err) => web_error("INTERNAL_ERROR", format!("search task failed: {join_err}")),
        }
    }

    #[tool(
        description = "Fetch a public `http://` or `https://` web page with SSRF private-IP protection (validates literal IPs, DNS resolutions, and every redirect hop) and clean HTML-to-Markdown extraction. Optionally routes through configured extraction APIs (`FIRECRAWL_API_KEY`, `EXA_API_KEY`, `PARALLEL_API_KEY`, `TAVILY_API_KEY`) with automatic direct HTTP/TLS fallback.",
        annotations(read_only_hint = true, open_world_hint = true)
    )]
    pub async fn web_fetch(&self, Parameters(args): Parameters<WebFetchRequest>) -> CallToolResult {
        if args.url.trim().is_empty() {
            return web_error("INVALID_INPUT", "`web_fetch` requires a non-empty `url`");
        }
        let config = Arc::clone(&self.config);
        let outcome = tokio::task::spawn_blocking(move || {
            let keys = config.load_keys();
            execute_fetch(
                &keys,
                &args,
                config.default_timeout_ms,
                config.effective_user_agent(),
            )
        })
        .await;

        match outcome {
            Ok(Ok(value)) => CallToolResult::structured(value),
            Ok(Err(msg)) => {
                let code = if msg.contains("SSRF") {
                    "SSRF_BLOCKED"
                } else {
                    "WEB_FETCH_ERROR"
                };
                web_error(code, msg)
            }
            Err(join_err) => web_error("INTERNAL_ERROR", format!("fetch task failed: {join_err}")),
        }
    }
}

impl ServerHandler for WebMcpServer {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new(
                env!("CARGO_PKG_NAME"),
                env!("CARGO_PKG_VERSION"),
            ))
            .with_instructions(
                "Standalone web search and SSRF-safe web fetch MCP server. Use `web_search` to find \
                 relevant pages and `web_fetch` to read public URLs as clean Markdown.",
            )
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        let supports_cache_hints = context
            .protocol_version()
            .is_some_and(|version| version >= ProtocolVersion::V_2026_07_28);
        Ok(ListToolsResult {
            result_type: Some(ResultType::COMPLETE),
            tools: Self::tool_router().list_all(),
            meta: None,
            next_cursor: None,
            ttl_ms: supports_cache_hints.then_some(0),
            cache_scope: supports_cache_hints.then_some(CacheScope::Public),
        })
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        let tcc = rmcp::handler::server::tool::ToolCallContext::new(self, request, context);
        Self::tool_router().call(tcc).await
    }

    fn get_tool(&self, name: &str) -> Option<Tool> {
        Self::tool_router().get(name).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    #[test]
    fn exposes_web_search_and_web_fetch_with_object_schemas() {
        let tools = WebMcpServer::list_tool_definitions();
        let names: Vec<&str> = tools.iter().map(|t| t.name.as_ref()).collect();
        assert_eq!(tools.len(), 2);
        assert!(names.contains(&"web_search"));
        assert!(names.contains(&"web_fetch"));
        for tool in &tools {
            assert_eq!(
                tool.input_schema.get("type").and_then(Value::as_str),
                Some("object"),
                "tool `{}` input_schema must have root type `object`",
                tool.name
            );
        }
    }

    #[tokio::test]
    async fn web_fetch_rejects_ssrf_targets_with_typed_error_code() {
        let server = WebMcpServer::new(WebServerConfig::default());
        let res = server
            .web_fetch(Parameters(WebFetchRequest {
                url: "http://127.0.0.1:8080/internal".into(),
                ..Default::default()
            }))
            .await;
        assert_eq!(res.is_error, Some(true));
        let payload = res.structured_content.expect("structured error payload");
        assert_eq!(payload["code"], "SSRF_BLOCKED");
    }

    #[tokio::test]
    async fn web_search_rejects_empty_query() {
        let server = WebMcpServer::new(WebServerConfig::default());
        let res = server
            .web_search(Parameters(WebSearchRequest {
                query: "   ".into(),
                ..Default::default()
            }))
            .await;
        assert_eq!(res.is_error, Some(true));
        let payload = res.structured_content.expect("structured error payload");
        assert_eq!(payload["code"], "INVALID_INPUT");
    }
}
