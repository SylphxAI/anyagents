use std::collections::HashMap;
use std::sync::RwLock;

/// Which provider protocol a route speaks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Protocol {
    /// Anthropic Messages API (`POST /v1/messages`).
    AnthropicMessages,
    /// OpenAI Chat Completions (`POST /v1/chat/completions`).
    OpenAiChat,
    /// OpenAI Responses API (`POST /v1/responses`).
    OpenAiResponses,
}

/// One agent's provider route.
///
/// `api_key` is the API key the user stored for that provider. The gateway has
/// no field for a vendor subscription credential and never reads one.
#[derive(Clone, Debug)]
pub struct ProviderRoute {
    pub agent: String,
    /// Name recorded on usage events, chosen by the server (`"anthropic"`, `"openai"`, ...).
    pub provider: String,
    pub protocol: Protocol,
    /// Provider origin, for example `https://api.example.com`, with no path.
    pub base_url: String,
    pub model: String,
    pub api_key: String,
}

/// Resolves a per-agent gateway bearer token to a provider route.
pub trait RouteTable: Send + Sync {
    fn resolve(&self, token: &str) -> Option<ProviderRoute>;
}

/// In-memory route table for tests and for a server that keeps routes itself.
#[derive(Debug, Default)]
pub struct MemoryRoutes {
    routes: RwLock<HashMap<String, ProviderRoute>>,
}

impl MemoryRoutes {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&self, token: impl Into<String>, route: ProviderRoute) {
        self.routes
            .write()
            .expect("route table")
            .insert(token.into(), route);
    }
}

impl RouteTable for MemoryRoutes {
    fn resolve(&self, token: &str) -> Option<ProviderRoute> {
        self.routes.read().expect("route table").get(token).cloned()
    }
}
