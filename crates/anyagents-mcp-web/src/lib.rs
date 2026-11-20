//! Optional standalone stdio MCP server for web search and SSRF-safe web fetch.
//!
//! Uses pure Rust HTTP/TLS (`ureq` + `rustls`), works out of the box with zero
//! API keys via DuckDuckGo search and direct HTML-to-Markdown fetch, and
//! upgrades seamlessly when provider keys (`BRAVE_API_KEY`, `EXA_API_KEY`,
//! `TAVILY_API_KEY`, `PARALLEL_API_KEY`, `FIRECRAWL_API_KEY`,
//! `PERPLEXITY_API_KEY`) are present in the environment.

pub mod config;
pub mod fetch;
pub mod html;
pub mod http;
pub mod search;
pub mod server;
pub mod ssrf;

pub use config::WebServerConfig;
pub use server::WebMcpServer;
