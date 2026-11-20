//! Optional standalone stdio MCP server for headless Chromium CDP browser automation.
//!
//! Decoupled completely from the AnyAgents coordinator: any MCP-compatible agent
//! harness (`agy`, `claude-code`, `codex`, `opencode`, `spiron`, or custom clients)
//! can mount `anyagents-mcp-browser` directly over stdio.

pub mod chromium;
pub mod config;
pub mod profile_sync;
pub mod server;
pub mod session;
pub mod url_guard;

pub use config::BrowserServerConfig;
pub use server::BrowserMcpServer;
