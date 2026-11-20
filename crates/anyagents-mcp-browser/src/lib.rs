//! Optional standalone stdio MCP server for headless Chromium CDP browser automation.
//!
//! Decoupled completely from the AnyAgents coordinator: any MCP-compatible agent
//! harness (`agy`, `claude-code`, `codex`, `opencode`, `spiron`, or custom clients)
//! can mount `anyagents-mcp-browser` directly over stdio.

pub mod chromium;
pub mod config;
pub mod desktop;
pub mod profile_sync;
pub mod proxy;
pub mod secrets;
pub mod server;
pub mod session;
pub mod signals;
pub mod takeover;
pub mod tunnel;
pub mod url_guard;

pub use config::{BrowserMode, BrowserServerConfig};
pub use server::BrowserMcpServer;
