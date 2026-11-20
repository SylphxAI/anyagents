//! Optional standalone stdio MCP server for desktop GUI, screenshot, and accessibility automation.
//!
//! Decoupled completely from the AnyAgents coordinator: uses
//! `agent_workstation::PlatformAdapter` for native accessibility trees & input
//! plus OS screenshot/input helpers, and returns structured `UNSUPPORTED`
//! errors cleanly when run inside headless containers without a display.

pub mod backend;
pub mod config;
pub mod error;
pub mod server;

pub use backend::{ComputerBackend, DisplayAvailability};
pub use config::ComputerServerConfig;
pub use server::ComputerMcpServer;
