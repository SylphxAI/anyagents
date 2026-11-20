//! Configuration for the standalone `anyagents-mcp-computer` stdio MCP server.

use clap::Parser;

/// CLI arguments and environment configuration for `anyagents-mcp-computer`.
#[derive(Clone, Debug, Default, Parser)]
#[command(
    name = "anyagents-mcp-computer",
    version,
    about = "Standalone stdio MCP server providing optional desktop GUI, screenshot, and accessibility tools"
)]
pub struct ComputerServerConfig {
    /// Optional explicit X11 display identifier (e.g. `:0`, `:99`).
    #[arg(long, env = "DISPLAY")]
    pub display: Option<String>,

    /// Optional explicit Wayland display socket name (e.g. `wayland-0`).
    #[arg(long, env = "WAYLAND_DISPLAY")]
    pub wayland_display: Option<String>,
}

impl ComputerServerConfig {
    /// Non-empty trimmed `DISPLAY` value, if configured.
    pub fn effective_display(&self) -> Option<&str> {
        self.display
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
    }

    /// Non-empty trimmed `WAYLAND_DISPLAY` value, if configured.
    pub fn effective_wayland_display(&self) -> Option<&str> {
        self.wayland_display
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
    }
}
