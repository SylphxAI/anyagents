//! Universal 7-tool MCP surface shared identically across all harnesses and transports.

/// The one universal AnyAgents MCP tool surface exposed identically across
/// every harness (`agy`, `claude-code`, `codex`, `opencode`, `spiron`, ...)
/// and every transport (stdio and Streamable HTTP `/mcp`).
pub const UNIVERSAL_MCP_TOOLS: &[&str] = &[
    "agents",
    "send",
    "inbox",
    "conversation",
    "memory",
    "schedules",
    "groups",
];

/// Whether `raw_name` is permitted on the AnyAgents MCP surface.
///
/// Permits the 7 canonical tools plus backward-compatible coordination aliases
/// (`schedule`, `search`, `spawn`, `subagents`, `describe`, `archive`, `wake`,
/// `interact`) that delegate directly to them.
pub fn allows_tool(raw_name: &str) -> bool {
    let clean = raw_name
        .trim()
        .strip_prefix("mcp__anyagents__")
        .unwrap_or(raw_name.trim());
    matches!(
        clean,
        "agents"
            | "send"
            | "inbox"
            | "conversation"
            | "memory"
            | "schedules"
            | "groups"
            | "schedule"
            | "search"
            | "spawn"
            | "subagents"
            | "describe"
            | "archive"
            | "wake"
            | "interact"
    )
}
