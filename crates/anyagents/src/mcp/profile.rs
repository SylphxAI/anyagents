//! MCP tool surface profiles (`Full`, `Remote`, `Coordination`) and profile enforcement.

use std::path::Path;

/// MCP tool surface profile.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    clap::ValueEnum,
    serde::Deserialize,
    serde::Serialize,
)]
#[serde(rename_all = "lowercase")]
pub enum McpProfile {
    /// Exposes all AnyAgents MCP tools (`send`, `inbox`, `agents`, `groups`, `schedules`,
    /// `conversation`, `code`, `web_search`, `web_fetch`, `memory`, `skills`, `browser`,
    /// `computer`). Default for `anyagents mcp` over `stdio` across all local CLI harnesses
    /// (`spiron`, `claude-code`, `codex`, `agy`, `opencode`). Never served over HTTP, where
    /// tools run inside the server process ([`McpProfile::resolve_http`]).
    #[default]
    Full,
    /// The most any Streamable HTTP `POST /mcp` call gets, whatever its credential, and the
    /// default there: all Cloud Agents (`spiron-cloud`, `claude-cloud`, `openai-cloud`,
    /// `cursor-cloud`) and remote sandboxes. Exposes coordination, sandbox-side host-free
    /// `code`, `memory`, and `skills`: control-plane state and sandboxed capabilities that
    /// belong to the agent. Direct host-bound tools (`files`, `browser`, `computer`, `device`,
    /// `media`, `web_search`, `web_fetch`) are omitted, and `workspace.*` inside `code` fails
    /// closed unless a remote sandbox backend is bound to the agent (zero coordinator-host
    /// filesystem exposure).
    Remote,
    /// Optional explicit coordination-only profile (`--profile coordination` or `?profile=coordination`).
    /// Exposes only pure multi-agent coordination tools (`send`, `inbox`, `agents`, `groups`,
    /// `schedules`, `conversation`).
    Coordination,
}

pub const FULL_MCP_TOOLS: &[&str] = &[
    "send",
    "inbox",
    "agents",
    "groups",
    "schedules",
    "conversation",
    "code",
    "web_search",
    "web_fetch",
    "memory",
    "skills",
    "browser",
    "computer",
];

/// The remote profile: coordination, host-free sandbox-side `code`, and the agent's
/// own memory and skills. Direct host-bound tools (`files`, `web_search`, `web_fetch`,
/// `browser`, `computer`, `device`, `media`) stay out so unisolated HTTP calls never
/// touch the coordinator host filesystem or spend control-plane network credentials.
pub const REMOTE_MCP_TOOLS: &[&str] = &[
    "send",
    "inbox",
    "agents",
    "groups",
    "schedules",
    "conversation",
    "code",
    "memory",
    "skills",
];

pub const COORDINATION_MCP_TOOLS: &[&str] = &[
    "send",
    "inbox",
    "agents",
    "groups",
    "schedules",
    "conversation",
];

impl McpProfile {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Remote => "remote",
            Self::Coordination => "coordination",
        }
    }

    pub fn from_str_opt(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "full" | "stdio" | "local" => Some(Self::Full),
            "remote" | "http" | "cloud" => Some(Self::Remote),
            "coordination" | "coord" => Some(Self::Coordination),
            _ => None,
        }
    }

    pub fn exposed_tools(&self) -> &'static [&'static str] {
        match self {
            Self::Full => FULL_MCP_TOOLS,
            Self::Remote => REMOTE_MCP_TOOLS,
            Self::Coordination => COORDINATION_MCP_TOOLS,
        }
    }

    pub fn allows_tool(&self, raw_name: &str) -> bool {
        let clean = raw_name
            .trim()
            .strip_prefix("mcp__anyagents__")
            .unwrap_or(raw_name.trim());
        match clean {
            // Direct host-bound tools require `Full`.
            "files" | "web" | "web_search" | "web_fetch" | "browser" | "computer" | "device"
            | "media" => matches!(self, Self::Full),
            // Host-free sandbox-side `code` + control-plane `memory`/`skills` run on `Full` and `Remote`.
            "code" | "memory" | "skills" => matches!(self, Self::Full | Self::Remote),
            "send" | "inbox" | "agents" | "groups" | "schedules" | "schedule" | "conversation"
            | "search" | "spawn" | "subagents" | "describe" | "archive" | "wake" | "interact" => {
                true
            }
            _ => false,
        }
    }

    pub fn resolve_stdio(explicit: Option<Self>, _harness: Option<&str>, _config: &Path) -> Self {
        explicit.unwrap_or(Self::Full)
    }

    /// The profile an MCP call over HTTP gets, whatever credential it
    /// presents: `remote`, or the narrower `coordination` when asked for.
    /// Over HTTP the tools run inside the server process, as the server's own
    /// user, so `full` (files, exec, code, browser, computer, ...) would hand
    /// the caller the server's identity. It is never served here; `full`
    /// exists only on stdio, where the tool server runs as the agent itself.
    pub fn resolve_http(explicit: Option<&str>) -> Result<Self, String> {
        match explicit.map(str::trim).filter(|s| !s.is_empty()) {
            None => Ok(Self::Remote),
            Some(raw) => match Self::from_str_opt(raw) {
                Some(Self::Remote) => Ok(Self::Remote),
                Some(Self::Coordination) => Ok(Self::Coordination),
                Some(Self::Full) => Err(format!(
                    "MCP profile `{raw}` is not served over HTTP, where tools would run as the server; \
                     HTTP serves `remote` or `coordination`, and `full` is available only over stdio \
                     (`anyagents mcp`)"
                )),
                None => Err(format!(
                    "invalid MCP profile `{raw}`; HTTP serves `remote` or `coordination`"
                )),
            },
        }
    }
}

#[cfg(test)]
pub const CANONICAL_MCP_TOOLS: &[&str] = &["code"];
