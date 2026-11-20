//! Standalone `rmcp` stdio MCP server exposing optional desktop GUI tools.

use std::path::Path;

use agent_workstation::TreeOptions;
use base64::Engine as _;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{
    CacheScope, CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock,
    Implementation, ListToolsResult, PaginatedRequestParams, ProtocolVersion, ResultType,
    ServerCapabilities, ServerConfig, Tool,
};
use rmcp::service::RequestContext;
use rmcp::{ErrorData, RoleServer, ServerHandler, tool, tool_router};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::backend::{ComputerBackend, MouseButton, ScreenRegion, ScrollDirection, save_png};
use crate::config::ComputerServerConfig;
use crate::error::ComputerError;

/// Standalone MCP server for desktop screenshots, accessibility snapshots, and GUI input.
#[derive(Clone)]
pub struct ComputerMcpServer {
    backend: ComputerBackend,
}

impl ComputerMcpServer {
    /// Construct a server backed by the current host OS desktop environment.
    pub fn new(config: &ComputerServerConfig) -> Self {
        Self {
            backend: ComputerBackend::from_config(config),
        }
    }

    /// Construct a server with an explicit [`ComputerBackend`] (for tests).
    pub fn with_backend(backend: ComputerBackend) -> Self {
        Self { backend }
    }

    /// Inspect all registered MCP tool definitions.
    pub fn list_tool_definitions() -> Vec<Tool> {
        Self::tool_router().list_all()
    }
}

/// Parameters for `computer_screenshot`.
#[derive(Debug, Default, Deserialize, JsonSchema)]
pub struct ComputerScreenshotArgs {
    /// Optional rectangular crop region in logical screen pixels (`{ x, y, width, height }`).
    #[serde(default)]
    pub region: Option<ScreenRegion>,
    /// Optional filesystem path where the PNG screenshot should also be written.
    #[serde(default)]
    pub path: Option<String>,
}

/// Parameters for `computer_click`.
#[derive(Debug, Default, Deserialize, JsonSchema)]
pub struct ComputerClickArgs {
    /// Logical X coordinate in pixels (used when `ref` is omitted).
    #[serde(default)]
    pub x: Option<i32>,
    /// Logical Y coordinate in pixels (used when `ref` is omitted).
    #[serde(default)]
    pub y: Option<i32>,
    /// Optional semantic accessibility node ref (`@<snapshot_id>:e<N>`) from `computer_snapshot`.
    #[serde(default, alias = "ref", alias = "ref_id")]
    pub node_ref: Option<String>,
    /// Mouse button: `"left"` (default), `"right"`, or `"middle"`.
    #[serde(default)]
    pub button: Option<String>,
    /// Click count (`1` for single click, `2` for double-click; clamped `1..=3`).
    #[serde(default)]
    pub count: Option<u32>,
    /// Optional modifier keys held during the click (e.g. `["ctrl"]`, `["shift"]`).
    #[serde(default)]
    pub modifiers: Vec<String>,
}

/// Parameters for `computer_type`.
#[derive(Debug, Deserialize, JsonSchema)]
pub struct ComputerTypeArgs {
    /// Text to type or set into the target control.
    pub text: String,
    /// Optional semantic accessibility node ref (`@<snapshot_id>:e<N>`) from `computer_snapshot`.
    #[serde(default, alias = "ref", alias = "ref_id")]
    pub node_ref: Option<String>,
    /// Optional X coordinate to click before typing.
    #[serde(default)]
    pub x: Option<i32>,
    /// Optional Y coordinate to click before typing.
    #[serde(default)]
    pub y: Option<i32>,
    /// Optional per-keystroke delay in milliseconds (X11 `xdotool` fallback only).
    #[serde(default)]
    pub delay_ms: Option<u32>,
}

/// Parameters for `computer_key`.
#[derive(Debug, Deserialize, JsonSchema)]
pub struct ComputerKeyArgs {
    /// Key name or chord (e.g. `"Return"`, `"Escape"`, `"Tab"`, `"ctrl+c"`, `"cmd+v"`).
    pub key: String,
    /// Optional modifier keys combined with `key` (e.g. `["ctrl", "shift"]`).
    #[serde(default)]
    pub modifiers: Vec<String>,
}

/// Parameters for `computer_scroll`.
#[derive(Debug, Deserialize, JsonSchema)]
pub struct ComputerScrollArgs {
    /// Scroll direction: `"up"`, `"down"`, `"left"`, or `"right"`.
    pub direction: String,
    /// Number of scroll notches (`1..=100`, default `3`).
    #[serde(default)]
    pub amount: Option<u32>,
    /// Optional logical X coordinate where scrolling occurs.
    #[serde(default)]
    pub x: Option<i32>,
    /// Optional logical Y coordinate where scrolling occurs.
    #[serde(default)]
    pub y: Option<i32>,
    /// Optional semantic accessibility node ref (`@<snapshot_id>:e<N>`) to scroll.
    #[serde(default, alias = "ref", alias = "ref_id")]
    pub node_ref: Option<String>,
}

/// Parameters for `computer_snapshot`.
#[derive(Debug, Default, Deserialize, JsonSchema)]
pub struct ComputerSnapshotArgs {
    /// Maximum accessibility tree nesting depth (default `16`).
    #[serde(default)]
    pub max_depth: Option<usize>,
    /// Maximum number of accessibility nodes returned (default `500`).
    #[serde(default)]
    pub max_nodes: Option<usize>,
    /// Optional target process id filter.
    #[serde(default)]
    pub pid: Option<u32>,
}

fn error_result(err: ComputerError) -> CallToolResult {
    CallToolResult::structured_error(err.to_json())
}

fn image_tool_result(metadata: Value, png_base64: String) -> CallToolResult {
    let mut result = CallToolResult::structured(metadata);
    result.structured_content = None;
    result
        .content
        .push(ContentBlock::image(png_base64, "image/png"));
    result
}

fn parse_optional_coord(
    x: Option<i32>,
    y: Option<i32>,
) -> Result<Option<(i32, i32)>, ComputerError> {
    match (x, y) {
        (Some(x), Some(y)) => Ok(Some((x, y))),
        (None, None) => Ok(None),
        _ => Err(ComputerError::invalid_input(
            "both `x` and `y` must be supplied together when targeting coordinates",
        )),
    }
}

#[tool_router]
impl ComputerMcpServer {
    #[tool(
        description = "Capture a PNG screenshot of the host desktop (or an optional `region`). Returns a native MCP image block and optionally saves the PNG to `path`. Returns typed `UNSUPPORTED` cleanly in headless containers without a display.",
        annotations(read_only_hint = true, open_world_hint = false)
    )]
    pub async fn computer_screenshot(
        &self,
        Parameters(args): Parameters<ComputerScreenshotArgs>,
    ) -> CallToolResult {
        match self.backend.screenshot(args.region).await {
            Ok((png, backend_name, width, height)) => {
                let save_path = args
                    .path
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_owned);
                if let Some(ref dest) = save_path
                    && let Err(err) = save_png(Path::new(dest), &png).await
                {
                    return error_result(err);
                }
                let encoded = base64::engine::general_purpose::STANDARD.encode(&png);
                let metadata = json!({
                    "action": "screenshot",
                    "backend": backend_name,
                    "mime_type": "image/png",
                    "width": width,
                    "height": height,
                    "bytes": png.len(),
                    "region": args.region,
                    "path": save_path,
                    "image_attached": true,
                });
                image_tool_result(metadata, encoded)
            }
            Err(err) => error_result(err),
        }
    }

    #[tool(
        description = "Click a desktop element by semantic accessibility `ref` (`@<snapshot_id>:e<N>`) or logical `(x, y)` screen coordinates. Returns typed `UNSUPPORTED` cleanly on headless containers without a display.",
        annotations(read_only_hint = false, open_world_hint = false)
    )]
    pub async fn computer_click(
        &self,
        Parameters(args): Parameters<ComputerClickArgs>,
    ) -> CallToolResult {
        let button = match MouseButton::parse(args.button.as_deref()) {
            Ok(b) => b,
            Err(err) => return error_result(err),
        };
        let coord = match parse_optional_coord(args.x, args.y) {
            Ok(c) => c,
            Err(err) => return error_result(err),
        };
        match self
            .backend
            .click(
                args.node_ref.as_deref(),
                coord,
                button,
                args.count.unwrap_or(1),
                &args.modifiers,
            )
            .await
        {
            Ok(val) => CallToolResult::structured(val),
            Err(err) => error_result(err),
        }
    }

    #[tool(
        description = "Type Unicode text into the focused desktop control, a semantic accessibility `ref` (`@<snapshot_id>:e<N>`), or after clicking `(x, y)`. Returns typed `UNSUPPORTED` cleanly on headless containers.",
        annotations(read_only_hint = false, open_world_hint = false)
    )]
    pub async fn computer_type(
        &self,
        Parameters(args): Parameters<ComputerTypeArgs>,
    ) -> CallToolResult {
        let coord = match parse_optional_coord(args.x, args.y) {
            Ok(c) => c,
            Err(err) => return error_result(err),
        };
        match self
            .backend
            .type_text(&args.text, args.node_ref.as_deref(), coord, args.delay_ms)
            .await
        {
            Ok(val) => CallToolResult::structured(val),
            Err(err) => error_result(err),
        }
    }

    #[tool(
        description = "Press a desktop key or shortcut chord (e.g. `Return`, `Escape`, `ctrl+c`, `cmd+l`). Returns typed `UNSUPPORTED` cleanly on headless containers.",
        annotations(read_only_hint = false, open_world_hint = false)
    )]
    pub async fn computer_key(
        &self,
        Parameters(args): Parameters<ComputerKeyArgs>,
    ) -> CallToolResult {
        match self.backend.press_key(&args.key, &args.modifiers).await {
            Ok(val) => CallToolResult::structured(val),
            Err(err) => error_result(err),
        }
    }

    #[tool(
        description = "Scroll `up`, `down`, `left`, or `right` by `amount` notches at an optional semantic accessibility `ref`, `(x, y)` coordinates, or the current pointer. Returns typed `UNSUPPORTED` cleanly on headless containers.",
        annotations(read_only_hint = false, open_world_hint = false)
    )]
    pub async fn computer_scroll(
        &self,
        Parameters(args): Parameters<ComputerScrollArgs>,
    ) -> CallToolResult {
        let direction = match ScrollDirection::parse(&args.direction) {
            Ok(d) => d,
            Err(err) => return error_result(err),
        };
        let coord = match parse_optional_coord(args.x, args.y) {
            Ok(c) => c,
            Err(err) => return error_result(err),
        };
        match self
            .backend
            .scroll(
                direction,
                args.amount.unwrap_or(3),
                args.node_ref.as_deref(),
                coord,
            )
            .await
        {
            Ok(val) => CallToolResult::structured(val),
            Err(err) => error_result(err),
        }
    }

    #[tool(
        description = "Capture a structured desktop accessibility tree snapshot with `@<snapshot_id>:e<N>` element refs for semantic `computer_click`/`computer_type`/`computer_scroll` actions. Returns typed `UNSUPPORTED` cleanly on headless containers.",
        annotations(read_only_hint = true, open_world_hint = false)
    )]
    pub async fn computer_snapshot(
        &self,
        Parameters(args): Parameters<ComputerSnapshotArgs>,
    ) -> CallToolResult {
        let mut opts = TreeOptions::default();
        if let Some(d) = args.max_depth {
            opts.max_depth = d;
        }
        if let Some(n) = args.max_nodes {
            opts.max_nodes = n;
        }
        opts.pid = args.pid;

        match self.backend.snapshot(opts).await {
            Ok(tree) => CallToolResult::structured(json!({
                "action": "snapshot",
                "snapshot_id": tree.snapshot_id,
                "platform": tree.platform,
                "total_nodes": tree.total_nodes,
                "truncated": tree.truncated,
                "outline": tree.text,
                "roots": tree.roots,
            })),
            Err(err) => error_result(err),
        }
    }
}

impl ServerHandler for ComputerMcpServer {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new(
                env!("CARGO_PKG_NAME"),
                env!("CARGO_PKG_VERSION"),
            ))
            .with_instructions(
                "Standalone desktop GUI and accessibility MCP server. Prefer `computer_snapshot` + `@<snapshot>:e<N>` \
                 refs when available, or `computer_screenshot` with pixel coordinates. Headless environments without \
                 a display return structured `UNSUPPORTED` errors cleanly.",
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
    use std::sync::Arc;

    use agent_workstation::MockPlatformAdapter;

    use super::*;
    use crate::backend::DisplayAvailability;

    #[test]
    fn exposes_all_computer_tools_with_object_schemas() {
        let tools = ComputerMcpServer::list_tool_definitions();
        let names: Vec<&str> = tools.iter().map(|t| t.name.as_ref()).collect();
        for expected in [
            "computer_screenshot",
            "computer_click",
            "computer_type",
            "computer_key",
            "computer_scroll",
            "computer_snapshot",
        ] {
            assert!(
                names.contains(&expected),
                "missing expected MCP tool `{expected}` in {names:?}"
            );
        }
        assert_eq!(tools.len(), 6);
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
    async fn headless_mcp_tools_return_structured_unsupported_errors() {
        let backend = ComputerBackend::with_parts(
            DisplayAvailability::detect_for("linux", None, None),
            Arc::new(MockPlatformAdapter::default()),
        );
        let server = ComputerMcpServer::with_backend(backend);

        let shot = server
            .computer_screenshot(Parameters(ComputerScreenshotArgs::default()))
            .await;
        assert_eq!(shot.is_error, Some(true));
        assert_eq!(shot.structured_content.unwrap()["code"], "UNSUPPORTED");

        let click = server
            .computer_click(Parameters(ComputerClickArgs {
                x: Some(100),
                y: Some(200),
                ..Default::default()
            }))
            .await;
        assert_eq!(click.is_error, Some(true));
        assert_eq!(click.structured_content.unwrap()["code"], "UNSUPPORTED");

        let typed = server
            .computer_type(Parameters(ComputerTypeArgs {
                text: "hello".into(),
                node_ref: None,
                x: None,
                y: None,
                delay_ms: None,
            }))
            .await;
        assert_eq!(typed.is_error, Some(true));
        assert_eq!(typed.structured_content.unwrap()["code"], "UNSUPPORTED");

        let key = server
            .computer_key(Parameters(ComputerKeyArgs {
                key: "Return".into(),
                modifiers: Vec::new(),
            }))
            .await;
        assert_eq!(key.is_error, Some(true));
        assert_eq!(key.structured_content.unwrap()["code"], "UNSUPPORTED");

        let scroll = server
            .computer_scroll(Parameters(ComputerScrollArgs {
                direction: "down".into(),
                amount: Some(3),
                x: None,
                y: None,
                node_ref: None,
            }))
            .await;
        assert_eq!(scroll.is_error, Some(true));
        assert_eq!(scroll.structured_content.unwrap()["code"], "UNSUPPORTED");
    }

    #[tokio::test]
    async fn save_png_writes_bytes_and_creates_parent_directories() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("nested/capture.png");
        save_png(&target, b"\x89PNG\r\n\x1a\n").await.unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"\x89PNG\r\n\x1a\n");
    }
}
