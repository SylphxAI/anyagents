//! Standalone `rmcp` stdio MCP server exposing browser automation tools.

use std::path::Path;
use std::time::Duration;

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

use crate::config::BrowserServerConfig;
use crate::session::{BrowserError, BrowserSession, select_explicit_target};
use crate::url_guard::{MAX_TEXT_BYTES, navigable_url, readable_page_url, truncate_utf8};

/// Standalone MCP server for headless Chromium CDP browser automation.
#[derive(Clone)]
pub struct BrowserMcpServer {
    session: BrowserSession,
}

impl BrowserMcpServer {
    /// Construct a new server instance backed by `config`.
    pub fn new(config: BrowserServerConfig) -> Self {
        Self {
            session: BrowserSession::new(config),
        }
    }

    /// Gracefully shut down any running Chromium child process.
    pub async fn shutdown(&self) {
        let _ = self.session.close().await;
    }

    /// Inspect all registered MCP tool definitions (useful in tests and introspection).
    pub fn list_tool_definitions() -> Vec<Tool> {
        Self::tool_router().list_all()
    }

    async fn run_navigate(&self, args: BrowserNavigateArgs) -> Result<Value, BrowserError> {
        let action = args
            .action
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_ascii_lowercase)
            .unwrap_or_else(|| "goto".to_string());

        let normalized_url = match action.as_str() {
            "goto" | "open" | "navigate" => {
                let raw = args.url.as_deref().ok_or_else(|| {
                    BrowserError::invalid_input("`browser_navigate` requires `url`")
                })?;
                Some(navigable_url(raw).map_err(BrowserError::invalid_input)?)
            }
            "back" | "forward" | "reload" | "refresh" => None,
            other => {
                return Err(BrowserError::invalid_input(format!(
                    "unknown navigation action `{other}`; expected `goto`, `back`, `forward`, or `reload`"
                )));
            }
        };

        let mut live = self
            .session
            .acquire_with_profile(args.profile.as_deref())
            .await?;
        let web = live.web();
        select_explicit_target(web, args.target_id.as_deref()).await?;
        let final_url = match action.as_str() {
            "goto" | "open" | "navigate" => {
                let url = normalized_url.as_deref().expect("validated above");
                web.navigate(url).await?
            }
            "back" => web.history(-1).await?,
            "forward" => web.history(1).await?,
            "reload" | "refresh" => web.reload().await?,
            _ => unreachable!("validated above"),
        };
        optional_pause(args.wait_ms).await;
        let (target_id, title) = active_tab_meta(web).await;
        let mut payload = json!({
            "action": action,
            "url": final_url,
            "title": title,
            "target_id": target_id,
        });
        if args.include_snapshot == Some(true) {
            ensure_readable_page(web).await?;
            let snap = web.snapshot().await?;
            let (text, truncated) = truncate_utf8(snap.text, MAX_TEXT_BYTES);
            payload["snapshot_id"] = json!(snap.id);
            payload["refs"] = json!(snap.refs);
            payload["snapshot"] = json!(text);
            payload["truncated"] = json!(truncated);
        }
        Ok(payload)
    }

    async fn run_snapshot(&self, args: BrowserSnapshotArgs) -> Result<Value, BrowserError> {
        let mode = args
            .mode
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_ascii_lowercase)
            .unwrap_or_else(|| "accessibility".to_string());
        if !matches!(
            mode.as_str(),
            "accessibility" | "ax" | "snapshot" | "text" | "read" | "html" | "dom"
        ) {
            return Err(BrowserError::invalid_input(format!(
                "unknown snapshot mode `{mode}`; expected `accessibility`, `text`, or `html`"
            )));
        }

        let mut live = self.session.acquire().await?;
        let web = live.web();
        select_explicit_target(web, args.target_id.as_deref()).await?;
        let url = ensure_readable_page(web).await?;
        let (target_id, title) = active_tab_meta(web).await;
        match mode.as_str() {
            "accessibility" | "ax" | "snapshot" => {
                let snap = web.snapshot().await?;
                let (text, truncated) = truncate_utf8(snap.text, MAX_TEXT_BYTES);
                Ok(json!({
                    "mode": "accessibility",
                    "url": snap.url,
                    "title": title,
                    "target_id": target_id,
                    "snapshot_id": snap.id,
                    "refs": snap.refs,
                    "snapshot": text,
                    "truncated": truncated,
                }))
            }
            "text" | "read" => {
                let raw = web.read().await?;
                let (text, truncated) = truncate_utf8(raw, MAX_TEXT_BYTES);
                Ok(json!({
                    "mode": "text",
                    "url": url,
                    "title": title,
                    "target_id": target_id,
                    "text": text,
                    "truncated": truncated,
                }))
            }
            "html" | "dom" => {
                let sel = args
                    .selector
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty());
                let raw = web.html(sel).await?;
                let (html, truncated) = truncate_utf8(raw, MAX_TEXT_BYTES);
                Ok(json!({
                    "mode": "html",
                    "url": url,
                    "title": title,
                    "target_id": target_id,
                    "selector": sel,
                    "html": html,
                    "truncated": truncated,
                }))
            }
            _ => unreachable!("validated above"),
        }
    }

    async fn run_click(&self, args: BrowserClickArgs) -> Result<Value, BrowserError> {
        let selector = args.selector.trim().to_string();
        if selector.is_empty() {
            return Err(BrowserError::invalid_input(
                "`browser_click` requires a non-empty `selector`",
            ));
        }
        let mut live = self.session.acquire().await?;
        let web = live.web();
        select_explicit_target(web, args.target_id.as_deref()).await?;
        ensure_readable_page(web).await?;
        web.click(&selector).await?;
        optional_pause(args.wait_ms).await;
        let url = web.current_url().await.unwrap_or_default();
        let (target_id, title) = active_tab_meta(web).await;
        Ok(json!({
            "ok": true,
            "selector": selector,
            "url": url,
            "title": title,
            "target_id": target_id,
        }))
    }

    async fn run_type(&self, args: BrowserTypeArgs) -> Result<Value, BrowserError> {
        let selector = args.selector.trim().to_string();
        if selector.is_empty() {
            return Err(BrowserError::invalid_input(
                "`browser_type` requires a non-empty `selector`",
            ));
        }
        let mut live = self.session.acquire().await?;
        let web = live.web();
        select_explicit_target(web, args.target_id.as_deref()).await?;
        ensure_readable_page(web).await?;
        web.fill(&selector, &args.text).await?;
        let mut submitted = false;
        if let Some(key) = args
            .press_key
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            web.press(key).await?;
            submitted = key.eq_ignore_ascii_case("enter") || key.eq_ignore_ascii_case("return");
        } else if args.submit == Some(true) {
            web.press("Enter").await?;
            submitted = true;
        }
        optional_pause(args.wait_ms).await;
        let url = web.current_url().await.unwrap_or_default();
        let (target_id, title) = active_tab_meta(web).await;
        Ok(json!({
            "ok": true,
            "selector": selector,
            "chars": args.text.chars().count(),
            "submitted": submitted,
            "url": url,
            "title": title,
            "target_id": target_id,
        }))
    }

    async fn run_screenshot(
        &self,
        args: BrowserScreenshotArgs,
    ) -> Result<(Value, String), BrowserError> {
        let full_page = args.full_page.unwrap_or(false);
        let save_path = args
            .path
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_owned);

        let mut live = self.session.acquire().await?;
        let web = live.web();
        select_explicit_target(web, args.target_id.as_deref()).await?;
        let url = ensure_readable_page(web).await?;
        let (target_id, title) = active_tab_meta(web).await;
        let shot = web.page_screenshot(full_page).await?;
        if let Some(ref dest) = save_path {
            let path = Path::new(dest);
            if let Some(parent) = path.parent()
                && !parent.as_os_str().is_empty()
            {
                tokio::fs::create_dir_all(parent).await.map_err(|e| {
                    BrowserError::new(
                        "IO_ERROR",
                        format!(
                            "failed creating screenshot directory `{}`: {e}",
                            parent.display()
                        ),
                    )
                })?;
            }
            tokio::fs::write(path, &shot.png).await.map_err(|e| {
                BrowserError::new(
                    "IO_ERROR",
                    format!("failed writing screenshot `{dest}`: {e}"),
                )
            })?;
        }
        let encoded = base64::engine::general_purpose::STANDARD.encode(&shot.png);
        let metadata = json!({
            "url": url,
            "title": title,
            "target_id": target_id,
            "full_page": full_page,
            "mime_type": "image/png",
            "width": shot.width,
            "height": shot.height,
            "bytes": shot.png.len(),
            "truncated": shot.truncated,
            "path": save_path,
            "image_attached": true,
        });
        Ok((metadata, encoded))
    }

    async fn run_eval(&self, args: BrowserEvalArgs) -> Result<Value, BrowserError> {
        let script = args.script.trim().to_string();
        if script.is_empty() {
            return Err(BrowserError::invalid_input(
                "`browser_eval` requires a non-empty `script`",
            ));
        }
        let mut live = self.session.acquire().await?;
        let web = live.web();
        select_explicit_target(web, args.target_id.as_deref()).await?;
        let url = ensure_readable_page(web).await?;
        let (target_id, _) = active_tab_meta(web).await;
        let value = web.eval(&script).await?;
        let serialized = value.to_string();
        if serialized.len() > MAX_TEXT_BYTES {
            let (preview, _) = truncate_utf8(serialized, MAX_TEXT_BYTES);
            Ok(json!({
                "url": url,
                "target_id": target_id,
                "result": preview,
                "truncated": true,
            }))
        } else {
            Ok(json!({
                "url": url,
                "target_id": target_id,
                "result": value,
                "truncated": false,
            }))
        }
    }

    async fn run_tabs(&self, args: BrowserTabsArgs) -> Result<Value, BrowserError> {
        let action = args
            .action
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_ascii_lowercase)
            .unwrap_or_else(|| "list".to_string());

        let new_tab_url = match action.as_str() {
            "list" => None,
            "new" | "open" => {
                let url = match args.url.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
                    Some(raw) => navigable_url(raw).map_err(BrowserError::invalid_input)?,
                    None => "about:blank".to_string(),
                };
                Some(url)
            }
            "activate" | "select" | "switch" => {
                if args
                    .target_id
                    .as_deref()
                    .map(str::trim)
                    .is_none_or(str::is_empty)
                {
                    return Err(BrowserError::invalid_input(
                        "`browser_tabs` with `action = \"activate\"` requires `target_id`",
                    ));
                }
                None
            }
            "close" => {
                if args
                    .target_id
                    .as_deref()
                    .map(str::trim)
                    .is_none_or(str::is_empty)
                {
                    return Err(BrowserError::invalid_input(
                        "`browser_tabs` with `action = \"close\"` requires `target_id`",
                    ));
                }
                None
            }
            other => {
                return Err(BrowserError::invalid_input(format!(
                    "unknown tab action `{other}`; expected `list`, `new`, `activate`, or `close`"
                )));
            }
        };

        let mut live = self.session.acquire().await?;
        let web = live.web();
        match action.as_str() {
            "list" => {}
            "new" | "open" => {
                let url = new_tab_url.as_deref().expect("validated above");
                web.new_tab(url).await?;
            }
            "activate" | "select" | "switch" => {
                select_explicit_target(web, args.target_id.as_deref()).await?;
            }
            "close" => {
                let verified = select_explicit_target(web, args.target_id.as_deref())
                    .await?
                    .expect("explicit target_id verified");
                web.close_tab(&verified).await?;
            }
            _ => unreachable!("validated above"),
        }
        let tabs = web.tabs().await?;
        let active_id = tabs.iter().find(|t| t.active).map(|t| t.id.clone());
        Ok(json!({
            "action": action,
            "active_target_id": active_id,
            "count": tabs.len(),
            "tabs": tabs,
        }))
    }
}

/// Parameters for `browser_navigate`.
#[derive(Debug, Default, Deserialize, JsonSchema)]
pub struct BrowserNavigateArgs {
    /// Target URL (`https://` is assumed when no scheme is provided; `http://`, `https://`, and `about:blank` are permitted).
    #[serde(default)]
    pub url: Option<String>,
    /// Navigation action: `"goto"` (default when `url` is set), `"back"`, `"forward"`, or `"reload"`.
    #[serde(default)]
    pub action: Option<String>,
    /// Optional explicit tab `target_id` (fails closed with `STALE_TARGET` if absent).
    #[serde(default)]
    pub target_id: Option<String>,
    /// Optional post-navigation wait in milliseconds (clamped to 10 000 ms).
    #[serde(default)]
    pub wait_ms: Option<u64>,
    /// When `true`, also capture and return the accessibility snapshot in the response.
    #[serde(default)]
    pub include_snapshot: Option<bool>,
    /// Optional named persistent browser profile synced with the AnyAgents coordinator
    /// (`GET/PUT <ANYAGENTS_API_URL>/v1/browser-profiles/<profile>/archive`).
    #[serde(default)]
    pub profile: Option<String>,
}

/// Parameters for `browser_snapshot`.
#[derive(Debug, Default, Deserialize, JsonSchema)]
pub struct BrowserSnapshotArgs {
    /// Snapshot mode: `"accessibility"` (default outline with `@eN` refs), `"text"` (visible innerText), or `"html"` (outer HTML).
    #[serde(default)]
    pub mode: Option<String>,
    /// Optional ref (`@eN` / `@<snapshot_id>:eN`) or CSS selector when `mode = "html"`.
    #[serde(default, alias = "ref", alias = "target")]
    pub selector: Option<String>,
    /// Optional explicit tab `target_id`.
    #[serde(default)]
    pub target_id: Option<String>,
}

/// Parameters for `browser_click`.
#[derive(Debug, Deserialize, JsonSchema)]
pub struct BrowserClickArgs {
    /// Element to click: a snapshot ref (`"@e1"` or `"@<snapshot_id>:e1"`) or a CSS selector (`"#submit"`).
    #[serde(alias = "ref", alias = "target")]
    pub selector: String,
    /// Optional explicit tab `target_id`.
    #[serde(default)]
    pub target_id: Option<String>,
    /// Optional post-click wait in milliseconds (clamped to 10 000 ms).
    #[serde(default)]
    pub wait_ms: Option<u64>,
}

/// Parameters for `browser_type`.
#[derive(Debug, Deserialize, JsonSchema)]
pub struct BrowserTypeArgs {
    /// Input field to fill: a snapshot ref (`"@e1"` or `"@<snapshot_id>:e1"`) or a CSS selector.
    #[serde(alias = "ref", alias = "target")]
    pub selector: String,
    /// Text to type into the field (replaces existing value).
    #[serde(alias = "value")]
    pub text: String,
    /// Press `Enter` inside the field after typing.
    #[serde(default)]
    pub submit: Option<bool>,
    /// Optional explicit key chord to press after typing (e.g. `"Tab"`, `"Enter"`, `"Escape"`).
    #[serde(default)]
    pub press_key: Option<String>,
    /// Optional explicit tab `target_id`.
    #[serde(default)]
    pub target_id: Option<String>,
    /// Optional post-type wait in milliseconds (clamped to 10 000 ms).
    #[serde(default)]
    pub wait_ms: Option<u64>,
}

/// Parameters for `browser_screenshot`.
#[derive(Debug, Default, Deserialize, JsonSchema)]
pub struct BrowserScreenshotArgs {
    /// Capture the full scrollable page instead of just the viewport.
    #[serde(default)]
    pub full_page: Option<bool>,
    /// Optional filesystem path where the PNG screenshot should also be written.
    #[serde(default)]
    pub path: Option<String>,
    /// Optional explicit tab `target_id`.
    #[serde(default)]
    pub target_id: Option<String>,
}

/// Parameters for `browser_eval`.
#[derive(Debug, Deserialize, JsonSchema)]
pub struct BrowserEvalArgs {
    /// JavaScript expression to evaluate in the active page (promises are awaited).
    #[serde(alias = "expression", alias = "js")]
    pub script: String,
    /// Optional explicit tab `target_id`.
    #[serde(default)]
    pub target_id: Option<String>,
}

/// Parameters for `browser_tabs`.
#[derive(Debug, Default, Deserialize, JsonSchema)]
pub struct BrowserTabsArgs {
    /// Tab operation: `"list"` (default), `"new"`, `"activate"`, or `"close"`.
    #[serde(default)]
    pub action: Option<String>,
    /// URL to open when `action = "new"` (defaults to `about:blank`).
    #[serde(default)]
    pub url: Option<String>,
    /// Tab target id for `"activate"` or `"close"`.
    #[serde(default)]
    pub target_id: Option<String>,
}

/// Parameters for `browser_close`.
#[derive(Debug, Default, Deserialize, JsonSchema)]
pub struct BrowserCloseArgs {}

fn error_result(err: BrowserError) -> CallToolResult {
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

async fn optional_pause(wait_ms: Option<u64>) {
    let ms = wait_ms.unwrap_or(0).min(10_000);
    if ms > 0 {
        tokio::time::sleep(Duration::from_millis(ms)).await;
    }
}

async fn active_tab_meta(web: &mut agent_workstation::browser::Web) -> (Option<String>, String) {
    web.tabs()
        .await
        .ok()
        .and_then(|tabs| tabs.into_iter().find(|t| t.active))
        .map(|t| ((!t.id.is_empty()).then_some(t.id), t.title))
        .unwrap_or((None, String::new()))
}

async fn ensure_readable_page(
    web: &mut agent_workstation::browser::Web,
) -> Result<String, BrowserError> {
    let url = web.current_url().await?;
    if !readable_page_url(&url) {
        return Err(BrowserError::invalid_input(
            "browser page access permits only `http://`, `https://`, and `about:blank`",
        ));
    }
    Ok(url)
}

#[tool_router]
impl BrowserMcpServer {
    #[tool(
        description = "Navigate the browser to a URL (`url`), or step through history (`action`: `goto` | `back` | `forward` | `reload`). Launches headless Chromium automatically on first use. Set `include_snapshot: true` to return the accessibility outline immediately.",
        annotations(read_only_hint = false, open_world_hint = true)
    )]
    pub async fn browser_navigate(
        &self,
        Parameters(args): Parameters<BrowserNavigateArgs>,
    ) -> CallToolResult {
        match self.run_navigate(args).await {
            Ok(val) => CallToolResult::structured(val),
            Err(err) => error_result(err),
        }
    }

    #[tool(
        description = "Inspect the active browser page: returns a compact accessibility tree outline with interactive element refs (`@e1`, `@<snapshot_id>:e1`) by default (`mode: \"accessibility\"`), visible text (`mode: \"text\"`), or outer HTML (`mode: \"html\"`, optional `selector`).",
        annotations(read_only_hint = true, open_world_hint = true)
    )]
    pub async fn browser_snapshot(
        &self,
        Parameters(args): Parameters<BrowserSnapshotArgs>,
    ) -> CallToolResult {
        match self.run_snapshot(args).await {
            Ok(val) => CallToolResult::structured(val),
            Err(err) => error_result(err),
        }
    }

    #[tool(
        description = "Click an element on the active page identified by a snapshot ref (`@e1` or `@<snapshot_id>:e1`) or a CSS selector (`#submit`). Refs fail with `STALE_REF` once replaced by a navigation or newer snapshot.",
        annotations(read_only_hint = false, open_world_hint = true)
    )]
    pub async fn browser_click(
        &self,
        Parameters(args): Parameters<BrowserClickArgs>,
    ) -> CallToolResult {
        match self.run_click(args).await {
            Ok(val) => CallToolResult::structured(val),
            Err(err) => error_result(err),
        }
    }

    #[tool(
        description = "Focus, clear, and type `text` into an input field identified by a snapshot ref (`@e1` or `@<snapshot_id>:e1`) or CSS selector. Set `submit: true` to press Enter afterward, or pass `press_key` for any key chord.",
        annotations(read_only_hint = false, open_world_hint = true)
    )]
    pub async fn browser_type(
        &self,
        Parameters(args): Parameters<BrowserTypeArgs>,
    ) -> CallToolResult {
        match self.run_type(args).await {
            Ok(val) => CallToolResult::structured(val),
            Err(err) => error_result(err),
        }
    }

    #[tool(
        description = "Capture a PNG screenshot of the active page viewport (or the entire scrollable page with `full_page: true`). Returns an attached MCP image block and optionally writes the PNG to `path`.",
        annotations(read_only_hint = true, open_world_hint = true)
    )]
    pub async fn browser_screenshot(
        &self,
        Parameters(args): Parameters<BrowserScreenshotArgs>,
    ) -> CallToolResult {
        match self.run_screenshot(args).await {
            Ok((metadata, encoded)) => image_tool_result(metadata, encoded),
            Err(err) => error_result(err),
        }
    }

    #[tool(
        description = "Evaluate a JavaScript expression in the active page (promises are awaited) and return its JSON-serialized value.",
        annotations(read_only_hint = false, open_world_hint = true)
    )]
    pub async fn browser_eval(
        &self,
        Parameters(args): Parameters<BrowserEvalArgs>,
    ) -> CallToolResult {
        match self.run_eval(args).await {
            Ok(val) => CallToolResult::structured(val),
            Err(err) => error_result(err),
        }
    }

    #[tool(
        description = "Manage browser tabs: `list` (default), `new` (with optional `url`), `activate` (with `target_id`), or `close` (with `target_id`). Explicit `target_id` fails closed with `STALE_TARGET` when absent.",
        annotations(read_only_hint = false, open_world_hint = true)
    )]
    pub async fn browser_tabs(
        &self,
        Parameters(args): Parameters<BrowserTabsArgs>,
    ) -> CallToolResult {
        match self.run_tabs(args).await {
            Ok(val) => CallToolResult::structured(val),
            Err(err) => error_result(err),
        }
    }

    #[tool(
        description = "Close the running headless Chromium instance cleanly. If `--profile-dir` / `ANYAGENTS_BROWSER_PROFILE_DIR` is configured, cookies and site storage remain on disk for future sessions.",
        annotations(
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true
        )
    )]
    pub async fn browser_close(
        &self,
        Parameters(_args): Parameters<BrowserCloseArgs>,
    ) -> CallToolResult {
        let outcome = self.session.close_with_sync().await;
        let mut payload = json!({
            "status": "closed",
            "was_running": outcome.was_running,
            "profile_preserved": outcome.profile_preserved,
            "profile_synced": outcome.profile_synced,
        });
        if let Some(err) = outcome.sync_error {
            payload["sync_error"] = json!(err);
        }
        CallToolResult::structured(payload)
    }
}

impl ServerHandler for BrowserMcpServer {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new(
                env!("CARGO_PKG_NAME"),
                env!("CARGO_PKG_VERSION"),
            ))
            .with_instructions(
                "Standalone Chromium CDP browser automation server. Call `browser_navigate` to open a page, \
                 `browser_snapshot` for interactive `@eN` refs, `browser_click`/`browser_type`/`browser_eval` to \
                 interact, `browser_screenshot` for visual inspection, `browser_tabs` to manage tabs, and \
                 `browser_close` when finished.",
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

    #[test]
    fn exposes_all_eight_canonical_browser_tools_with_object_schemas() {
        let tools = BrowserMcpServer::list_tool_definitions();
        let names: Vec<&str> = tools.iter().map(|t| t.name.as_ref()).collect();
        for expected in [
            "browser_navigate",
            "browser_snapshot",
            "browser_click",
            "browser_type",
            "browser_screenshot",
            "browser_eval",
            "browser_tabs",
            "browser_close",
        ] {
            assert!(
                names.contains(&expected),
                "missing expected MCP tool `{expected}` in {names:?}"
            );
        }
        assert_eq!(tools.len(), 8);
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
    async fn browser_close_is_idempotent_when_no_browser_is_running() {
        let server = BrowserMcpServer::new(BrowserServerConfig::default());
        let res = server.browser_close(Parameters(BrowserCloseArgs {})).await;
        assert_eq!(res.is_error, Some(false));
        let payload = res.structured_content.expect("structured payload");
        assert_eq!(payload["status"], "closed");
        assert_eq!(payload["was_running"], false);
        assert_eq!(payload["profile_preserved"], false);
    }

    #[tokio::test]
    async fn browser_tools_validate_inputs_without_launching_chromium() {
        let server = BrowserMcpServer::new(BrowserServerConfig::default());

        let nav = server
            .browser_navigate(Parameters(BrowserNavigateArgs {
                url: Some("file:///etc/passwd".into()),
                ..Default::default()
            }))
            .await;
        assert_eq!(nav.is_error, Some(true));
        assert_eq!(nav.structured_content.unwrap()["code"], "INVALID_INPUT");

        let click = server
            .browser_click(Parameters(BrowserClickArgs {
                selector: "   ".into(),
                target_id: None,
                wait_ms: None,
            }))
            .await;
        assert_eq!(click.is_error, Some(true));
        assert_eq!(click.structured_content.unwrap()["code"], "INVALID_INPUT");

        let eval = server
            .browser_eval(Parameters(BrowserEvalArgs {
                script: " ".into(),
                target_id: None,
            }))
            .await;
        assert_eq!(eval.is_error, Some(true));
        assert_eq!(eval.structured_content.unwrap()["code"], "INVALID_INPUT");

        let tab = server
            .browser_tabs(Parameters(BrowserTabsArgs {
                action: Some("activate".into()),
                url: None,
                target_id: None,
            }))
            .await;
        assert_eq!(tab.is_error, Some(true));
        assert_eq!(tab.structured_content.unwrap()["code"], "INVALID_INPUT");
    }
}
