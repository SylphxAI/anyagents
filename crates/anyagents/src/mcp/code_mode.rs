//! Sandbox-side Programmatic Code Mode execution and extensible namespace registry
//! powered by Master `agent_tools::code_mode` (`CodeModeRunner`, `ToolRegistry`,
//! `FnNamespaceHandler`, `SANDBOX_WORKER_JS`, `SANDBOX_HTTP_CLIENT_JS`, and
//! `generate_ts_declarations`).
//!
//! Works identically across both `McpProfile::Full` and `McpProfile::Remote` with
//! strict host isolation: under `McpProfile::Remote`, `workspace.*` routes exclusively
//! to an active remote `WorkspaceBackend` (failing closed with zero coordinator-host
//! filesystem exposure when no remote sandbox is bound), while `coordination.*`,
//! `memory.*`, `skills.*`, and cloud `workstation.*` execute with full `Promise.all`
//! concurrent batching.

use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use agent_tools::WorkspaceBackend;
use agent_tools::code_mode::{
    CodeExecRequest, CodeLimits as KitCodeLimits, CodeModeRunner, FnNamespaceHandler,
    SANDBOX_WORKER_JS, ToolCallRequest, ToolDispatcher, ToolError, ToolRegistry,
    generate_ts_declarations,
};
use serde_json::{Value, json};

use super::*;

/// How far past a command's own timeout the `code` call it runs in stays
/// open, so the command's error reaches the snippet before the call ends.
/// A timed-out command's group gets [`code_children::TERM_GRACE`] after
/// SIGTERM before SIGKILL; the call must outlast that and the answer.
pub(super) const CODE_CALL_GRACE_MS: u64 = code_children::TERM_GRACE.as_millis() as u64 + 1_500;
const _: () = assert!(CODE_CALL_GRACE_MS >= code_children::TERM_GRACE.as_millis() as u64 + 1_000);
const MAX_CODE_RESULT_BYTES: usize = 48 * 1024;
const MAX_CODE_TOOL_CALLS: usize = 256;

/// Shared per-turn execution context passed into mounted `ToolRegistry` handlers.
#[derive(Clone)]
struct CodeExecContext {
    mcp: AgentMcp,
    started: Instant,
    deadline_ms: Arc<AtomicU64>,
    max_ms: u64,
    deliveries: Arc<Mutex<Vec<Value>>>,
    media_blocks: Arc<Mutex<Vec<rmcp::model::ContentBlock>>>,
}

impl CodeExecContext {
    fn record_media(&self, blocks: Vec<rmcp::model::ContentBlock>) {
        if !blocks.is_empty() {
            self.media_blocks
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .extend(blocks);
        }
    }

    fn record_deliveries(&self, val: &Value) {
        if let Some(items) = val.get("deliveries").and_then(Value::as_array) {
            self.deliveries
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .extend(items.iter().cloned());
        }
    }

    async fn call_coordination(&self, method: &str, params: Value) -> Result<Value, ToolError> {
        let normalized = normalize_coordination_args(method, params);
        let (val, blocks) = self
            .mcp
            .dispatch_code_tool(method, normalized)
            .await
            .map_err(ToolError::Execution)?;
        if method == "send" {
            self.record_deliveries(&val);
        }
        self.record_media(blocks);
        Ok(val)
    }

    async fn call_memory(&self, method: &str, params: Value) -> Result<Value, ToolError> {
        let args = normalize_memory_args(method, params);
        let (val, blocks) = self
            .mcp
            .dispatch_code_tool("memory", args)
            .await
            .map_err(ToolError::Execution)?;
        self.record_media(blocks);
        Ok(val)
    }

    async fn call_skills(&self, method: &str, params: Value) -> Result<Value, ToolError> {
        let args = normalize_skills_args(method, params);
        let (val, blocks) = self
            .mcp
            .dispatch_code_tool("skills", args)
            .await
            .map_err(ToolError::Execution)?;
        self.record_media(blocks);
        Ok(val)
    }

    async fn call_workspace(&self, method: &str, params: Value) -> Result<Value, ToolError> {
        let mut args = normalize_workspace_args(method, params);
        args = budget_code_call(
            "files",
            args,
            self.started.elapsed(),
            &self.deadline_ms,
            self.max_ms,
        );
        if self.mcp.profile == McpProfile::Remote {
            // Remote profile strictly forbids coordinator-host filesystem access.
            let local_root = self
                .mcp
                .agent
                .workdir
                .clone()
                .unwrap_or_else(|| PathBuf::from("/workspace"));
            let config_dir = self.mcp.client.config.clone();
            let parsed: FilesArgs = serde_json::from_value(args)
                .map_err(|e| ToolError::InvalidParams(format!("invalid workspace args: {e}")))?;
            let method_name = method.to_string();
            let limits = CodeLimits::current();
            let scope = current_call().scope();
            let (val, blocks) = tokio::task::spawn_blocking(move || {
                let _scope = code_children::enter(scope);
                let backend = crate::sandbox_providers::resolve_workspace_backend_for_mode(
                    Some(&config_dir),
                    local_root.clone(),
                    crate::sandbox_providers::SandboxMode::RemoteRequired,
                )
                .map_err(|_| {
                    ToolError::Execution(format!(
                        "workspace.{method_name} requires an active remote sandbox under MCP profile \
                         `remote`; coordinator-host filesystem access is forbidden"
                    ))
                })?;
                if !backend.is_remote() {
                    return Err(ToolError::Execution(format!(
                        "workspace.{method_name} refused: non-remote workspace backend under MCP profile `remote`"
                    )));
                }
                execute_files_tool_with_media_backend(backend.as_ref(), &local_root, parsed, limits)
                    .map_err(ToolError::Execution)
            })
            .await
            .map_err(|e| ToolError::Execution(format!("workspace task join error: {e}")))??;
            self.record_media(blocks);
            return Ok(val);
        }
        let (val, blocks) = self
            .mcp
            .dispatch_code_tool("files", args)
            .await
            .map_err(ToolError::Execution)?;
        self.record_media(blocks);
        Ok(val)
    }

    async fn call_web(&self, method: &str, params: Value) -> Result<Value, ToolError> {
        if self.mcp.profile == McpProfile::Remote {
            return Err(ToolError::Execution(format!(
                "web.{method} is disabled by active MCP profile `remote` (host network egress blocked)"
            )));
        }
        let args = normalize_web_args(method, params);
        let (val, blocks) = self
            .mcp
            .dispatch_code_tool("web", args)
            .await
            .map_err(ToolError::Execution)?;
        self.record_media(blocks);
        Ok(val)
    }

    async fn call_workstation(&self, method: &str, params: Value) -> Result<Value, ToolError> {
        if let Some(desktop_action) = method.strip_prefix("desktop_") {
            return self.call_computer(desktop_action, params).await;
        }
        if self.mcp.profile == McpProfile::Remote {
            return Err(ToolError::Execution(format!(
                "workstation.{method} requires a remote/session workstation or `full` MCP profile; \
                 coordinator-host browser access is blocked under `remote`"
            )));
        }
        let args = merge_action_field(method, params);
        let (val, blocks) = self
            .mcp
            .dispatch_code_tool("browser", args)
            .await
            .map_err(ToolError::Execution)?;
        self.record_media(blocks);
        Ok(val)
    }

    async fn call_computer(&self, method: &str, params: Value) -> Result<Value, ToolError> {
        let args = normalize_computer_args(method, params);
        if self.mcp.profile == McpProfile::Remote
            && crate::computer::target(&args) != crate::computer::Target::Cloud
        {
            return Err(ToolError::Execution(format!(
                "computer.{method} is disabled under MCP profile `remote` unless targeting a cloud sandbox"
            )));
        }
        let (val, blocks) = self
            .mcp
            .dispatch_code_tool("computer", args)
            .await
            .map_err(ToolError::Execution)?;
        self.record_media(blocks);
        Ok(val)
    }

    async fn call_host_only_tool(
        &self,
        tool: &str,
        method: &str,
        params: Value,
    ) -> Result<Value, ToolError> {
        if self.mcp.profile == McpProfile::Remote {
            return Err(ToolError::Execution(format!(
                "{tool}.{method} is disabled by active MCP profile `remote`"
            )));
        }
        let args = if method == "call" || method == "action" {
            params
        } else {
            merge_action_field(method, params)
        };
        let (val, blocks) = self
            .mcp
            .dispatch_code_tool(tool, args)
            .await
            .map_err(ToolError::Execution)?;
        self.record_media(blocks);
        Ok(val)
    }
}

fn merge_action_field(action: &str, params: Value) -> Value {
    match params {
        Value::Object(mut map) => {
            map.insert("action".to_string(), Value::String(action.to_string()));
            Value::Object(map)
        }
        Value::Null => json!({ "action": action }),
        other => json!({ "action": action, "value": other }),
    }
}

fn normalize_coordination_args(method: &str, params: Value) -> Value {
    if method == "skills"
        && let Value::String(name) = params
    {
        return json!({ "action": "read", "name": name });
    }
    if params.is_null() { json!({}) } else { params }
}

fn normalize_memory_args(method: &str, params: Value) -> Value {
    let action = if method == "upsert" { "write" } else { method };
    match (action, params) {
        ("read", Value::String(topic)) => json!({ "action": "read", "topic": topic }),
        ("delete", Value::String(topic)) => {
            json!({ "action": "delete", "topic": topic, "delete": true })
        }
        ("delete", Value::Object(mut map)) => {
            map.insert("action".into(), json!("delete"));
            map.insert("delete".into(), json!(true));
            Value::Object(map)
        }
        ("list" | "status", Value::String(agent)) => json!({ "action": action, "agent": agent }),
        ("search" | "recall", Value::String(query)) => json!({ "action": action, "query": query }),
        (_, other) => merge_action_field(action, other),
    }
}

fn normalize_skills_args(method: &str, params: Value) -> Value {
    match (method, params) {
        ("read" | "get", Value::String(name)) => json!({ "action": method, "name": name }),
        ("search", Value::String(query)) => json!({ "action": "search", "query": query }),
        (_, other) => merge_action_field(method, other),
    }
}

fn normalize_workspace_args(method: &str, params: Value) -> Value {
    match (method, params) {
        ("patch", Value::String(patch)) => json!({ "action": "patch", "patch": patch }),
        ("read", Value::String(path)) => json!({ "action": "read", "path": path }),
        ("list", Value::String(path)) => json!({ "action": "list", "path": path }),
        ("glob", Value::String(pattern)) => json!({ "action": "glob", "pattern": pattern }),
        ("grep", Value::String(query)) => json!({ "action": "grep", "query": query }),
        ("exec", Value::String(cmd)) => json!({ "action": "exec", "cmd": cmd }),
        (_, other) => merge_action_field(method, other),
    }
}

fn normalize_web_args(method: &str, params: Value) -> Value {
    match (method, params) {
        ("search", Value::String(query)) => json!({ "action": "search", "query": query }),
        ("fetch", Value::String(url)) => json!({ "action": "fetch", "url": url }),
        (_, other) => merge_action_field(method, other),
    }
}

fn normalize_computer_args(method: &str, params: Value) -> Value {
    if method == "action" {
        return params;
    }
    match (method, params) {
        ("click", Value::String(r)) => json!({ "action": "click", "ref": r }),
        ("focus_window", Value::String(wid)) => {
            json!({ "action": "focus_window", "window_id": wid })
        }
        ("clipboard_set" | "type", Value::String(text)) => {
            json!({ "action": method, "text": text })
        }
        ("key", Value::String(key)) => json!({ "action": "key", "key": key }),
        ("wait", Value::Number(ms)) => json!({ "action": "wait", "duration_ms": ms }),
        ("open_url", Value::String(url)) => json!({ "action": "open_url", "url": url }),
        (_, other) => merge_action_field(method, other),
    }
}

/// Build the unified `agent_tools::code_mode::ToolRegistry` mounting `workspace.*`,
/// `web.*`, `memory.*`, `workstation.*`, `coordination.*`, `skills.*`, and legacy
/// `anyagents.*` / `browser.*` / `computer.*` / `device.*` / `media.*` namespaces.
fn build_registry(ctx: CodeExecContext) -> ToolRegistry {
    let mut reg = ToolRegistry::new();

    // workspace.*
    {
        let c = ctx.clone();
        reg.mount(Arc::new(FnNamespaceHandler::new(
            "workspace",
            &[
                "read",
                "write",
                "edit",
                "multi_edit",
                "patch",
                "glob",
                "grep",
                "list",
                "exec",
            ],
            move |method, params| {
                let c = c.clone();
                async move { c.call_workspace(&method, params).await }
            },
        )));
    }

    // memory.* (Raw-RAG agent-memory)
    {
        let c = ctx.clone();
        reg.mount(Arc::new(FnNamespaceHandler::new(
            "memory",
            &[
                "read",
                "write",
                "upsert",
                "delete",
                "list",
                "search",
                "recall",
                "status",
                "consolidate",
            ],
            move |method, params| {
                let c = c.clone();
                async move { c.call_memory(&method, params).await }
            },
        )));
    }

    // web.*
    {
        let c = ctx.clone();
        reg.mount(Arc::new(FnNamespaceHandler::new(
            "web",
            &["search", "fetch"],
            move |method, params| {
                let c = c.clone();
                async move { c.call_web(&method, params).await }
            },
        )));
    }

    // workstation.* (agent-workstation web + native desktop)
    {
        let c = ctx.clone();
        reg.mount(Arc::new(FnNamespaceHandler::new(
            "workstation",
            &[
                "open",
                "snapshot",
                "click",
                "fill",
                "fill_secret",
                "eval",
                "screenshot",
                "tabs",
                "status",
                "close",
                "desktop_snapshot",
                "desktop_click",
                "desktop_type",
                "desktop_key",
                "desktop_scroll",
                "desktop_list_windows",
            ],
            move |method, params| {
                let c = c.clone();
                async move { c.call_workstation(&method, params).await }
            },
        )));
    }

    // coordination.* (canonical multi-agent coordination namespace)
    {
        let c = ctx.clone();
        reg.mount(Arc::new(FnNamespaceHandler::new(
            "coordination",
            &[
                "send",
                "inbox",
                "agents",
                "schedule",
                "schedules",
                "conversation",
                "search",
                "groups",
                "skills",
                "subagents",
                "spawn",
                "describe",
                "archive",
                "wake",
                "interact",
            ],
            move |method, params| {
                let c = c.clone();
                async move { c.call_coordination(&method, params).await }
            },
        )));
    }

    // anyagents.* (backward-compatible top-level orchestrator alias)
    {
        let c = ctx.clone();
        reg.mount(Arc::new(FnNamespaceHandler::new(
            "anyagents",
            &[
                "send",
                "inbox",
                "agents",
                "schedule",
                "schedules",
                "conversation",
                "search",
                "groups",
                "browser",
                "skills",
                "memory",
                "web",
                "computer",
                "device",
                "media",
                "files",
                "subagents",
                "spawn",
                "describe",
                "archive",
                "wake",
                "interact",
            ],
            move |method, params| {
                let c = c.clone();
                async move {
                    match method.as_str() {
                        "files" => {
                            let action = params
                                .get("action")
                                .and_then(Value::as_str)
                                .unwrap_or("read")
                                .to_string();
                            c.call_workspace(&action, params).await
                        }
                        "memory" => {
                            let action = params
                                .get("action")
                                .and_then(Value::as_str)
                                .unwrap_or("read")
                                .to_string();
                            c.call_memory(&action, params).await
                        }
                        "web" => {
                            let action = match &params {
                                Value::String(s) if s.trim().starts_with("http") => "fetch",
                                Value::String(_) => "search",
                                other => other
                                    .get("action")
                                    .and_then(Value::as_str)
                                    .unwrap_or("search"),
                            }
                            .to_string();
                            c.call_web(&action, params).await
                        }
                        "browser" => c.call_host_only_tool("browser", "call", params).await,
                        "computer" => {
                            let action = params
                                .get("action")
                                .and_then(Value::as_str)
                                .unwrap_or("action")
                                .to_string();
                            c.call_computer(&action, params).await
                        }
                        "device" => c.call_host_only_tool("device", "call", params).await,
                        "media" => c.call_host_only_tool("media", "call", params).await,
                        other => c.call_coordination(other, params).await,
                    }
                }
            },
        )));
    }

    // skills.*
    {
        let c = ctx.clone();
        reg.mount(Arc::new(FnNamespaceHandler::new(
            "skills",
            &["list", "read", "get", "search"],
            move |method, params| {
                let c = c.clone();
                async move { c.call_skills(&method, params).await }
            },
        )));
    }

    // computer.*
    {
        let c = ctx.clone();
        reg.mount(Arc::new(FnNamespaceHandler::new(
            "computer",
            &[
                "snapshot",
                "list_windows",
                "focus_window",
                "clipboard_get",
                "clipboard_set",
                "screenshot",
                "cursor_position",
                "move",
                "click",
                "mouse_down",
                "mouse_up",
                "drag",
                "scroll",
                "type",
                "key",
                "hold_key",
                "wait",
                "zoom",
                "open_url",
                "action",
            ],
            move |method, params| {
                let c = c.clone();
                async move { c.call_computer(&method, params).await }
            },
        )));
    }

    // device.*
    {
        let c = ctx.clone();
        reg.mount(Arc::new(FnNamespaceHandler::new(
            "device",
            &[
                "snapshot",
                "screenshot",
                "tap",
                "swipe",
                "type",
                "press",
                "install_app",
                "launch_app",
                "terminate_app",
                "open_url",
                "rotate",
            ],
            move |method, params| {
                let c = c.clone();
                async move { c.call_host_only_tool("device", &method, params).await }
            },
        )));
    }

    // media.*
    {
        let c = ctx;
        reg.mount(Arc::new(FnNamespaceHandler::new(
            "media",
            &["image_generate", "video_generate", "speech", "transcribe"],
            move |method, params| {
                let c = c.clone();
                async move { c.call_host_only_tool("media", &method, params).await }
            },
        )));
    }

    reg
}

/// Compose the sandbox isolate script directly from Master
/// `agent_tools::code_mode::SANDBOX_WORKER_JS`, layering AnyAgents' nested
/// `anyagents.{memory,web,workstation,computer,device,media}` compatibility
/// aliases, `.hits`/`.text` convenience getters, and `const web`/`const tools`
/// snippet shadowing without duplicating the core isolate runtime.
pub(super) fn compose_worker_js() -> String {
    let base = SANDBOX_WORKER_JS;
    // 1. Allow snippets to shadow `web`/`workspace`/`anyagents` cleanly inside the inner block
    //    just as `tools` is shadowed.
    let patched = base.replace(
        "const shadow = \"tools, console, process, require, module, exports, Bun, Deno, fetch, WebSocket\";",
        "const shadow = \"tools, anyagents, workspace, web, console, process, require, module, exports, Bun, Deno, fetch, WebSocket\";",
    );
    let patched = patched.replace(
        "const result = await fn(env.tools, env.console);",
        "const result = await fn(env.tools, env.tools.anyagents, env.tools.workspace, env.tools.web, env.console);",
    );
    // 2. Preserve non-enumerable `.hits` (`results`) and `.text` (`content`) response aliases.
    let patched = patched.replace(
        "try { resolve(JSON.parse(resJson)); } catch (e) { reject(e); }",
        "try { const parsed = JSON.parse(resJson); if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) { if ('results' in parsed && !('hits' in parsed)) Object.defineProperty(parsed, 'hits', { get() { return this.results; }, enumerable: false, configurable: true }); if ('content' in parsed && !('text' in parsed)) Object.defineProperty(parsed, 'text', { get() { return this.content; }, enumerable: false, configurable: true }); } resolve(parsed); } catch (e) { reject(e); }",
    );
    // 3. Attach nested namespace properties (`anyagents.memory.write`, `anyagents.web.fetch`, etc.)
    //    before freezing `anyagents` and `tools`.
    let patched = patched.replace(
        "        Object.freeze(tools);",
        r#"        if (tools.anyagents) {
          const merged = Object.create(null);
          for (const k of Object.keys(tools.anyagents)) merged[k] = tools.anyagents[k];
          for (const sub of ["memory", "web", "workstation", "computer", "device", "media"]) {
            if (tools[sub]) {
              const baseFn = tools.anyagents[sub];
              const callable = typeof baseFn === "function"
                ? Object.assign((a0, a1) => baseFn(a0, a1), tools[sub])
                : tools[sub];
              Object.freeze(callable);
              merged[sub] = callable;
              tools[sub] = callable;
              globalThis[sub] = callable;
            }
          }
          Object.freeze(merged);
          tools.anyagents = merged;
          globalThis.anyagents = merged;
        }
        Object.freeze(tools);"#,
    );
    // 4. Support `workspace.read(path, opts)` / `workspace.write(path, content)` / `workspace.patch(str)`
    //    when passing a string shorthand + options object.
    patched.replace(
        "if (method === \"list\" && typeof a0 === \"string\") return { ...(a1 || {}), path: a0 };",
        "if (method === \"list\" && typeof a0 === \"string\") return { ...(a1 || {}), path: a0 };\n            if (method === \"patch\" && typeof a0 === \"string\") return { ...(a1 || {}), patch: a0 };",
    )
}

pub(super) struct CodeChildGuard {
    pub(super) child: Option<std::process::Child>,
}

impl Drop for CodeChildGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            #[cfg(unix)]
            {
                let pgid = child.id() as i32;
                if pgid > 0 {
                    unsafe {
                        let _ = libc::kill(-pgid, libc::SIGKILL);
                    }
                }
            }
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// Drive `work` until it finishes or the moving deadline (milliseconds after
/// `started`) passes.
async fn until_deadline<F: std::future::Future>(
    started: Instant,
    deadline_ms: &AtomicU64,
    work: F,
) -> Result<F::Output, ()> {
    let mut work = std::pin::pin!(work);
    loop {
        let at = started + std::time::Duration::from_millis(deadline_ms.load(Ordering::SeqCst));
        tokio::select! {
            out = &mut work => return Ok(out),
            () = tokio::time::sleep_until(at.into()) => {
                let now = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
                if now >= deadline_ms.load(Ordering::SeqCst) {
                    return Err(());
                }
            }
        }
    }
}

/// Fit a `workspace.exec` a snippet runs into the time its `code` call has.
pub(super) fn budget_code_call(
    tool: &str,
    mut args: Value,
    elapsed: std::time::Duration,
    deadline_ms: &AtomicU64,
    max_ms: u64,
) -> Value {
    if tool.trim().trim_start_matches("mcp__anyagents__") != "files" {
        return args;
    }
    let Some(obj) = args.as_object_mut() else {
        return args;
    };
    let action = obj
        .get("action")
        .and_then(Value::as_str)
        .map(|a| a.trim().to_ascii_lowercase())
        .unwrap_or_default();
    let is_exec = match action.as_str() {
        "exec" | "bash" | "sh" | "command" => true,
        "" => obj.contains_key("cmd") || obj.contains_key("command"),
        _ => false,
    };
    if !is_exec {
        return args;
    }
    let elapsed_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
    let wanted = ["timeout_ms", "timeout"]
        .iter()
        .find_map(|key| obj.get(*key).and_then(Value::as_u64));
    let Some(wanted) = wanted else {
        let left = deadline_ms
            .load(Ordering::SeqCst)
            .saturating_sub(elapsed_ms)
            .saturating_sub(CODE_CALL_GRACE_MS)
            .max(100);
        obj.insert("timeout_ms".to_string(), left.into());
        return args;
    };
    let room = max_ms
        .saturating_sub(elapsed_ms)
        .saturating_sub(CODE_CALL_GRACE_MS)
        .max(100);
    let granted = wanted.min(room);
    if granted < wanted {
        obj.remove("timeout");
        obj.insert("timeout_ms".to_string(), granted.into());
        obj.insert("requested_timeout_ms".to_string(), wanted.into());
    }
    let needed = elapsed_ms
        .saturating_add(granted)
        .saturating_add(CODE_CALL_GRACE_MS)
        .min(max_ms);
    deadline_ms.fetch_max(needed, Ordering::SeqCst);
    args
}

fn find_js_runtime() -> Result<(PathBuf, bool), String> {
    if let Ok(resolved) = anyagents_toolchain::global().first_of(&["node", "bun"], "mcp.js") {
        let is_node = resolved.name == "node";
        return Ok((resolved.path, is_node));
    }
    let (path, kind) = agent_tools::code_mode::detect_js_runtime()?;
    Ok((path, kind == agent_tools::code_mode::JsRuntimeKind::Node))
}

impl AgentMcp {
    /// Build a [`ToolRegistry`] reflecting this agent's active MCP profile and
    /// return its generated TypeScript declarations (`.d.ts`).
    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn code_mode_registry_for_inspection(&self) -> ToolRegistry {
        let limits = CodeLimits::current();
        let ctx = CodeExecContext {
            mcp: self.clone(),
            started: Instant::now(),
            deadline_ms: Arc::new(AtomicU64::new(limits.default_ms)),
            max_ms: limits.max_ms,
            deliveries: Arc::new(Mutex::new(Vec::new())),
            media_blocks: Arc::new(Mutex::new(Vec::new())),
        };
        build_registry(ctx)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn code_mode_ts_declarations(&self) -> String {
        let reg = self.code_mode_registry_for_inspection();
        generate_ts_declarations(&reg)
    }

    /// Resolve a remote sandbox backend if one is configured for this agent.
    fn resolve_remote_sandbox_backend(&self) -> Option<Arc<dyn WorkspaceBackend>> {
        let local_root = self
            .agent
            .workdir
            .clone()
            .unwrap_or_else(|| PathBuf::from("/workspace"));
        let backend = crate::sandbox_providers::resolve_workspace_backend_for_mode(
            Some(&self.client.config),
            local_root,
            crate::sandbox_providers::SandboxMode::RemoteRequired,
        )
        .ok()?;
        backend.is_remote().then(|| Arc::from(backend))
    }

    pub(super) async fn execute_code_tool(&self, args: CodeArgs) -> Result<MultimediaJson, String> {
        let limits = CodeLimits::current();
        let timeout_ms = limits.resolve(args.timeout_ms);
        let started = Instant::now();
        let deadline_ms = Arc::new(AtomicU64::new(timeout_ms));
        let deliveries = Arc::new(Mutex::new(Vec::<Value>::new()));
        let media_blocks = Arc::new(Mutex::new(Vec::<rmcp::model::ContentBlock>::new()));

        let ctx = CodeExecContext {
            mcp: self.clone(),
            started,
            deadline_ms: Arc::clone(&deadline_ms),
            max_ms: limits.max_ms,
            deliveries: Arc::clone(&deliveries),
            media_blocks: Arc::clone(&media_blocks),
        };
        let registry = build_registry(ctx);
        let specs = registry.specs();
        let dispatcher = registry.into_dispatcher();

        // Optional in-sandbox execution mode: when an agent has a remote sandbox backend
        // AND an explicit sandbox-reachable RPC URL (or `ANYAGENTS_SANDBOX_CODE_EXEC=sandbox`)
        // is configured, execute the snippet strictly inside the remote sandbox container
        // using `agent_tools::code_mode::CodeModeRunner::execute_in_sandbox` +
        // `SANDBOX_HTTP_CLIENT_JS` over bearer-authenticated HTTP JSON-RPC 2.0.
        if let Some(remote_backend) = self.resolve_remote_sandbox_backend()
            && std::env::var("ANYAGENTS_SANDBOX_CODE_EXEC")
                .ok()
                .is_some_and(|v| v.trim().eq_ignore_ascii_case("sandbox"))
        {
            let runner = CodeModeRunner::new(dispatcher.clone()).with_limits(KitCodeLimits {
                default_ms: limits.default_ms,
                max_ms: limits.max_ms,
                max_output_bytes: MAX_CODE_RESULT_BYTES,
                max_tool_calls: MAX_CODE_TOOL_CALLS,
            });
            let rpc_server = runner
                .serve_http_rpc("127.0.0.1:0".parse().expect("loopback addr"))
                .await?;
            let endpoint_url = std::env::var("ANYAGENTS_SANDBOX_RPC_URL")
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| rpc_server.endpoint_url());
            let exec_res = runner
                .execute_in_sandbox(
                    remote_backend,
                    Some((endpoint_url, rpc_server.token().to_string())),
                    CodeExecRequest {
                        code: args.code.clone(),
                        timeout_ms: args.timeout_ms,
                        max_output_bytes: Some(MAX_CODE_RESULT_BYTES),
                        max_tool_calls: Some(MAX_CODE_TOOL_CALLS),
                    },
                )
                .await;
            drop(rpc_server);
            let deliveries = deliveries
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            let media_blocks = media_blocks
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            if !exec_res.ok {
                let err = exec_res
                    .error
                    .unwrap_or_else(|| "sandbox code execution failed".into());
                return Err(format_code_error(&args.code, &err, false, &exec_res.logs));
            }
            return self.finalize_code_result(
                exec_res.result,
                deliveries,
                media_blocks,
                exec_res.logs,
                exec_res.duration_ms,
                None,
            );
        }

        // Standard host-free VM isolate driven by `agent_tools::code_mode::SANDBOX_WORKER_JS`
        // + `ToolDispatcher::dispatch_batch` (microtask-batched concurrent `Promise.all`).
        let (runtime_bin, is_node) = find_js_runtime()?;
        let worker_js = compose_worker_js();
        let mut cmd = Command::new(&runtime_bin);
        cmd.env_clear()
            .env("PATH", "/usr/local/bin:/usr/bin:/bin")
            .env("DESK_NODE_SHIM", "off")
            .env("NODE_NO_WARNINGS", "1");
        if let Some(home) = std::env::var_os("HOME") {
            cmd.env("HOME", home);
        }
        if is_node {
            cmd.arg("--no-warnings");
        }
        cmd.arg("-e")
            .arg(&worker_js)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt as _;
            cmd.process_group(0);
        }

        let mut child = cmd.spawn().map_err(|e| {
            format!(
                "failed to spawn code sandbox ({}): {e}",
                runtime_bin.display()
            )
        })?;
        let mut child_stdin = child
            .stdin
            .take()
            .ok_or_else(|| "code sandbox stdin missing".to_string())?;
        let child_stdout = child
            .stdout
            .take()
            .ok_or_else(|| "code sandbox stdout missing".to_string())?;
        let child_stderr = child
            .stderr
            .take()
            .ok_or_else(|| "code sandbox stderr missing".to_string())?;
        let _guard = CodeChildGuard { child: Some(child) };

        let start_payload = json!({
            "type": "start",
            "code": args.code,
            "namespaces": specs,
        });
        let mut start_line = serde_json::to_vec(&start_payload).map_err(|e| e.to_string())?;
        start_line.push(b'\n');
        child_stdin
            .write_all(&start_line)
            .and_then(|()| child_stdin.flush())
            .map_err(|e| format!("failed to send code to sandbox: {e}"))?;

        let shared_stdin = Arc::new(Mutex::new(child_stdin));
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Value>();

        std::thread::spawn(move || {
            let reader = BufReader::new(child_stdout);
            for line in reader.lines().map_while(Result::ok) {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                if let Ok(val) = serde_json::from_str::<Value>(trimmed)
                    && tx.send(val).is_err()
                {
                    break;
                }
            }
        });

        let stderr_handle = std::thread::spawn(move || {
            use std::io::Read as _;
            let mut buf = Vec::new();
            let mut take = child_stderr.take(16 * 1024);
            let _ = take.read_to_end(&mut buf);
            String::from_utf8_lossy(&buf).into_owned()
        });

        let mut in_flight = tokio::task::JoinSet::new();
        let call_counter = Arc::new(AtomicUsize::new(0));
        let children = Arc::new(code_children::CodeChildren::default());
        let _end_children = code_children::EndOnDrop(Arc::clone(&children));
        let code_call = current_call().with_children(Arc::clone(&children));

        let loop_res = until_deadline(started, &deadline_ms, async {
            while let Some(msg) = rx.recv().await {
                let msg_type = msg.get("type").and_then(Value::as_str).unwrap_or("");
                match msg_type {
                    "batch" => {
                        let calls: Vec<ToolCallRequest> = msg
                            .get("calls")
                            .cloned()
                            .and_then(|v| serde_json::from_value(v).ok())
                            .unwrap_or_default();
                        let d: ToolDispatcher = dispatcher.clone();
                        let counter = Arc::clone(&call_counter);
                        let stdin_clone = Arc::clone(&shared_stdin);
                        let call = code_call.clone();
                        in_flight.spawn(as_call(call, async move {
                            let replies = d
                                .dispatch_batch(calls, MAX_CODE_TOOL_CALLS, Some(&counter))
                                .await;
                            let frame = json!({
                                "type": "batch_reply",
                                "replies": replies,
                            });
                            if let Ok(mut guard) = stdin_clone.lock() {
                                let _ = writeln!(guard, "{frame}");
                                let _ = guard.flush();
                            }
                        }));
                    }
                    "call" => {
                        if let Ok(req) = serde_json::from_value::<ToolCallRequest>(msg) {
                            let d: ToolDispatcher = dispatcher.clone();
                            let counter = Arc::clone(&call_counter);
                            let stdin_clone = Arc::clone(&shared_stdin);
                            let call = code_call.clone();
                            in_flight.spawn(as_call(call, async move {
                                let count = counter.fetch_add(1, Ordering::SeqCst) + 1;
                                let reply = if count > MAX_CODE_TOOL_CALLS {
                                    agent_tools::code_mode::ToolCallResponse::err(
                                        req.id,
                                        agent_tools::code_mode::RpcError::budget_exceeded(format!(
                                            "exceeded maximum of {MAX_CODE_TOOL_CALLS} tool calls per `code` turn"
                                        )),
                                    )
                                } else {
                                    d.dispatch_one(req).await
                                };
                                let frame = json!({
                                    "type": "reply",
                                    "id": reply.id,
                                    "ok": reply.ok,
                                    "result": reply.result,
                                    "error": reply.error,
                                });
                                if let Ok(mut guard) = stdin_clone.lock() {
                                    let _ = writeln!(guard, "{frame}");
                                    let _ = guard.flush();
                                }
                            }));
                        }
                    }
                    "done" => return Ok(msg),
                    _ => {}
                }
            }
            Err("code sandbox exited before returning a result".to_string())
        })
        .await;

        in_flight.abort_all();
        drop(shared_stdin);
        children.end();
        let stopped = children.settle(code_children::SETTLE).await;
        let stopped_note = code_children::stopped_note(&stopped);

        let done_msg = match loop_res {
            Ok(Ok(msg)) => msg,
            Ok(Err(err)) => {
                let stderr = stderr_handle.join().unwrap_or_default();
                let mut msg = if stderr.trim().is_empty() {
                    err
                } else {
                    format!("{err}: {}", stderr.trim())
                };
                if let Some(note) = stopped_note {
                    msg.push_str(&format!("\n{note}"));
                }
                return Err(msg);
            }
            Err(()) => {
                let mut msg = code_contract::code_timeout_message(
                    deadline_ms.load(Ordering::SeqCst),
                    args.timeout_ms,
                    limits.max_ms,
                );
                if let Some(note) = stopped_note {
                    msg.push_str(&format!("\n{note}"));
                }
                return Err(msg);
            }
        };

        let duration_ms = started.elapsed().as_millis() as u64;
        let logs: Vec<String> = done_msg
            .get("logs")
            .and_then(Value::as_array)
            .map(|arr| {
                arr.iter()
                    .filter_map(Value::as_str)
                    .map(ToOwned::to_owned)
                    .collect()
            })
            .unwrap_or_default();

        let ok = done_msg.get("ok").and_then(Value::as_bool).unwrap_or(false);
        if !ok {
            let err = done_msg
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("unknown code execution error")
                .to_string();
            let compile = done_msg.get("phase").and_then(Value::as_str) == Some("compile");
            let mut full = format_code_error(&args.code, &err, compile, &logs);
            if let Some(note) = stopped_note {
                full.push_str(&format!("\n{note}"));
            }
            return Err(full);
        }

        let raw_result = done_msg.get("result").cloned().unwrap_or(Value::Null);
        let deliveries = deliveries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let media_blocks = media_blocks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        self.finalize_code_result(
            raw_result,
            deliveries,
            media_blocks,
            logs,
            duration_ms,
            stopped_note,
        )
    }

    fn finalize_code_result(
        &self,
        raw_result: Value,
        deliveries: Vec<Value>,
        media_blocks: Vec<rmcp::model::ContentBlock>,
        logs: Vec<String>,
        duration_ms: u64,
        stopped_note: Option<String>,
    ) -> Result<MultimediaJson, String> {
        let serialized = serde_json::to_string(&raw_result).unwrap_or_else(|_| "null".to_string());
        let mut preservation_error = None;
        if serialized.len() > MAX_CODE_RESULT_BYTES
            && !contains_media(&raw_result)
            && self.profile == McpProfile::Full
        {
            static CODE_SPILL_SEQ: AtomicU64 = AtomicU64::new(0);
            let dir = agent_spill_dir(&self.client.config, &self.agent.id);
            let seq = CODE_SPILL_SEQ.fetch_add(1, Ordering::Relaxed);
            match spill_file("code-result", serialized.as_bytes(), &dir, seq) {
                Ok(path) => {
                    let preview_end = floor_char_boundary_str(&serialized, 1024);
                    let mut value = json!({
                        "result_file": path.display().to_string(),
                        "artifact": artifact_info(&path, serialized.as_bytes()),
                        "preview": &serialized[..preview_end],
                        "offloaded": true,
                        "truncated": false,
                        "deliveries": deliveries,
                        "logs": logs,
                        "duration_ms": duration_ms,
                        "hint": "Full result was saved to `result_file` (lossless). Inspect it via workspace.read({ path: result_file, offset_bytes, max_bytes }) or workspace.grep.",
                    });
                    if let Some(note) = stopped_note {
                        value["stopped"] = note.into();
                    }
                    return Ok(MultimediaJson(value, media_blocks));
                }
                Err(err) => {
                    preservation_error = Some(format!(
                        "Artifact unavailable; original result retained inline: {err}"
                    ));
                }
            }
        }

        let mut value = json!({
            "result": raw_result,
            "deliveries": deliveries,
            "truncated": false,
            "logs": logs,
            "duration_ms": duration_ms,
        });
        if let Some(error) = preservation_error {
            value["preservation_error"] = error.into();
        }
        if let Some(note) = stopped_note {
            value["stopped"] = note.into();
        }
        Ok(MultimediaJson(value, media_blocks))
    }
}

fn format_code_error(code: &str, err: &str, compile: bool, logs: &[String]) -> String {
    let mut full = err.to_string();
    if let Some(fix) = code_contract::diagnose(code, err, compile) {
        full.push_str(&format!("\nfix: {fix}"));
    }
    if !logs.is_empty() {
        full.push_str(&format!("\nlogs:\n{}", logs.join("\n")));
    }
    full
}
