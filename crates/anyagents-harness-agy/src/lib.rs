//! Spec: `docs/vision.md` "First version" item 2 (any harness through one
//! adapter contract) and the architecture head "Lazy harness setup".
//!
//! The Antigravity harness is one catalog entry, not a copied adapter: this
//! crate is the thin name the supervisor starts it by. Its launch shape, closing
//! line and usage come from `StreamSpec::for_program("agy")`.

use std::path::{Path, PathBuf};

use anyagents_harness::{
    DeliverOutcome, DeliveryMode, Harness, HarnessError, HarnessHealth, SteerSupport, TurnUsage,
};
pub use anyagents_harness_stream::detect_agy_session_quota_and_error;
use anyagents_harness_stream::{
    AgyRpcConfig, Launch, StreamHarness, StreamSpec, cancel_line, message_line,
};

/// One long-lived Antigravity child.
pub struct AgyHarness {
    inner: StreamHarness,
    spec: StreamSpec,
}

impl AgyHarness {
    /// Start one child.
    ///
    /// `program` is the `agy` executable, or a test double. `session_id`, when
    /// set, is passed as `--resume`.
    pub fn start(
        program: impl Into<PathBuf>,
        session_id: Option<&str>,
        environment: &[(&str, &str)],
    ) -> Result<Self, HarnessError> {
        let spec = StreamSpec::for_program("agy");
        Self::start_with_spec(spec, program, session_id, environment, None)
    }

    /// Start one child with a custom `StreamSpec` and optional working directory.
    pub fn start_with_spec(
        spec: StreamSpec,
        program: impl Into<PathBuf>,
        session_id: Option<&str>,
        environment: &[(&str, &str)],
        work_dir: Option<&Path>,
    ) -> Result<Self, HarnessError> {
        let environment: Vec<(String, String)> = environment
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect();
        let mut args = spec.args.clone();
        if let Some(id) = session_id {
            args.push(spec.resume_flag.to_string());
            args.push(id.to_string());
        }
        let (mode, steer) = if work_dir.is_some() {
            (DeliveryMode::Steer, SteerSupport::Yes)
        } else {
            (DeliveryMode::Followup, SteerSupport::Unproven)
        };
        let fake_mode = std::env::var_os("ANYAGENTS_FAKE_MODE").is_some()
            || environment.iter().any(|(k, _)| k == "ANYAGENTS_FAKE_MODE");
        let agy_rpc = if work_dir.is_some() && !fake_mode {
            environment
                .iter()
                .find(|(k, _)| k == "ANYAGENTS_AGENT_CONFIG" || k == "AGY_CONFIG_DIR")
                .map(|(_, v)| {
                    let csrf_token = environment
                        .iter()
                        .find(|(k, _)| k == "ANYAGENTS_AGY_CSRF_TOKEN")
                        .map(|(_, val)| val.clone())
                        .unwrap_or_else(|| {
                            let nanos = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .map(|d| d.as_nanos())
                                .unwrap_or(0);
                            format!("agy-csrf-{}-{nanos}", std::process::id())
                        });
                    if !args.iter().any(|a| a == "--csrf_token") {
                        args.push("--csrf_token".to_string());
                        args.push(csrf_token.clone());
                    }
                    let port_override = environment
                        .iter()
                        .find(|(k, _)| k == "ANYAGENTS_AGY_HTTP_PORT")
                        .and_then(|(_, val)| val.parse::<u16>().ok());
                    AgyRpcConfig {
                        config_dir: PathBuf::from(v),
                        csrf_token,
                        port_override,
                    }
                })
        } else {
            None
        };
        let inner = StreamHarness::launch_with_agy_rpc(
            Launch {
                spec: spec.clone(),
                program: program.into(),
                args,
                environment,
                work_dir: work_dir.map(Path::to_path_buf),
                session_id: session_id.map(str::to_string),
                mode,
                steer,
            },
            agy_rpc,
        )?;
        if let Some(id) = session_id {
            inner.seed_session_id(id);
        }
        Ok(Self { inner, spec })
    }

    pub fn written_ids(&self) -> Vec<String> {
        self.inner.written_ids()
    }
    pub fn turns_started(&self) -> usize {
        self.inner.turns_started()
    }
    pub fn session_id(&self) -> Option<String> {
        self.inner.session_id()
    }
    pub fn wait_finished(&self, id: &str, timeout: std::time::Duration) -> bool {
        self.inner.wait_finished(id, timeout)
    }
    pub fn wait_quota(&self, timeout: std::time::Duration) -> Option<i64> {
        self.inner.wait_quota(timeout)
    }
    pub fn seen(&self, id: &str) -> bool {
        self.inner.seen(id)
    }
    pub fn wait_seen(&self, id: &str, timeout: std::time::Duration) -> bool {
        self.inner.wait_seen(id, timeout)
    }
    pub fn usage_for(&self, id: &str) -> Option<TurnUsage> {
        self.inner.usage_for(id)
    }
}

impl Harness for AgyHarness {
    fn deliver(
        &mut self,
        message: &anyagents_harness::OutboundMessage,
    ) -> Result<DeliverOutcome, HarnessError> {
        self.inner
            .deliver(message, &message_line(&self.spec, message))
    }

    fn background(&mut self) -> Result<(), HarnessError> {
        self.inner.background()
    }

    fn resume(&mut self, session: &str) -> Result<(), HarnessError> {
        self.inner.resume(session)
    }

    fn cancel(&mut self) -> Result<(), HarnessError> {
        self.inner.cancel(&cancel_line(&self.spec))
    }

    fn input_ready(&self) -> bool {
        self.inner.input_ready()
    }

    fn health(&self) -> HarnessHealth {
        self.inner.health()
    }

    fn clear_quota(&mut self) {
        self.inner.clear_quota();
    }

    fn take_reclaimed_steers(&mut self) -> Vec<String> {
        self.inner.take_reclaimed_steers()
    }

    fn process_id(&self) -> Option<u32> {
        self.inner.process_id()
    }

    fn turns_served(&self) -> u32 {
        self.inner.turns_served()
    }

    fn served_usage(&self) -> TurnUsage {
        self.inner.served_usage()
    }

    fn watch_activity(&self, notify: std::sync::Arc<dyn Fn() + Send + Sync>) {
        self.inner.watch_activity(notify);
    }
}

/// Native tool policy: the one function behind both the advertised set
/// (`settings.json` `permissions.deny`) and the hook allow predicate
/// (`hooks.json` `anyagents-native-tool-guard` matcher), so a tool is
/// advertised if and only if the hook allows it.
///
/// `agy` has no supported `settings.json` key to exclude tools from what the
/// model sees: only `queuedMessages`, `trustedWorkspaces` and `permissions`
/// validate (other keys are rejected as `unknown setting`), and
/// `permissions.deny` blocks at permission time without hiding the tool.
/// The model therefore still proposes every native built-in below, and the
/// static deny plus hook are the enforcement. Both derive below; tools outside
/// this known set stay usable natively and are listed in the pull-request
/// description rather than silently assumed covered.
///
/// File/exec tools are gated by `ANYAGENTS_ALLOW_NATIVE_FILE_TOOLS == "1"`;
/// messaging/schedule/web/browser tools are gated by the presence of
/// `ANYAGENTS_ALLOW_NATIVE_SUBAGENTS`. `read_file`/`view_file` stay out of
/// static deny (which overrides a hook allow in `agy`) and are allowed only
/// for MCP schema reads under `~/.gemini/antigravity-cli/mcp/`.
pub const AGY_FILE_TOOLS: &[&str] = &[
    "read_file",
    "view_file",
    "view_file_outline",
    "view_code_item",
    "view_content_chunk",
    "code_action",
    "write_file",
    "edit_file",
    "grep_search",
    "find_by_name",
    "find",
    "list_dir",
    "list_directory",
    "run_command",
    "manage_task",
    "send_command_input",
    "command_status",
    "shell_exec",
];

pub const AGY_SUBAGENT_TOOLS: &[&str] = &[
    "invoke_subagent",
    "define_subagent",
    "manage_subagents",
    "send_message",
    "schedule",
    "search_web",
    "read_url_content",
    "browser_subagent",
    "open_browser_url",
    "browser_*",
];

/// `(allow_file_tools, allow_subagents)` from the process environment.
pub fn agy_allow_flags_from_env() -> (bool, bool) {
    let allow_file = std::env::var("ANYAGENTS_ALLOW_NATIVE_FILE_TOOLS")
        .ok()
        .is_some_and(|v| v.trim() == "1");
    let allow_subagents = std::env::var_os("ANYAGENTS_ALLOW_NATIVE_SUBAGENTS").is_some();
    (allow_file, allow_subagents)
}

/// Whether the hook allows `name` unconditionally (ignoring the
/// `read_file`/`view_file` MCP-schema path exception, which the hook
/// command implements). `browser_*` matches the `browser_` family.
pub fn agy_tool_is_allowed(name: &str, allow_file_tools: bool, allow_subagents: bool) -> bool {
    if AGY_FILE_TOOLS.contains(&name) {
        return allow_file_tools;
    }
    if AGY_SUBAGENT_TOOLS.contains(&name) {
        return allow_subagents;
    }
    if name.starts_with("browser_") && AGY_SUBAGENT_TOOLS.contains(&"browser_*") {
        return allow_subagents;
    }
    // Unknown tools are outside the generated config: allowed by both sides.
    true
}

/// Hook tool names denied unconditionally (including `read_file`/`view_file`,
/// which the hook then narrows to MCP schema reads).
pub fn agy_denied_hook_tools(allow_file_tools: bool, allow_subagents: bool) -> Vec<String> {
    let mut out = Vec::new();
    if !allow_file_tools {
        out.extend(AGY_FILE_TOOLS.iter().map(|s| (*s).to_string()));
    }
    if !allow_subagents {
        out.extend(AGY_SUBAGENT_TOOLS.iter().map(|s| (*s).to_string()));
    }
    out
}

/// `permissions.deny` entries for the denied set, excluding
/// `read_file`/`view_file` (see module docs).
pub fn agy_static_deny_entries(allow_file_tools: bool, allow_subagents: bool) -> Vec<String> {
    let mut out = Vec::new();
    for tool in agy_denied_hook_tools(allow_file_tools, allow_subagents) {
        match tool.as_str() {
            "run_command" => {
                out.push("run_command".to_string());
                out.push("command(*)".to_string());
            }
            "read_url_content" => {
                out.push("read_url_content".to_string());
                out.push("read_url(*)".to_string());
            }
            "open_browser_url" => {
                out.push("open_browser_url".to_string());
                out.push("execute_url(*)".to_string());
            }
            "write_file" => {
                out.push("write_file".to_string());
                out.push("write_file(*)".to_string());
            }
            "read_file" | "view_file" => {}
            _ => out.push(tool),
        }
    }
    out
}

/// Every permission string this policy can generate (for healing stale
/// entries when the flags flip).
pub fn agy_all_static_deny_entries() -> Vec<String> {
    agy_static_deny_entries(false, false)
}

/// `PreToolUse` matcher, or `None` when nothing is denied (hook removed).
pub fn agy_hook_matcher(allow_file_tools: bool, allow_subagents: bool) -> Option<String> {
    let denied = agy_denied_hook_tools(allow_file_tools, allow_subagents);
    if denied.is_empty() {
        return None;
    }
    let parts: Vec<String> = denied.iter().map(|t| t.replace('*', ".*")).collect();
    Some(format!("^({})$", parts.join("|")))
}

/// Hook command: denies every matched tool except `read_file`/`view_file`
/// MCP schema reads, which it allows.
pub fn agy_hook_command() -> String {
    "python3 -c 'import json,os,sys\ndeny=\"Native built-in tools are disabled under AnyAgents. Invoke AnyAgents MCP tools via call_mcp_tool with ServerName=\\\"anyagents\\\" (read schemas in ~/.gemini/antigravity-cli/mcp/anyagents/*.json if needed): e.g. ToolName=\\\"code\\\" with Arguments={\\\"code\\\":\\\"return await workspace.read({ path: \\\\\\\"...\\\\\\\" });\\\"} for workspace/web/anyagents operations, ToolName=\\\"send\\\" with Arguments={\\\"to\\\":[\\\"parent\\\"],\\\"text\\\":\\\"...\\\"} to reply, ToolName=\\\"agents\\\", ToolName=\\\"schedules\\\", or ToolName=\\\"skills\\\".\"\ntry:\n d=json.load(sys.stdin);tc=d.get(\"toolCall\") or {}\n if tc.get(\"name\") in (\"read_file\",\"view_file\"):\n  p=(tc.get(\"args\") or {}).get(\"Path\") or \"\"\n  mcp=os.path.realpath(os.path.expanduser(\"~/.gemini/antigravity-cli/mcp\"))\n  rp=os.path.realpath(os.path.expanduser(p)) if p else \"\"\n  if rp.startswith(mcp+\"/\") and rp.endswith(\".json\") and os.path.isfile(rp):\n   print(json.dumps({\"decision\":\"allow\",\"permissionOverrides\":[\"read_file(*)\",\"read_file\",\"view_file\"]}));sys.exit(0)\nexcept Exception:\n pass\nprint(json.dumps({\"decision\":\"deny\",\"reason\":deny}))' 2>/dev/null || printf '%s\\n' '{\"decision\":\"deny\",\"reason\":\"Native built-in tools are disabled under AnyAgents; invoke AnyAgents MCP tools via call_mcp_tool with ServerName=\\\"anyagents\\\" and ToolName=\\\"code\\\" or \\\"send\\\" instead.\"}'".to_string()
}
