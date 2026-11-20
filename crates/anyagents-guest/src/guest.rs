//! Embedded 9-harness guest supervisor (`anyagents-guest serve`).
//!
//! Launches and supervises any of the 9 local harness adapters inside an
//! isolated sandbox microVM, speaking AnyAgents `stream-json` over stdin/stdout
//! so `envd` Connect-RPC (`process.Process/Start`) streams bidirectional turns,
//! mid-turn `steer`, `cancel`, `TurnUsage`, and persisted session IDs cleanly.

use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::time::{Duration, Instant};

#[cfg(test)]
use anyagents_harness::Delivery;
use anyagents_harness::{
    Context, DeliverOutcome, Harness, HarnessError, OutboundMessage, Proof, TurnState, TurnUsage,
    normalize_harness_name, spawn_command,
};
use anyagents_harness_agy::AgyHarness;
use anyagents_harness_claude::ClaudeHarness;
use anyagents_harness_codex::CodexHarness;
use anyagents_harness_muse::MuseHarness;
use anyagents_harness_omp::OmpHarness;
use anyagents_harness_opencode::OpenCodeHarness;
use anyagents_harness_spiron::{SpironHarness, SpironLaunch};
use anyagents_harness_stream::StreamSpec;
use clap::Args;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::activity::{
    DEFAULT_HEARTBEAT_INTERVAL_MS, GuestActivitySnapshot, clear_activity_snapshot,
    write_activity_snapshot,
};
use crate::gemini::GeminiGuestHarness;

/// Default unified persistent volume mount root inside sandboxes.
pub const DEFAULT_DATA_MOUNT_DIR: &str = "/home/user/agent-data";
/// Default persistent agent state directory (`/home/user/agent-data/state`).
pub const DEFAULT_STATE_DIR: &str = "/home/user/agent-data/state";
/// Default persistent workspace directory (`/home/user/agent-data/workspace`).
pub const DEFAULT_WORKDIR: &str = "/home/user/agent-data/workspace";

const GUEST_CONFIG_FILE: &str = "guest-config.json";
const REPO_SPEC_FILE: &str = "repo-spec.json";
const IDLE_TICK: Duration = Duration::from_millis(80);

#[derive(Args, Clone, Debug, Default)]
pub struct HealthArgs {
    /// Persistent agent state directory (defaults to `/home/user/agent-data/state`).
    #[arg(long)]
    pub state_dir: Option<PathBuf>,
    /// Persistent workspace directory (defaults to `/home/user/agent-data/workspace`).
    #[arg(long)]
    pub workdir: Option<PathBuf>,
}

#[derive(Args, Clone, Debug)]
pub struct ServeArgs {
    /// Target harness selector (`claude-code`, `codex`, `opencode`, `agy`, `muse`, `omp`, `spiron`).
    #[arg(long)]
    pub harness: Option<String>,
    /// Explicit path to the underlying vendor CLI binary inside the sandbox.
    #[arg(long)]
    pub program: Option<PathBuf>,
    /// Persistent agent state directory (defaults to `/home/user/agent-data/state`).
    #[arg(long)]
    pub state_dir: Option<PathBuf>,
    /// Persistent workspace directory (defaults to `/home/user/agent-data/workspace`).
    #[arg(long)]
    pub workdir: Option<PathBuf>,
    /// Model identifier override.
    #[arg(long)]
    pub model: Option<String>,
    /// Session ID override to resume.
    #[arg(long)]
    pub session: Option<String>,
    /// Explicit git binary path when repo checkout is needed.
    #[arg(long)]
    pub git_program: Option<PathBuf>,
    /// Interval in milliseconds for emitting remote `system/heartbeat` frames (`0` disables stream ticks).
    #[arg(long)]
    pub heartbeat_interval_ms: Option<u64>,
    /// Trailing CLI flags forwarded to compatible adapters.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub extra_args: Vec<String>,
}

/// Resolve the guest state directory from explicit CLI override, env vars, or `/home/user/agent-data/state`.
pub fn resolve_default_state_dir(explicit: Option<PathBuf>) -> PathBuf {
    if let Some(p) = explicit {
        return p;
    }
    for key in ["ANYAGENTS_GUEST_STATE_DIR", "ANYAGENTS_SANDBOX_STATE_DIR"] {
        if let Some(val) = env_non_empty_path(key) {
            return val;
        }
    }
    if let Some(data_dir) = env_non_empty_path("ANYAGENTS_SANDBOX_DATA_DIR") {
        return data_dir.join("state");
    }
    PathBuf::from(DEFAULT_STATE_DIR)
}

/// Resolve the guest working directory from explicit CLI override, env vars, or `/home/user/agent-data/workspace`.
pub fn resolve_default_workdir(explicit: Option<PathBuf>) -> PathBuf {
    if let Some(p) = explicit {
        return p;
    }
    for key in ["ANYAGENTS_GUEST_WORKDIR", "ANYAGENTS_SANDBOX_WORKDIR"] {
        if let Some(val) = env_non_empty_path(key) {
            return val;
        }
    }
    if let Some(data_dir) = env_non_empty_path("ANYAGENTS_SANDBOX_DATA_DIR") {
        return data_dir.join("workspace");
    }
    PathBuf::from(DEFAULT_WORKDIR)
}

fn env_non_empty_path(key: &str) -> Option<PathBuf> {
    std::env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
}

/// Optional configuration synced by the coordinator into `<state-dir>/guest-config.json`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct GuestConfig {
    #[serde(default)]
    pub agent_id: Option<String>,
    #[serde(default)]
    pub agent_name: Option<String>,
    #[serde(default)]
    pub harness: Option<String>,
    #[serde(default)]
    pub program: Option<PathBuf>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub workdir: Option<PathBuf>,
    #[serde(default)]
    pub extra_args: Vec<String>,
    #[serde(default)]
    pub env: Vec<(String, String)>,
    #[serde(default)]
    pub mcp_url: Option<String>,
    #[serde(default)]
    pub mcp_token_file: Option<PathBuf>,
    #[serde(default)]
    pub repo_spec: Option<GuestRepoSpec>,
}

/// Git repository clone/checkout specification synced by coordinator.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GuestRepoSpec {
    pub repo: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base: Option<String>,
}

pub(crate) trait GuestHarness: Harness {
    fn session_id(&self) -> Option<String>;
    fn answer_for(&self, id: &str, state_dir: &Path) -> Option<String>;
    fn usage_for_id(&self, _id: &str) -> Option<TurnUsage> {
        None
    }
    fn failure_for_id(&self, _id: &str) -> Option<String> {
        None
    }
}

/// Normalize CLI harness selectors accepted by `anyagents-guest serve` and `anyagents-guest turn`.
pub(crate) fn canonical_guest_harness(raw: &str) -> String {
    match raw.trim().to_ascii_lowercase().as_str() {
        "claude" | "claude-code" | "claude_code" => "claude-code".to_string(),
        "gemini" | "gemini-cli" | "gemini_cli" => "gemini".to_string(),
        "spiron-local" | "spiron_local" => "spiron".to_string(),
        other => normalize_harness_name(other).to_string(),
    }
}

struct ClaudeAdapter(ClaudeHarness);
impl Harness for ClaudeAdapter {
    fn deliver(&mut self, msg: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        self.0.deliver(msg)
    }
    fn proven(&self, id: &str) -> Option<Proof> {
        self.0.proven(id)
    }
    fn stop(&mut self) {
        self.0.stop();
    }
    fn background(&mut self) -> Result<(), HarnessError> {
        self.0.background()
    }
    fn resume(&mut self, session: &str) -> Result<(), HarnessError> {
        self.0.resume(session)
    }
    fn cancel(&mut self) -> Result<(), HarnessError> {
        self.0.cancel()
    }
    fn health(&self) -> anyagents_harness::HarnessHealth {
        self.0.health()
    }
    fn served_usage(&self) -> TurnUsage {
        self.0.served_usage()
    }
    fn turns_served(&self) -> u32 {
        self.0.turns_served()
    }
    fn exit_diagnostic(&self) -> Option<String> {
        self.0.exit_diagnostic()
    }
    fn watch_activity(&self, notify: Arc<dyn Fn() + Send + Sync>) {
        self.0.watch_activity(notify);
    }
}
impl GuestHarness for ClaudeAdapter {
    fn session_id(&self) -> Option<String> {
        self.0.session_id()
    }
    fn answer_for(&self, id: &str, state_dir: &Path) -> Option<String> {
        self.0
            .result_for(id)
            .or_else(|| self.0.task_turn_result(id).map(|(_, text)| text))
            .or_else(|| read_claude_transcript_reply(state_dir, self.0.session_id().as_deref()))
    }
    fn usage_for_id(&self, id: &str) -> Option<TurnUsage> {
        self.0.usage_for(id)
    }
    fn failure_for_id(&self, id: &str) -> Option<String> {
        self.0.failure_for(id)
    }
}

struct CodexAdapter(CodexHarness);
impl Harness for CodexAdapter {
    fn deliver(&mut self, msg: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        self.0.deliver(msg)
    }
    fn proven(&self, id: &str) -> Option<Proof> {
        self.0.proven(id)
    }
    fn stop(&mut self) {
        self.0.stop();
    }
    fn background(&mut self) -> Result<(), HarnessError> {
        self.0.background()
    }
    fn resume(&mut self, session: &str) -> Result<(), HarnessError> {
        self.0.resume(session)
    }
    fn cancel(&mut self) -> Result<(), HarnessError> {
        self.0.cancel()
    }
    fn health(&self) -> anyagents_harness::HarnessHealth {
        self.0.health()
    }
    fn served_usage(&self) -> TurnUsage {
        self.0.served_usage()
    }
    fn turns_served(&self) -> u32 {
        self.0.turns_served()
    }
    fn exit_diagnostic(&self) -> Option<String> {
        self.0.exit_diagnostic()
    }
    fn watch_activity(&self, notify: Arc<dyn Fn() + Send + Sync>) {
        self.0.watch_activity(notify);
    }
}
impl GuestHarness for CodexAdapter {
    fn session_id(&self) -> Option<String> {
        self.0.session_id()
    }
    fn answer_for(&self, id: &str, _state_dir: &Path) -> Option<String> {
        self.0.task_turn_result(id).map(|(_, text)| text)
    }
    fn usage_for_id(&self, id: &str) -> Option<TurnUsage> {
        self.0.usage_for(id)
    }
}

struct OpenCodeAdapter(OpenCodeHarness);
impl Harness for OpenCodeAdapter {
    fn deliver(&mut self, msg: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        self.0.deliver(msg)
    }
    fn proven(&self, id: &str) -> Option<Proof> {
        self.0.proven(id)
    }
    fn stop(&mut self) {
        self.0.stop();
    }
    fn background(&mut self) -> Result<(), HarnessError> {
        self.0.background()
    }
    fn resume(&mut self, session: &str) -> Result<(), HarnessError> {
        self.0.resume(session)
    }
    fn cancel(&mut self) -> Result<(), HarnessError> {
        self.0.cancel()
    }
    fn health(&self) -> anyagents_harness::HarnessHealth {
        self.0.health()
    }
    fn served_usage(&self) -> TurnUsage {
        self.0.served_usage()
    }
    fn turns_served(&self) -> u32 {
        self.0.turns_served()
    }
    fn exit_diagnostic(&self) -> Option<String> {
        self.0.exit_diagnostic()
    }
    fn watch_activity(&self, notify: Arc<dyn Fn() + Send + Sync>) {
        self.0.watch_activity(notify);
    }
}
impl GuestHarness for OpenCodeAdapter {
    fn session_id(&self) -> Option<String> {
        self.0.session_id()
    }
    fn answer_for(&self, id: &str, _state_dir: &Path) -> Option<String> {
        self.0.answer(id)
    }
}

struct AgyAdapter(AgyHarness);
impl Harness for AgyAdapter {
    fn deliver(&mut self, msg: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        self.0.deliver(msg)
    }
    fn proven(&self, id: &str) -> Option<Proof> {
        self.0.proven(id)
    }
    fn stop(&mut self) {
        self.0.stop();
    }
    fn background(&mut self) -> Result<(), HarnessError> {
        self.0.background()
    }
    fn resume(&mut self, session: &str) -> Result<(), HarnessError> {
        self.0.resume(session)
    }
    fn cancel(&mut self) -> Result<(), HarnessError> {
        self.0.cancel()
    }
    fn health(&self) -> anyagents_harness::HarnessHealth {
        self.0.health()
    }
    fn served_usage(&self) -> TurnUsage {
        self.0.served_usage()
    }
    fn turns_served(&self) -> u32 {
        self.0.turns_served()
    }
    fn exit_diagnostic(&self) -> Option<String> {
        self.0.exit_diagnostic()
    }
    fn watch_activity(&self, notify: Arc<dyn Fn() + Send + Sync>) {
        self.0.watch_activity(notify);
    }
}
impl GuestHarness for AgyAdapter {
    fn session_id(&self) -> Option<String> {
        self.0.session_id()
    }
    fn answer_for(&self, id: &str, _state_dir: &Path) -> Option<String> {
        self.0.task_turn_result(id).map(|(_, text)| text)
    }
    fn usage_for_id(&self, id: &str) -> Option<TurnUsage> {
        self.0.usage_for(id)
    }
}

struct MuseAdapter(MuseHarness);
impl Harness for MuseAdapter {
    fn deliver(&mut self, msg: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        self.0.deliver(msg)
    }
    fn proven(&self, id: &str) -> Option<Proof> {
        self.0.proven(id)
    }
    fn stop(&mut self) {
        self.0.stop();
    }
    fn background(&mut self) -> Result<(), HarnessError> {
        self.0.background()
    }
    fn resume(&mut self, session: &str) -> Result<(), HarnessError> {
        self.0.resume(session)
    }
    fn cancel(&mut self) -> Result<(), HarnessError> {
        self.0.cancel()
    }
    fn health(&self) -> anyagents_harness::HarnessHealth {
        self.0.health()
    }
    fn served_usage(&self) -> TurnUsage {
        self.0.served_usage()
    }
    fn turns_served(&self) -> u32 {
        self.0.turns_served()
    }
    fn exit_diagnostic(&self) -> Option<String> {
        self.0.exit_diagnostic()
    }
    fn watch_activity(&self, notify: Arc<dyn Fn() + Send + Sync>) {
        self.0.watch_activity(notify);
    }
}
impl GuestHarness for MuseAdapter {
    fn session_id(&self) -> Option<String> {
        self.0.session_id()
    }
    fn answer_for(&self, id: &str, _state_dir: &Path) -> Option<String> {
        self.0.task_turn_result(id).map(|(_, text)| text)
    }
}

struct OmpAdapter(OmpHarness);
impl Harness for OmpAdapter {
    fn deliver(&mut self, msg: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        self.0.deliver(msg)
    }
    fn proven(&self, id: &str) -> Option<Proof> {
        self.0.proven(id)
    }
    fn stop(&mut self) {
        self.0.stop();
    }
    fn background(&mut self) -> Result<(), HarnessError> {
        self.0.background()
    }
    fn resume(&mut self, session: &str) -> Result<(), HarnessError> {
        self.0.resume(session)
    }
    fn cancel(&mut self) -> Result<(), HarnessError> {
        self.0.cancel()
    }
    fn health(&self) -> anyagents_harness::HarnessHealth {
        self.0.health()
    }
    fn served_usage(&self) -> TurnUsage {
        self.0.served_usage()
    }
    fn turns_served(&self) -> u32 {
        self.0.turns_served()
    }
    fn exit_diagnostic(&self) -> Option<String> {
        self.0.exit_diagnostic()
    }
    fn watch_activity(&self, notify: Arc<dyn Fn() + Send + Sync>) {
        self.0.watch_activity(notify);
    }
}
impl GuestHarness for OmpAdapter {
    fn session_id(&self) -> Option<String> {
        self.0.session_id()
    }
    fn answer_for(&self, id: &str, _state_dir: &Path) -> Option<String> {
        self.0.task_turn_result(id).map(|(_, text)| text)
    }
}

struct SpironAdapter(SpironHarness);
impl Harness for SpironAdapter {
    fn deliver(&mut self, msg: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        self.0.deliver(msg)
    }
    fn proven(&self, id: &str) -> Option<Proof> {
        self.0.proven(id)
    }
    fn stop(&mut self) {
        self.0.stop();
    }
    fn background(&mut self) -> Result<(), HarnessError> {
        self.0.background()
    }
    fn resume(&mut self, session: &str) -> Result<(), HarnessError> {
        self.0.resume(session)
    }
    fn cancel(&mut self) -> Result<(), HarnessError> {
        self.0.cancel()
    }
    fn health(&self) -> anyagents_harness::HarnessHealth {
        self.0.health()
    }
    fn served_usage(&self) -> TurnUsage {
        self.0.served_usage()
    }
    fn turns_served(&self) -> u32 {
        self.0.turns_served()
    }
    fn exit_diagnostic(&self) -> Option<String> {
        self.0.exit_diagnostic()
    }
    fn watch_activity(&self, notify: Arc<dyn Fn() + Send + Sync>) {
        self.0.watch_activity(notify);
    }
}
impl GuestHarness for SpironAdapter {
    fn session_id(&self) -> Option<String> {
        self.0.session_id()
    }
    fn answer_for(&self, id: &str, _state_dir: &Path) -> Option<String> {
        self.0
            .text_for(id)
            .or_else(|| self.0.task_turn_result(id).map(|(_, text)| text))
    }
    fn usage_for_id(&self, id: &str) -> Option<TurnUsage> {
        self.0.usage_for(id)
    }
}

struct GeminiAdapter(GeminiGuestHarness);
impl Harness for GeminiAdapter {
    fn deliver(&mut self, msg: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        self.0.deliver(msg)
    }
    fn proven(&self, id: &str) -> Option<Proof> {
        self.0.proven(id)
    }
    fn stop(&mut self) {
        self.0.stop();
    }
    fn background(&mut self) -> Result<(), HarnessError> {
        self.0.background()
    }
    fn resume(&mut self, session: &str) -> Result<(), HarnessError> {
        self.0.resume(session)
    }
    fn cancel(&mut self) -> Result<(), HarnessError> {
        self.0.cancel()
    }
    fn health(&self) -> anyagents_harness::HarnessHealth {
        self.0.health()
    }
    fn served_usage(&self) -> TurnUsage {
        self.0.served_usage()
    }
    fn turns_served(&self) -> u32 {
        self.0.turns_served()
    }
    fn watch_activity(&self, notify: Arc<dyn Fn() + Send + Sync>) {
        self.0.watch_activity(notify);
    }
}
impl GuestHarness for GeminiAdapter {
    fn session_id(&self) -> Option<String> {
        self.0.session_id()
    }
    fn answer_for(&self, id: &str, _state_dir: &Path) -> Option<String> {
        self.0.answer(id)
    }
    fn usage_for_id(&self, id: &str) -> Option<TurnUsage> {
        self.0.usage_for(id)
    }
    fn failure_for_id(&self, id: &str) -> Option<String> {
        self.0.failure_for(id)
    }
}

/// Entry point for `anyagents-guest serve`.
pub fn run_serve(mut args: ServeArgs) -> Result<(), String> {
    if args.harness.is_none() {
        args.harness = std::env::var("ANYAGENTS_GUEST_HARNESS").ok();
    }
    args.state_dir = Some(resolve_default_state_dir(args.state_dir.take()));
    args.workdir = Some(resolve_default_workdir(args.workdir.take()));
    if args.heartbeat_interval_ms.is_none() {
        args.heartbeat_interval_ms = std::env::var("ANYAGENTS_GUEST_HEARTBEAT_INTERVAL_MS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok());
    }
    let reader = BufReader::new(std::io::stdin());
    let stdout = std::io::stdout();
    let mut writer = std::io::BufWriter::new(stdout.lock());
    run_serve_io(args, reader, &mut writer)
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
enum StdinEvent {
    User {
        msg: OutboundMessage,
        uuid: String,
        raw_message: Value,
    },
    Interrupt {
        request_id: Option<String>,
    },
    Eof,
}

#[derive(Clone, Debug)]
struct PendingTurn {
    id: String,
    uuid: String,
    delivered: bool,
    base_usage: TurnUsage,
    base_turns: u32,
}

pub(crate) fn run_serve_io<R: BufRead + Send + 'static, W: Write>(
    args: ServeArgs,
    reader: R,
    writer: &mut W,
) -> Result<(), String> {
    let state_dir = args
        .state_dir
        .clone()
        .unwrap_or_else(|| PathBuf::from(DEFAULT_STATE_DIR));
    let default_workdir = args
        .workdir
        .clone()
        .unwrap_or_else(|| PathBuf::from(DEFAULT_WORKDIR));
    let heartbeat_interval_ms = args
        .heartbeat_interval_ms
        .unwrap_or(DEFAULT_HEARTBEAT_INTERVAL_MS);
    let heartbeat_interval =
        (heartbeat_interval_ms > 0).then(|| Duration::from_millis(heartbeat_interval_ms));
    std::fs::create_dir_all(&state_dir)
        .map_err(|e| format!("cannot create state dir {}: {e}", state_dir.display()))?;
    let file_cfg = load_guest_config(&state_dir);
    let work_dir = file_cfg
        .as_ref()
        .and_then(|c| c.workdir.clone())
        .unwrap_or(default_workdir);
    std::fs::create_dir_all(&work_dir)
        .map_err(|e| format!("cannot create work dir {}: {e}", work_dir.display()))?;

    let repo_spec = file_cfg
        .as_ref()
        .and_then(|c| c.repo_spec.clone())
        .or_else(|| load_repo_spec(&state_dir));
    if let Some(spec) = repo_spec {
        ensure_repo_checked_out(&work_dir, &spec, args.git_program.as_deref())?;
    }

    let raw_harness = args
        .harness
        .clone()
        .or_else(|| file_cfg.as_ref().and_then(|c| c.harness.clone()))
        .unwrap_or_else(|| "claude-code".to_string());
    let harness_name = canonical_guest_harness(&raw_harness);
    let program = args
        .program
        .clone()
        .or_else(|| file_cfg.as_ref().and_then(|c| c.program.clone()))
        .unwrap_or_else(|| resolve_guest_program(&harness_name));
    let model = args
        .model
        .clone()
        .or_else(|| file_cfg.as_ref().and_then(|c| c.model.clone()));
    let session = args
        .session
        .clone()
        .or_else(|| file_cfg.as_ref().and_then(|c| c.session_id.clone()))
        .or_else(|| read_persisted_session(&state_dir, &harness_name));
    let mut extra_args = file_cfg
        .as_ref()
        .map(|c| c.extra_args.clone())
        .unwrap_or_default();
    if !args.extra_args.is_empty() {
        extra_args = args.extra_args.clone();
    }

    let env = build_guest_env(&state_dir, &work_dir, file_cfg.as_ref());

    let mut adapter = launch_guest_harness(
        &harness_name,
        &program,
        model.as_deref(),
        session.as_deref(),
        &extra_args,
        &state_dir,
        &work_dir,
        &env,
        file_cfg.as_ref(),
    )
    .map_err(|err| format!("failed to start guest harness `{harness_name}`: {err}"))?;

    let wakeup = Arc::new((Mutex::new(false), Condvar::new()));
    {
        let w = Arc::clone(&wakeup);
        adapter.watch_activity(Arc::new(move || signal_wake(&w)));
    }

    let initial_health = adapter.health();
    let initial_sid = adapter
        .session_id()
        .or(session)
        .unwrap_or_else(|| format!("{harness_name}-guest-session"));
    emit_line(
        writer,
        &json!({
            "type": "system",
            "subtype": "init",
            "peer": "anyagents-guest/1.0",
            "claude_code_version": "guest-1.0",
            "session_id": initial_sid,
            "delivery": initial_health.delivery,
        }),
    )?;

    let (tx, rx) = mpsc::channel::<StdinEvent>();
    let input_wake = Arc::clone(&wakeup);
    std::thread::Builder::new()
        .name("anyagents-guest-stdin".into())
        .spawn(move || {
            let mut reader = reader;
            let mut line = String::new();
            let mut seq = 0u64;
            while let Ok(n) = reader.read_line(&mut line) {
                if n == 0 {
                    break;
                }
                let trimmed = line.trim();
                if !trimmed.is_empty()
                    && let Ok(val) = serde_json::from_str::<Value>(trimmed)
                {
                    match val.get("type").and_then(Value::as_str) {
                        Some("user") => {
                            seq += 1;
                            let msg_text = extract_user_text(&val);
                            let (id, body) = split_marker_or_uuid(&val, &msg_text, seq);
                            let uuid = val
                                .get("uuid")
                                .and_then(Value::as_str)
                                .map(str::to_string)
                                .unwrap_or_else(|| id.clone());
                            let raw_message = val.get("message").cloned().unwrap_or_else(|| {
                                json!({
                                    "role": "user",
                                    "content": [{ "type": "text", "text": msg_text }]
                                })
                            });
                            let msg = OutboundMessage {
                                id,
                                seq: seq as i64,
                                text: body,
                                sender: None,
                                reply_to: None,
                                source_time_ms: None,
                                context: Context::default(),
                            };
                            let _ = tx.send(StdinEvent::User {
                                msg,
                                uuid,
                                raw_message,
                            });
                            signal_wake(&input_wake);
                        }
                        Some("control_request")
                            if val.pointer("/request/subtype").and_then(Value::as_str)
                                == Some("interrupt") =>
                        {
                            let req_id = val
                                .get("request_id")
                                .and_then(Value::as_str)
                                .map(str::to_string);
                            let _ = tx.send(StdinEvent::Interrupt { request_id: req_id });
                            signal_wake(&input_wake);
                        }
                        _ => {}
                    }
                }
                line.clear();
            }
            let _ = tx.send(StdinEvent::Eof);
            signal_wake(&input_wake);
        })
        .map_err(|e| e.to_string())?;

    let mut queued: VecDeque<(OutboundMessage, String, Value)> = VecDeque::new();
    let mut inflight: VecDeque<PendingTurn> = VecDeque::new();
    let mut eof = false;
    let mut last_heartbeat = Instant::now();
    let mut last_activity_sync = Instant::now() - Duration::from_secs(10);

    let initial_snap = GuestActivitySnapshot::capture_current(
        &harness_name,
        Some(initial_sid.clone()),
        turn_state_label(initial_health.turn_state),
        0,
        0,
    );
    let _ = write_activity_snapshot(&state_dir, &initial_snap);

    loop {
        while let Ok(ev) = rx.try_recv() {
            match ev {
                StdinEvent::User {
                    msg,
                    uuid,
                    raw_message,
                } => queued.push_back((msg, uuid, raw_message)),
                StdinEvent::Interrupt { request_id } => {
                    let _ = adapter.cancel();
                    // Underlying stream harnesses (e.g. Claude/Stream) clear open turn tracking on
                    // cancel, so settle any still-inflight turns now instead of waiting forever.
                    while let Some(turn) = inflight.pop_front() {
                        let proven = adapter.proven(&turn.id);
                        let done = matches!(
                            proven,
                            Some(Proof::Taken {
                                turn_finished: true
                            }) | Some(Proof::Duplicate)
                        );
                        let err_override = (!done).then(|| "interrupted".to_string());
                        emit_turn_completion(
                            adapter.as_ref(),
                            &state_dir,
                            &harness_name,
                            &turn,
                            err_override,
                            writer,
                        )?;
                    }
                    if let Some(rid) = request_id {
                        emit_line(
                            writer,
                            &json!({
                                "type": "control_response",
                                "response": {
                                    "subtype": "success",
                                    "request_id": rid,
                                }
                            }),
                        )?;
                    }
                }
                StdinEvent::Eof => eof = true,
            }
        }

        // Offer queued inputs (supports immediate mid-turn steer when underlying harness admits).
        let mut remaining = VecDeque::new();
        while let Some((msg, uuid, raw_message)) = queued.pop_front() {
            let prev_usage = adapter.served_usage();
            let prev_turns = adapter.turns_served();
            match adapter.deliver(&msg) {
                Ok(DeliverOutcome::Accepted { turn_finished }) => {
                    emit_line(
                        writer,
                        &json!({
                            "type": "user",
                            "isReplay": true,
                            "uuid": uuid,
                            "message": raw_message,
                        }),
                    )?;
                    let mut turn = PendingTurn {
                        id: msg.id,
                        uuid,
                        delivered: true,
                        base_usage: prev_usage,
                        base_turns: prev_turns,
                    };
                    if turn_finished {
                        emit_turn_completion(
                            adapter.as_ref(),
                            &state_dir,
                            &harness_name,
                            &turn,
                            None,
                            writer,
                        )?;
                    } else {
                        turn.delivered = true;
                        inflight.push_back(turn);
                    }
                }
                Ok(DeliverOutcome::Duplicate) => {
                    emit_line(
                        writer,
                        &json!({
                            "type": "user",
                            "isReplay": true,
                            "duplicate": true,
                            "uuid": uuid,
                            "message": raw_message,
                        }),
                    )?;
                }
                Ok(DeliverOutcome::InFlight) => {
                    emit_line(
                        writer,
                        &json!({
                            "type": "user",
                            "isReplay": true,
                            "uuid": uuid,
                            "message": raw_message,
                        }),
                    )?;
                    inflight.push_back(PendingTurn {
                        id: msg.id,
                        uuid,
                        delivered: true,
                        base_usage: prev_usage,
                        base_turns: prev_turns,
                    });
                }
                Ok(DeliverOutcome::Deferred) => {
                    remaining.push_back((msg, uuid, raw_message));
                }
                Ok(DeliverOutcome::Quota { reset_at_ms }) => {
                    emit_line(
                        writer,
                        &json!({
                            "type": "result",
                            "subtype": "error_during_execution",
                            "is_error": true,
                            "session_id": adapter.session_id().unwrap_or_else(|| initial_sid.clone()),
                            "user_message_uuids": [uuid],
                            "quota_reset_at_ms": reset_at_ms,
                            "result": format!("quota exhausted (resets at {reset_at_ms:?})"),
                        }),
                    )?;
                }
                Ok(DeliverOutcome::Rejected { reason, .. }) => {
                    emit_line(
                        writer,
                        &json!({
                            "type": "result",
                            "subtype": "error_during_execution",
                            "is_error": true,
                            "session_id": adapter.session_id().unwrap_or_else(|| initial_sid.clone()),
                            "user_message_uuids": [uuid],
                            "result": reason,
                        }),
                    )?;
                }
                Err(HarnessError::Dead) => {
                    let reason = adapter
                        .exit_diagnostic()
                        .unwrap_or_else(|| "guest harness exited".into());
                    emit_line(
                        writer,
                        &json!({
                            "type": "result",
                            "subtype": "error_during_execution",
                            "is_error": true,
                            "session_id": adapter.session_id().unwrap_or_else(|| initial_sid.clone()),
                            "user_message_uuids": [uuid],
                            "result": reason,
                        }),
                    )?;
                    break;
                }
                Err(err) => {
                    emit_line(
                        writer,
                        &json!({
                            "type": "result",
                            "subtype": "error_during_execution",
                            "is_error": true,
                            "session_id": adapter.session_id().unwrap_or_else(|| initial_sid.clone()),
                            "user_message_uuids": [uuid],
                            "result": err.to_string(),
                        }),
                    )?;
                }
            }
        }
        queued = remaining;

        // Check completed turns and health.
        let health = adapter.health();
        if let Some(sid) = adapter.session_id() {
            let _ = persist_session(&state_dir, &harness_name, &sid);
        }

        let mut still_inflight = VecDeque::new();
        while let Some(turn) = inflight.pop_front() {
            let proven = adapter.proven(&turn.id);
            let done_by_proof = matches!(
                proven,
                Some(Proof::Taken {
                    turn_finished: true
                }) | Some(Proof::Duplicate)
            );
            let done_by_turns =
                adapter.turns_served() > turn.base_turns && health.turn_state == TurnState::Idle;
            let done_by_failure = adapter.failure_for_id(&turn.id).is_some();
            if done_by_proof || done_by_turns || done_by_failure || !health.process_alive {
                let err_override = adapter.failure_for_id(&turn.id).or_else(|| {
                    (!health.process_alive && !done_by_proof && !done_by_turns).then(|| {
                        adapter
                            .exit_diagnostic()
                            .unwrap_or_else(|| "guest harness process exited mid-turn".into())
                    })
                });
                emit_turn_completion(
                    adapter.as_ref(),
                    &state_dir,
                    &harness_name,
                    &turn,
                    err_override,
                    writer,
                )?;
            } else {
                still_inflight.push_back(turn);
            }
        }
        inflight = still_inflight;

        if (eof && queued.is_empty() && inflight.is_empty())
            || (!health.process_alive && inflight.is_empty() && queued.is_empty())
        {
            break;
        }

        let heartbeat_due = heartbeat_interval.is_some_and(|iv| last_heartbeat.elapsed() >= iv);
        if heartbeat_due || last_activity_sync.elapsed() >= Duration::from_secs(1) {
            let snap = GuestActivitySnapshot::capture_current(
                &harness_name,
                adapter.session_id().or_else(|| Some(initial_sid.clone())),
                turn_state_label(health.turn_state),
                inflight.len(),
                queued.len(),
            );
            let _ = write_activity_snapshot(&state_dir, &snap);
            last_activity_sync = Instant::now();
            if heartbeat_due {
                emit_line(writer, &snap.to_heartbeat_event())?;
                last_heartbeat = Instant::now();
            }
        }

        let wait_dur = match heartbeat_interval {
            Some(iv) => IDLE_TICK.min(iv.max(Duration::from_millis(10))),
            None => IDLE_TICK,
        };
        wait_wake(&wakeup, wait_dur);
    }

    clear_activity_snapshot(&state_dir);
    adapter.stop();
    if let Some(sid) = adapter.session_id() {
        let _ = persist_session(&state_dir, &harness_name, &sid);
    }
    Ok(())
}

pub(crate) fn turn_state_label(state: TurnState) -> &'static str {
    match state {
        TurnState::Idle => "idle",
        TurnState::InTurn => "in_turn",
    }
}

fn emit_turn_completion<W: Write>(
    adapter: &dyn GuestHarness,
    state_dir: &Path,
    harness_name: &str,
    turn: &PendingTurn,
    err_override: Option<String>,
    writer: &mut W,
) -> Result<(), String> {
    let sid = adapter
        .session_id()
        .unwrap_or_else(|| format!("{harness_name}-guest-session"));
    let _ = persist_session(state_dir, harness_name, &sid);
    let usage = adapter
        .usage_for_id(&turn.id)
        .unwrap_or_else(|| adapter.served_usage().difference(turn.base_usage.clone()));
    let cost_usd = usage.reported_cost_micros.map(|m| (m as f64) / 1_000_000.0);

    if let Some(err_msg) = err_override {
        return emit_line(
            writer,
            &json!({
                "type": "result",
                "subtype": "error_during_execution",
                "is_error": true,
                "session_id": sid,
                "user_message_uuids": [turn.uuid],
                "result": err_msg,
                "usage": usage_json(&usage),
                "total_cost_usd": cost_usd,
            }),
        );
    }

    let reply = adapter
        .answer_for(&turn.id, state_dir)
        .filter(|s| !s.trim().is_empty());

    if let Some(ref reply_text) = reply {
        emit_line(
            writer,
            &json!({
                "type": "assistant",
                "session_id": sid,
                "message": {
                    "role": "assistant",
                    "content": [{ "type": "text", "text": reply_text }]
                }
            }),
        )?;
    }

    emit_line(
        writer,
        &json!({
            "type": "result",
            "subtype": "success",
            "is_error": false,
            "session_id": sid,
            "user_message_uuids": [turn.uuid],
            "result": reply.unwrap_or_default(),
            "usage": usage_json(&usage),
            "total_cost_usd": cost_usd,
        }),
    )
}

pub(crate) fn usage_json(usage: &TurnUsage) -> Value {
    json!({
        "input_tokens": usage.input_tokens,
        "output_tokens": usage.output_tokens,
        "cache_read_input_tokens": usage.cache_read_tokens,
        "cache_creation_input_tokens": usage.cache_write_tokens,
        "reported_cost_micros": usage.reported_cost_micros,
    })
}

fn emit_line<W: Write>(writer: &mut W, value: &Value) -> Result<(), String> {
    let serialized = serde_json::to_string(value).map_err(|e| e.to_string())?;
    writeln!(writer, "{serialized}").map_err(|e| format!("stdout write failed: {e}"))?;
    writer
        .flush()
        .map_err(|e| format!("stdout flush failed: {e}"))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn launch_guest_harness(
    harness_name: &str,
    program: &Path,
    model: Option<&str>,
    session: Option<&str>,
    extra_args: &[String],
    state_dir: &Path,
    work_dir: &Path,
    env: &[(String, String)],
    file_cfg: Option<&GuestConfig>,
) -> Result<Box<dyn GuestHarness>, HarnessError> {
    let pairs: Vec<(&str, &str)> = env.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    match harness_name {
        "claude-code" => {
            let mut full_args = extra_args.to_vec();
            let bare = extra_args.iter().any(|a| a == "--guest-bare");
            let mcp_cfg = state_dir.join(".mcp.json");
            if mcp_cfg.is_file() && !full_args.iter().any(|a| a == "--mcp-config") {
                full_args.push("--mcp-config".to_string());
                full_args.push(mcp_cfg.display().to_string());
            }
            if !bare
                && let Some(m) = model.filter(|s| !s.trim().is_empty())
                && !full_args.iter().any(|a| a == "--model")
            {
                full_args.push("--model".to_string());
                full_args.push(m.to_string());
            }
            if !bare
                && !full_args
                    .iter()
                    .any(|a| a == "--dangerously-skip-permissions")
            {
                full_args.insert(0, "--dangerously-skip-permissions".to_string());
            }
            full_args.retain(|a| a != "--guest-bare");
            let h = ClaudeHarness::start_in_with_args(
                program,
                &full_args,
                session,
                &pairs,
                Some(work_dir),
            )?;
            Ok(Box::new(ClaudeAdapter(h)))
        }
        "codex" => {
            let h = CodexHarness::start_in(program, session, &pairs, Some(work_dir))?;
            Ok(Box::new(CodexAdapter(h)))
        }
        "opencode" => {
            let h = OpenCodeHarness::start_in_config(
                program,
                session,
                &pairs,
                Some(work_dir),
                state_dir,
            )?;
            Ok(Box::new(OpenCodeAdapter(h)))
        }
        "agy" => {
            let spec = build_agy_stream_spec(model, extra_args);
            let h = AgyHarness::start_with_spec(spec, program, session, &pairs, Some(work_dir))?;
            Ok(Box::new(AgyAdapter(h)))
        }
        "muse" => {
            let h = MuseHarness::start_in_with_args(
                program,
                extra_args,
                session,
                &pairs,
                Some(work_dir),
            )?;
            Ok(Box::new(MuseAdapter(h)))
        }
        "omp" => {
            let mut spec = StreamSpec::for_program("omp");
            if let Some(m) = model.filter(|s| !s.trim().is_empty()) {
                spec.args.extend(["--model".to_string(), m.to_string()]);
            }
            let sys_prompt = state_dir.join("SYSTEM_PROMPT.md");
            if sys_prompt.is_file() {
                spec.args.extend([
                    "--append-system-prompt".to_string(),
                    sys_prompt.display().to_string(),
                ]);
            }
            spec.args.extend(extra_args.iter().cloned());
            let h = OmpHarness::start_with_spec(spec, program, session, &pairs, Some(work_dir))?;
            Ok(Box::new(OmpAdapter(h)))
        }
        "spiron" => {
            let mcp_config = state_dir.join("spiron-mcp.json");
            let skills = state_dir.join(".agents/skills");
            let mut args = vec![
                "--headless".to_string(),
                "--json".to_string(),
                "--workdir".to_string(),
                work_dir.display().to_string(),
                "--state-dir".to_string(),
                state_dir.display().to_string(),
            ];
            if mcp_config.is_file() {
                args.extend(["--mcp-config".to_string(), mcp_config.display().to_string()]);
            }
            if skills.is_dir() {
                args.extend(["--skills-dir".to_string(), skills.display().to_string()]);
            }
            let sys_prompt = state_dir.join("SYSTEM_PROMPT.md");
            if sys_prompt.is_file() {
                args.extend([
                    "--instructions-file".to_string(),
                    sys_prompt.display().to_string(),
                ]);
            }
            if let Some(id) = file_cfg.and_then(|c| c.agent_id.as_deref()) {
                args.extend(["--agent-id".to_string(), id.to_string()]);
            }
            if let Some(name) = file_cfg.and_then(|c| c.agent_name.as_deref()) {
                args.extend(["--agent-name".to_string(), name.to_string()]);
            }
            if let Some(m) = model.filter(|s| !s.trim().is_empty()) {
                args.extend(["--model".to_string(), m.to_string()]);
            }
            args.extend(extra_args.iter().cloned());
            let h = SpironHarness::launch_listening(
                SpironLaunch {
                    program: program.to_path_buf(),
                    args,
                    environment: env.to_vec(),
                    work_dir: work_dir.to_path_buf(),
                    session: session.map(str::to_string),
                    state_dir: state_dir.to_path_buf(),
                },
                &state_dir.join("spiron.sock"),
                &state_dir.join("spiron.log"),
                Duration::from_secs(60),
                |_pid| Ok(()),
            )
            .map_err(|anyagents_harness_spiron::ListenError::Failed(msg)| {
                HarnessError::Failed(msg)
            })?;
            Ok(Box::new(SpironAdapter(h)))
        }
        "gemini" => {
            let h =
                GeminiGuestHarness::start(program, model, session, extra_args, &pairs, work_dir)?;
            Ok(Box::new(GeminiAdapter(h)))
        }
        other => Err(HarnessError::Failed(format!(
            "unsupported guest harness `{other}`"
        ))),
    }
}

pub(crate) fn resolve_guest_program(harness: &str) -> PathBuf {
    let canonical = canonical_guest_harness(harness);
    let bin_name = match canonical.as_str() {
        "claude-code" => "claude",
        "codex" => "codex",
        "opencode" => "opencode",
        "gemini" => "gemini",
        "agy" => "agy",
        "muse" => "muse",
        "omp" => "omp",
        "spiron" | "spiron-local" => "spiron",
        other => other,
    };
    if let Ok(resolved) = anyagents_toolchain::tool(bin_name, "anyagents-guest.serve") {
        return resolved.path;
    }
    for dir in ["/usr/local/bin", "/usr/bin", "/home/user/.local/bin"] {
        let candidate = Path::new(dir).join(bin_name);
        if candidate.is_file() {
            return candidate;
        }
    }
    PathBuf::from(format!("/usr/local/bin/{bin_name}"))
}

fn build_agy_stream_spec(model: Option<&str>, extra_args: &[String]) -> StreamSpec {
    let mut spec = StreamSpec::for_program("agy");
    let bare = extra_args.iter().any(|a| a == "--guest-bare");
    if !bare {
        spec.args = vec![
            "--input-format".to_string(),
            "stream-json".to_string(),
            "--output-format".to_string(),
            "stream-json".to_string(),
            "--dangerously-skip-permissions".to_string(),
        ];
        if let Some(m) = model.filter(|s| !s.trim().is_empty())
            && !extra_args.iter().any(|a| a == "--model")
        {
            spec.args.push("--model".to_string());
            spec.args.push(m.to_string());
        }
        spec.resume_flag = "--conversation";
    }
    for arg in extra_args {
        if arg != "--guest-bare" {
            spec.args.push(arg.clone());
        }
    }
    spec
}

pub(crate) fn build_guest_env(
    state_dir: &Path,
    work_dir: &Path,
    file_cfg: Option<&GuestConfig>,
) -> Vec<(String, String)> {
    let mut env = Vec::new();
    if let Some(cfg) = file_cfg {
        env.extend(cfg.env.iter().cloned());
    }
    let state_str = state_dir.display().to_string();
    let data_home = state_dir.join("data-home");
    let state_home = state_dir.join("state-home");
    let omp_dir = state_dir.join(".omp/agent");
    for d in [&data_home, &state_home, &omp_dir] {
        let _ = std::fs::create_dir_all(d);
    }
    for (k, v) in [
        ("HOME", state_str.as_str()),
        ("CLAUDE_CONFIG_DIR", state_str.as_str()),
        ("CODEX_HOME", state_str.as_str()),
        ("XDG_CONFIG_HOME", state_str.as_str()),
        ("XDG_DATA_HOME", &data_home.display().to_string()),
        ("XDG_STATE_HOME", &state_home.display().to_string()),
        ("PI_CODING_AGENT_DIR", &omp_dir.display().to_string()),
        ("SPIRON_STATE_DIR", state_str.as_str()),
        ("SPIRON_WORKSPACE", &work_dir.display().to_string()),
        ("ANYAGENTS_GUEST_STATE_DIR", state_str.as_str()),
        ("ANYAGENTS_GUEST_WORKDIR", &work_dir.display().to_string()),
        ("MUSE_NO_AUTO_UPDATE", "1"),
    ] {
        env.retain(|(ek, _)| ek != k);
        env.push((k.to_string(), v.to_string()));
    }
    repo::ensure_gh_auth_setup_git_for_env(state_dir, &env);
    env
}

pub(crate) fn load_guest_config(state_dir: &Path) -> Option<GuestConfig> {
    let raw = std::fs::read_to_string(state_dir.join(GUEST_CONFIG_FILE)).ok()?;
    serde_json::from_str(&raw).ok()
}

#[path = "repo.rs"]
mod repo;
pub(crate) use repo::{ensure_repo_checked_out, load_repo_spec};

pub(crate) fn read_persisted_session(state_dir: &Path, harness: &str) -> Option<String> {
    let raw = std::fs::read_to_string(state_dir.join(format!("{harness}-session-id"))).ok()?;
    let trimmed = raw.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

pub(crate) fn persist_session(
    state_dir: &Path,
    harness: &str,
    session_id: &str,
) -> Result<(), String> {
    let trimmed = session_id.trim();
    if trimmed.is_empty() {
        return Ok(());
    }
    let path = state_dir.join(format!("{harness}-session-id"));
    anyagents_harness::atomic_write_nonempty_file(&path, format!("{trimmed}\n").as_bytes(), 0o600)
        .map_err(|e| e.to_string())
}

fn read_claude_transcript_reply(state_dir: &Path, session_id: Option<&str>) -> Option<String> {
    let sid = session_id?;
    let projects_dir = state_dir.join("projects");
    let entries = std::fs::read_dir(&projects_dir).ok()?;
    for entry in entries.flatten() {
        let candidate = entry.path().join(format!("{sid}.jsonl"));
        if !candidate.is_file() {
            continue;
        }
        let Ok(raw) = std::fs::read_to_string(&candidate) else {
            continue;
        };
        let mut last_text: Option<String> = None;
        for line in raw.lines() {
            let Ok(val) = serde_json::from_str::<Value>(line) else {
                continue;
            };
            if val.get("type").and_then(Value::as_str) == Some("assistant")
                && let Some(arr) = val.pointer("/message/content").and_then(Value::as_array)
            {
                let txt: String = arr
                    .iter()
                    .filter_map(|item| item.get("text").and_then(Value::as_str))
                    .collect();
                if !txt.trim().is_empty() {
                    last_text = Some(txt);
                }
            }
        }
        if last_text.is_some() {
            return last_text;
        }
    }
    None
}

fn extract_user_text(val: &Value) -> String {
    if let Some(s) = val.pointer("/message/content").and_then(Value::as_str) {
        return s.to_string();
    }
    if let Some(arr) = val.pointer("/message/content").and_then(Value::as_array) {
        let mut out = String::new();
        for item in arr {
            if item.get("type").and_then(Value::as_str) == Some("text")
                && let Some(t) = item.get("text").and_then(Value::as_str)
            {
                if !out.is_empty() {
                    out.push('\n');
                }
                out.push_str(t);
            }
        }
        return out;
    }
    val.get("content")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

fn split_marker_or_uuid(val: &Value, raw_text: &str, seq: u64) -> (String, String) {
    if let Some(first_line) = raw_text.lines().next()
        && let Some(id) = first_line.strip_prefix(anyagents_harness::MESSAGE_MARKER_PREFIX)
        && !id.trim().is_empty()
    {
        let body = raw_text[first_line.len()..]
            .strip_prefix('\n')
            .unwrap_or("");
        return (id.trim().to_string(), body.to_string());
    }
    let uuid = val
        .get("uuid")
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| format!("guest-msg-{seq}"));
    (uuid, raw_text.to_string())
}

pub(crate) fn signal_wake(wakeup: &(Mutex<bool>, Condvar)) {
    let (lock, cv) = wakeup;
    if let Ok(mut dirty) = lock.lock() {
        *dirty = true;
    }
    cv.notify_all();
}

pub(crate) fn wait_wake(wakeup: &(Mutex<bool>, Condvar), timeout: Duration) {
    let (lock, cv) = wakeup;
    let guard = lock.lock().unwrap_or_else(|e| e.into_inner());
    let (mut guard, _) = cv
        .wait_timeout_while(guard, timeout, |dirty| !*dirty)
        .unwrap_or_else(|e| e.into_inner());
    *guard = false;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_fake_claude_script(dir: &Path) -> PathBuf {
        let script = dir.join("fake-claude");
        std::fs::write(
            &script,
            r#"#!/bin/sh
echo '{"type":"system","subtype":"init","session_id":"guest-sess-42","claude_code_version":"1.0","peer":"claude-code 1.0"}'
while IFS= read -r line; do
  case "$line" in
    *'"type":"user"'*)
      uuid=$(printf '%s\n' "$line" | sed -n 's/.*"uuid":"\([^"]*\)".*/\1/p')
      printf '{"type":"user","isReplay":true,"uuid":"%s","message":{"role":"user","content":[{"type":"text","text":"echo"}]}}\n' "$uuid"
      printf '{"type":"assistant","session_id":"guest-sess-42","message":{"role":"assistant","content":[{"type":"text","text":"guest reply"}]}}\n'
      printf '{"type":"result","subtype":"success","is_error":false,"session_id":"guest-sess-42","user_message_uuids":["%s"],"result":"guest reply","usage":{"input_tokens":11,"output_tokens":7},"total_cost_usd":0.01}\n' "$uuid"
      ;;
  esac
done
"#,
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        script
    }

    #[test]
    fn guest_serve_persists_session_usage_and_streams_claude_turns() {
        let tmp = tempfile::tempdir().unwrap();
        let state_dir = tmp.path().join("state");
        let work_dir = tmp.path().join("work");
        std::fs::create_dir_all(&state_dir).unwrap();
        std::fs::create_dir_all(&work_dir).unwrap();
        let script = write_fake_claude_script(tmp.path());

        let args = ServeArgs {
            harness: Some("claude-code".into()),
            program: Some(script),
            state_dir: Some(state_dir.clone()),
            workdir: Some(work_dir),
            model: None,
            session: None,
            git_program: None,
            heartbeat_interval_ms: None,
            extra_args: vec!["--guest-bare".into()],
        };
        let (line_tx, line_rx) = mpsc::channel::<String>();
        struct ChannelReader {
            rx: mpsc::Receiver<String>,
            buf: Vec<u8>,
            pos: usize,
        }
        impl std::io::Read for ChannelReader {
            fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
                while self.pos >= self.buf.len() {
                    match self.rx.recv() {
                        Ok(next) => {
                            self.buf = next.into_bytes();
                            self.pos = 0;
                        }
                        Err(_) => return Ok(0),
                    }
                }
                let n = (self.buf.len() - self.pos).min(out.len());
                out[..n].copy_from_slice(&self.buf[self.pos..self.pos + n]);
                self.pos += n;
                Ok(n)
            }
        }
        struct NotifyingWriter {
            buf: Vec<u8>,
            on_result: Option<mpsc::Sender<()>>,
        }
        impl Write for NotifyingWriter {
            fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
                self.buf.extend_from_slice(data);
                if let Some(tx) = &self.on_result
                    && String::from_utf8_lossy(&self.buf).contains("\"type\":\"result\"")
                {
                    let _ = tx.send(());
                    self.on_result = None;
                }
                Ok(data.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let (done_tx, done_rx) = mpsc::channel::<()>();
        let feeder = std::thread::spawn(move || {
            line_tx
                .send("{\"type\":\"user\",\"uuid\":\"u-1\",\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"hello guest\"}]}}\n".into())
                .unwrap();
            let _ = done_rx.recv_timeout(Duration::from_secs(10));
            let _ = line_tx.send(
                "{\"type\":\"control_request\",\"request_id\":\"req-1\",\"request\":{\"subtype\":\"interrupt\"}}\n".into(),
            );
        });
        let reader = BufReader::new(ChannelReader {
            rx: line_rx,
            buf: Vec::new(),
            pos: 0,
        });
        let mut out = NotifyingWriter {
            buf: Vec::new(),
            on_result: Some(done_tx),
        };
        run_serve_io(args, reader, &mut out).unwrap();
        feeder.join().unwrap();

        let text = String::from_utf8(out.buf).unwrap();
        assert!(text.contains("\"type\":\"system\""), "missing init: {text}");
        assert!(text.contains("\"isReplay\":true"), "missing replay: {text}");
        assert!(
            text.contains("\"type\":\"result\""),
            "missing result: {text}"
        );
        assert!(
            text.contains("\"input_tokens\":11"),
            "missing usage: {text}"
        );
        assert!(
            text.contains("\"type\":\"control_response\""),
            "missing cancel ack: {text}"
        );
        let saved_sid = std::fs::read_to_string(state_dir.join("claude-code-session-id")).unwrap();
        assert_eq!(saved_sid.trim(), "guest-sess-42");
    }

    #[test]
    fn guest_delivery_mapping_preserves_mid_turn_capabilities() {
        let delivery = Delivery {
            level: anyagents_harness::DeliveryLevel::MidTurn,
            idempotent: false,
            proof: std::borrow::Cow::Borrowed("ok"),
            peer: Some("opencode 1.0".into()),
        };
        let encoded = serde_json::to_string(&delivery).unwrap();
        let decoded: Delivery = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded.level, anyagents_harness::DeliveryLevel::MidTurn);
        assert_eq!(decoded.peer.as_deref(), Some("opencode 1.0"));
    }

    #[test]
    fn guest_default_paths_align_to_agent_data_and_respect_env_overrides() {
        static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let keys = [
            "ANYAGENTS_GUEST_STATE_DIR",
            "ANYAGENTS_SANDBOX_STATE_DIR",
            "ANYAGENTS_GUEST_WORKDIR",
            "ANYAGENTS_SANDBOX_WORKDIR",
            "ANYAGENTS_SANDBOX_DATA_DIR",
        ];
        let saved: Vec<(&str, Option<String>)> =
            keys.iter().map(|k| (*k, std::env::var(k).ok())).collect();
        for k in keys {
            unsafe { std::env::remove_var(k) };
        }

        assert_eq!(
            resolve_default_state_dir(None),
            PathBuf::from("/home/user/agent-data/state")
        );
        assert_eq!(
            resolve_default_workdir(None),
            PathBuf::from("/home/user/agent-data/workspace")
        );

        unsafe { std::env::set_var("ANYAGENTS_SANDBOX_DATA_DIR", "/mnt/persistent") };
        assert_eq!(
            resolve_default_state_dir(None),
            PathBuf::from("/mnt/persistent/state")
        );
        assert_eq!(
            resolve_default_workdir(None),
            PathBuf::from("/mnt/persistent/workspace")
        );

        unsafe {
            std::env::set_var("ANYAGENTS_GUEST_STATE_DIR", "/custom/state");
            std::env::set_var("ANYAGENTS_GUEST_WORKDIR", "/custom/work");
        }
        assert_eq!(
            resolve_default_state_dir(None),
            PathBuf::from("/custom/state")
        );
        assert_eq!(resolve_default_workdir(None), PathBuf::from("/custom/work"));

        for (k, val) in saved {
            match val {
                Some(v) => unsafe { std::env::set_var(k, v) },
                None => unsafe { std::env::remove_var(k) },
            }
        }
    }

    #[test]
    fn guest_serve_emits_heartbeat_and_reports_live_inflight_children() {
        let tmp = tempfile::tempdir().unwrap();
        let state_dir = tmp.path().join("state");
        let work_dir = tmp.path().join("workspace");
        std::fs::create_dir_all(&state_dir).unwrap();
        std::fs::create_dir_all(&work_dir).unwrap();

        let script = tmp.path().join("fake-claude-with-bg-job");
        std::fs::write(
            &script,
            r#"#!/bin/sh
echo '{"type":"system","subtype":"init","session_id":"hb-sess-1","claude_code_version":"1.0","peer":"claude-code 1.0"}'
sleep 30 &
bg_pid=$!
trap 'kill "$bg_pid" 2>/dev/null || true; exit 0' INT TERM EXIT
while IFS= read -r line; do
  :
done
kill "$bg_pid" 2>/dev/null || true
"#,
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        }

        let (line_tx, line_rx) = mpsc::channel::<String>();
        struct ChannelReader {
            rx: mpsc::Receiver<String>,
            buf: Vec<u8>,
            pos: usize,
        }
        impl std::io::Read for ChannelReader {
            fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
                while self.pos >= self.buf.len() {
                    match self.rx.recv() {
                        Ok(next) => {
                            self.buf = next.into_bytes();
                            self.pos = 0;
                        }
                        Err(_) => return Ok(0),
                    }
                }
                let n = (self.buf.len() - self.pos).min(out.len());
                out[..n].copy_from_slice(&self.buf[self.pos..self.pos + n]);
                self.pos += n;
                Ok(n)
            }
        }

        let (hb_tx, hb_rx) = mpsc::channel::<Value>();
        struct HeartbeatCapturingWriter {
            buf: Vec<u8>,
            pending_line: Vec<u8>,
            hb_tx: mpsc::Sender<Value>,
        }
        impl Write for HeartbeatCapturingWriter {
            fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
                self.buf.extend_from_slice(data);
                self.pending_line.extend_from_slice(data);
                while let Some(pos) = self.pending_line.iter().position(|&b| b == b'\n') {
                    let line: Vec<u8> = self.pending_line.drain(..=pos).collect();
                    if let Ok(val) = serde_json::from_slice::<Value>(&line)
                        && val.get("subtype").and_then(Value::as_str) == Some("heartbeat")
                    {
                        let _ = self.hb_tx.send(val);
                    }
                }
                Ok(data.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let state_probe = state_dir.clone();
        let work_probe = work_dir.clone();
        let stopper = std::thread::spawn(move || {
            let mut saw_busy_hb = None;
            let deadline = Instant::now() + Duration::from_secs(5);
            while Instant::now() < deadline {
                let rem = deadline.saturating_duration_since(Instant::now());
                match hb_rx.recv_timeout(rem) {
                    Ok(ev) => {
                        #[cfg(target_os = "linux")]
                        let ready = ev["inflight_children"].as_u64().unwrap_or(0) >= 1;
                        #[cfg(not(target_os = "linux"))]
                        let ready = true;
                        if ready {
                            let health =
                                crate::activity::build_health_payload(&state_probe, &work_probe);
                            saw_busy_hb = Some((ev, health));
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
            drop(line_tx);
            saw_busy_hb
        });

        let args = ServeArgs {
            harness: Some("claude-code".into()),
            program: Some(script),
            state_dir: Some(state_dir.clone()),
            workdir: Some(work_dir),
            model: None,
            session: None,
            git_program: None,
            heartbeat_interval_ms: Some(40),
            extra_args: vec!["--guest-bare".into()],
        };
        let reader = BufReader::new(ChannelReader {
            rx: line_rx,
            buf: Vec::new(),
            pos: 0,
        });
        let mut out = HeartbeatCapturingWriter {
            buf: Vec::new(),
            pending_line: Vec::new(),
            hb_tx,
        };
        run_serve_io(args, reader, &mut out).unwrap();
        let (hb_ev, health_ev) = stopper
            .join()
            .unwrap()
            .expect("should observe heartbeat with live background child");

        assert_eq!(hb_ev["type"], "system");
        assert_eq!(hb_ev["subtype"], "heartbeat");
        assert_eq!(hb_ev["harness"], "claude-code");
        #[cfg(target_os = "linux")]
        {
            assert!(
                hb_ev["inflight_children"].as_u64().unwrap_or(0) >= 1,
                "expected inflight_children >= 1 in heartbeat: {hb_ev}"
            );
            assert_eq!(
                hb_ev["busy"], true,
                "expected busy=true in heartbeat: {hb_ev}"
            );
            assert!(
                health_ev["inflight_children"].as_u64().unwrap_or(0) >= 1,
                "expected inflight_children >= 1 in health payload: {health_ev}"
            );
            assert_eq!(
                health_ev["busy"], true,
                "expected busy=true in health: {health_ev}"
            );
        }
        assert_eq!(health_ev["status"], "ok");
        assert_eq!(health_ev["binary"], "anyagents-guest");
        assert_eq!(health_ev["harnesses"], 9);
    }

    #[test]
    fn agy_launch_spec_uses_stream_json_and_conversation_resume_flag() {
        let spec = build_agy_stream_spec(Some("gemini-4-carbon-high"), &[]);
        assert_eq!(spec.resume_flag, "--conversation");
        assert_eq!(
            spec.args,
            vec![
                "--input-format",
                "stream-json",
                "--output-format",
                "stream-json",
                "--dangerously-skip-permissions",
                "--model",
                "gemini-4-carbon-high",
            ]
        );

        let bare =
            build_agy_stream_spec(Some("gemini-4-carbon-high"), &["--guest-bare".to_string()]);
        assert_eq!(bare.resume_flag, "--resume");
        assert_eq!(bare.args, vec!["--output-format", "stream-json"]);
    }
}
