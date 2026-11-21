//! Detached serverless push-callback turn runner (`anyagents-guest turn`).
//!
//! Enables pure scale-to-zero coordinators:
//! - With `--detached`, daemonizes a child worker (`setsid`, stdout/stderr redirected to
//!   `<state-dir>/turns/<turn-id>.log`), prints `{"status":"accepted","turn_id":"...","pid":N}`,
//!   and exits `0` immediately (<100ms) so the coordinator HTTP request closes immediately.
//! - The worker drives any of the supported CLI harnesses (`claude`, `codex`, `opencode`,
//!   `gemini`, `omp`, `agy`, `muse`, `spiron`) to completion, POSTing periodic
//!   heartbeats (with live `/proc` `inflight_children`), incremental `delta` events, and a final
//!   `turn_complete` or `turn_failed` JSON payload with `Authorization: Bearer <callback-token>`
//!   and bounded exponential-backoff retry (5 retries).

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[cfg(target_os = "linux")]
#[path = "turn_custody.rs"]
mod custody;

#[path = "turn_watchdog.rs"]
mod watchdog;
pub(crate) use watchdog::TurnWatchdog;
#[cfg(test)]
use watchdog::resolve_timeout_secs_from;
use watchdog::{STATE_DIR_PROGRESS_PROBE_INTERVAL, StateDirProgressTracker};

use anyagents_harness::{
    Context, DeliverOutcome, HarnessError, OutboundMessage, Proof, TurnState, TurnUsage,
};
use clap::Args;
use serde_json::{Value, json};

use crate::activity::{
    DEFAULT_HEARTBEAT_INTERVAL_MS, GuestActivitySnapshot, clear_activity_snapshot,
    write_activity_snapshot,
};
use crate::guest::{
    build_guest_env, canonical_guest_harness, ensure_repo_checked_out, launch_guest_harness,
    load_guest_config, load_repo_spec, persist_session, resolve_default_state_dir,
    resolve_default_workdir, resolve_guest_program, signal_wake, turn_state_label, usage_json,
    wait_wake,
};

const POLL_TICK: Duration = Duration::from_millis(25);
const DEFAULT_MAX_RETRIES: usize = 5;
const DEFAULT_INITIAL_BACKOFF_MS: u64 = 200;
const CALLBACK_TIMEOUT_SECS: u64 = 15;
const HEARTBEAT_TIMEOUT_SECS: u64 = 5;
const OPENCODE_DELTA_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Allowlisted native-harness OAuth credential files relative to `state_dir`.
///
/// When a native vendor CLI refreshes its own OAuth token inside a sandbox turn,
/// `anyagents-guest` detects modified files from this allowlist after `adapter.stop()`
/// and attaches them (base64-encoded) to the authenticated terminal callback so the
/// stateless coordinator can persist them back to durable storage under single-writer
/// custody without ever scraping binaries or spoofing vendor refresh protocols.
const SYNCABLE_AUTH_FILES: &[&str] = &[
    ".gemini/antigravity-cli/antigravity-oauth-token",
    ".claude/.credentials.json",
    ".credentials.json",
    ".codex/auth.json",
    "auth.json",
];
const MAX_SYNCABLE_AUTH_FILE_BYTES: usize = 64 * 1024;

/// CLI arguments for `anyagents-guest turn`.
#[derive(Args, Clone, Debug)]
pub struct TurnArgs {
    /// Daemonize into a detached background child, print accepted JSON, and exit immediately.
    #[arg(long)]
    pub detached: bool,
    /// Internal: wait for the detached launcher to register this worker.
    #[cfg(target_os = "linux")]
    #[arg(long, hide = true, conflicts_with = "detached")]
    pub detached_worker: bool,
    /// Stable turn identifier assigned by the coordinator (`pending_remote_turns.turn_id`).
    #[arg(long)]
    pub turn_id: String,
    /// Target AnyAgents agent identifier (`agt_...`).
    #[arg(long)]
    pub agent_id: String,
    /// Target CLI harness (`claude`, `codex`, `opencode`, `gemini`, `omp`, `agy`, `muse`, `spiron`).
    #[arg(long)]
    pub harness: Option<String>,
    /// Explicit path to the underlying vendor CLI binary inside the sandbox.
    #[arg(long)]
    pub program: Option<PathBuf>,
    /// Model identifier override.
    #[arg(long)]
    pub model: Option<String>,
    /// Existing session ID to resume.
    #[arg(long)]
    pub session: Option<String>,
    /// File containing the UTF-8 user prompt for this turn.
    #[arg(long)]
    pub prompt_file: Option<PathBuf>,
    /// File containing a JSON-serialized [`OutboundMessage`] envelope for this turn.
    #[arg(long)]
    pub message_file: Option<PathBuf>,
    /// Read the UTF-8 user prompt from standard input.
    #[arg(long)]
    pub prompt_stdin: bool,
    /// Inline UTF-8 user prompt text (alternative to `--prompt-file` / `--prompt-stdin`).
    #[arg(long)]
    pub prompt: Option<String>,
    /// Coordinator HTTP callback URL (`<PUBLIC_URL>/v1/internal/guest/events`).
    #[arg(long)]
    pub callback_url: String,
    /// Per-turn bearer token sent in `Authorization: Bearer <token>`.
    #[arg(long)]
    pub callback_token: Option<String>,
    /// File containing the per-turn bearer callback token (mode 0600).
    #[arg(long)]
    pub callback_token_file: Option<PathBuf>,
    /// Heartbeat interval in milliseconds (defaults to 5000ms; `0` sends only initial + terminal events).
    #[arg(long)]
    pub heartbeat_interval_ms: Option<u64>,
    /// Persistent agent state directory (defaults to `/home/user/agent-data/state`).
    #[arg(long)]
    pub state_dir: Option<PathBuf>,
    /// Persistent workspace directory (defaults to `/home/user/agent-data/workspace`).
    #[arg(long)]
    pub workdir: Option<PathBuf>,
    /// Explicit git binary path when repo checkout is needed.
    #[arg(long)]
    pub git_program: Option<PathBuf>,
    /// Trailing CLI flags forwarded to compatible adapters.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub extra_args: Vec<String>,
}

/// Entry point for `anyagents-guest turn`.
pub fn run_turn(args: TurnArgs) -> Result<(), String> {
    #[cfg(target_os = "linux")]
    if args.detached_worker {
        custody::wait_for_dispatch().map_err(|e| format!("cannot admit detached turn: {e}"))?;
    }
    validate_turn_id(&args.turn_id)?;
    let callback_url = args.callback_url.trim().to_string();
    if callback_url.is_empty()
        || (!callback_url.starts_with("https://") && !callback_url.starts_with("http://"))
    {
        return Err(format!(
            "invalid --callback-url `{callback_url}` (must start with http:// or https://)"
        ));
    }

    let state_dir = resolve_default_state_dir(args.state_dir.clone());
    let workdir = resolve_default_workdir(args.workdir.clone());
    std::fs::create_dir_all(&state_dir)
        .map_err(|e| format!("cannot create state dir {}: {e}", state_dir.display()))?;
    let turns_dir = state_dir.join("turns");
    std::fs::create_dir_all(&turns_dir)
        .map_err(|e| format!("cannot create turns dir {}: {e}", turns_dir.display()))?;

    let token = resolve_callback_token(&args)?;

    if args.detached {
        return spawn_detached_turn(&args, &state_dir, &workdir, &turns_dir, &token);
    }

    let prompt = read_prompt(&args)?;
    let cleanup_token_file = args
        .callback_token_file
        .as_deref()
        .filter(|p| p.starts_with(&turns_dir))
        .map(Path::to_path_buf);

    let result = execute_turn_foreground(
        &args,
        &state_dir,
        &workdir,
        &turns_dir,
        &callback_url,
        &token,
        &prompt,
    );

    if let Some(tf) = cleanup_token_file {
        let _ = std::fs::remove_file(tf);
    }
    result
}

fn spawn_detached_turn(
    args: &TurnArgs,
    state_dir: &Path,
    workdir: &Path,
    turns_dir: &Path,
    token: &str,
) -> Result<(), String> {
    // Fence the previous worker before sharing any per-turn files with a new one.
    #[cfg(target_os = "linux")]
    let (dispatch, launch_token) = (|| -> std::io::Result<_> {
        let dispatch = custody::Dispatch::acquire(turns_dir)?;
        dispatch.retire_prior()?;
        Ok((dispatch, custody::launch_token()?))
    })()
    .map_err(|e| format!("cannot fence prior detached turn: {e}"))?;

    // Materialize prompt to a file before detaching because child stdin is `/dev/null`.
    let prompt_file = if let Some(ref existing) = args.prompt_file {
        existing.clone()
    } else {
        let prompt = read_prompt(args)?;
        let path = turns_dir.join(format!("{}.prompt", args.turn_id));
        anyagents_harness::atomic_write_file(&path, prompt.as_bytes(), 0o600)
            .map_err(|e| format!("cannot persist turn prompt: {e}"))?;
        path
    };

    // Materialize structured OutboundMessage envelope when provided via --message-file
    // or ANYAGENTS_GUEST_MESSAGE_B64 so the detached child delivers full 6-dimension context.
    let message_file = if let Some(ref existing) = args.message_file {
        Some(existing.clone())
    } else {
        materialize_env_message_file(turns_dir, &args.turn_id)
    };

    // Pass callback token via a 0600 file in `<state-dir>/turns/<turn-id>.token` rather
    // than exposing it on child argv (`/proc/<pid>/cmdline`).
    let token_file = turns_dir.join(format!("{}.token", args.turn_id));
    anyagents_harness::atomic_write_nonempty_file(
        &token_file,
        format!("{token}\n").as_bytes(),
        0o600,
    )
    .map_err(|e| format!("cannot persist turn callback token: {e}"))?;

    // Create the per-turn atomic steer spool dir before detaching so coordinator
    // mid-turn steers can drop immediately after accepted JSON is returned.
    let _ = crate::steer::ensure_steer_spool_dir(turns_dir, &args.turn_id);

    let log_path = turns_dir.join(format!("{}.log", args.turn_id));
    let log_out = open_log_file(&log_path)?;
    let log_err = log_out
        .try_clone()
        .map_err(|e| format!("cannot clone turn log fd: {e}"))?;

    let exe = std::env::current_exe()
        .map_err(|e| format!("cannot resolve current anyagents-guest executable: {e}"))?;

    let mut cmd = Command::new(&exe);
    cmd.arg("turn")
        .arg("--turn-id")
        .arg(&args.turn_id)
        .arg("--agent-id")
        .arg(&args.agent_id)
        .arg("--prompt-file")
        .arg(&prompt_file)
        .arg("--callback-url")
        .arg(args.callback_url.trim())
        .arg("--callback-token-file")
        .arg(&token_file)
        .arg("--state-dir")
        .arg(state_dir)
        .arg("--workdir")
        .arg(workdir);

    if let Some(ref mf) = message_file {
        cmd.arg("--message-file").arg(mf);
    }
    cmd.env_remove(anyagents_harness::GUEST_MESSAGE_B64_ENV);

    if let Some(ref h) = args.harness {
        cmd.arg("--harness").arg(h);
    }
    if let Some(ref p) = args.program {
        cmd.arg("--program").arg(p);
    }
    if let Some(ref m) = args.model {
        cmd.arg("--model").arg(m);
    }
    if let Some(ref s) = args.session {
        cmd.arg("--session").arg(s);
    }
    if let Some(hb) = args.heartbeat_interval_ms {
        cmd.arg("--heartbeat-interval-ms").arg(hb.to_string());
    }
    if let Some(ref git) = args.git_program {
        cmd.arg("--git-program").arg(git);
    }
    cmd.stdin(Stdio::null())
        .stdout(Stdio::from(log_out))
        .stderr(Stdio::from(log_err));

    #[cfg(target_os = "linux")]
    cmd.arg("--detached-worker")
        .env(custody::OWNER_ENV, &launch_token)
        .stdin(Stdio::piped());
    if !args.extra_args.is_empty() {
        cmd.arg("--");
        cmd.args(&args.extra_args);
    }

    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt as _;
        // SAFETY: `setsid()` is async-signal-safe and detaches the worker into its own session
        // so closing the coordinator's exec/envd stream never SIGHUPs the running turn.
        unsafe {
            cmd.pre_exec(|| {
                if libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
    }

    let child = cmd
        .spawn()
        .map_err(|e| format!("failed to spawn detached turn worker: {e}"))?;
    let pid = child.id();
    #[cfg(target_os = "linux")]
    {
        let mut child = child;
        let mut gate = child.stdin.take().expect("piped detached worker stdin");
        let admit = dispatch
            .record(pid, launch_token)
            .and_then(|()| gate.write_all(&[1]));
        drop(gate);
        if let Err(e) = admit {
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!("cannot admit detached turn worker: {e}"));
        }
    }

    let accepted = json!({
        "status": "accepted",
        "turn_id": args.turn_id,
        "agent_id": args.agent_id,
        "pid": pid,
        "log_file": log_path.display().to_string(),
    });
    let mut stdout = std::io::stdout().lock();
    writeln!(stdout, "{accepted}").map_err(|e| format!("stdout write failed: {e}"))?;
    stdout
        .flush()
        .map_err(|e| format!("stdout flush failed: {e}"))?;
    Ok(())
}

/// RAII background heartbeat ticker that keeps the coordinator's remote-turn
/// lease alive continuously across blocking startup phases (`cloning`,
/// `starting`, `in_turn`) and joins cleanly on drop. Stops automatically if
/// [`TurnWatchdog`] expires so a deadlocked child cannot mask itself forever.
struct HeartbeatGuard {
    stop: Arc<(Mutex<bool>, Condvar)>,
    phase: Arc<Mutex<(Option<String>, &'static str)>>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl HeartbeatGuard {
    #[allow(clippy::too_many_arguments)]
    fn start(
        callback_url: &str,
        callback_token: &str,
        turn_id: &str,
        agent_id: &str,
        harness_name: &str,
        initial_session: Option<String>,
        initial_phase: &'static str,
        interval: Option<Duration>,
        state_dir: &Path,
        watchdog: TurnWatchdog,
    ) -> Self {
        let stop = Arc::new((Mutex::new(false), Condvar::new()));
        let phase = Arc::new(Mutex::new((initial_session, initial_phase)));
        let stop_bg = Arc::clone(&stop);
        let phase_bg = Arc::clone(&phase);
        let url = callback_url.to_string();
        let token = callback_token.to_string();
        let tid = turn_id.to_string();
        let aid = agent_id.to_string();
        let harness = harness_name.to_string();
        let sdir = state_dir.to_path_buf();

        let handle = std::thread::Builder::new()
            .name("anyagents-guest-hb".into())
            .spawn(move || {
                let tick = || -> bool {
                    let (sid, st) = phase_bg.lock().unwrap_or_else(|e| e.into_inner()).clone();
                    if watchdog.expired_reason(st == "in_turn", &harness).is_some() {
                        return false;
                    }
                    emit_heartbeat(&url, &token, &tid, &aid, &harness, sid, st, 1, &sdir);
                    true
                };
                if !tick() {
                    return;
                }
                let Some(iv) = interval else {
                    return;
                };
                let (lock, cv) = &*stop_bg;
                loop {
                    let guard = lock.lock().unwrap_or_else(|e| e.into_inner());
                    let (stopped, _) = cv
                        .wait_timeout_while(guard, iv, |s| !*s)
                        .unwrap_or_else(|e| e.into_inner());
                    if *stopped {
                        break;
                    }
                    drop(stopped);
                    if !tick() {
                        break;
                    }
                }
            })
            .ok();

        Self {
            stop,
            phase,
            handle,
        }
    }

    fn set_phase(&self, session_id: Option<String>, turn_state: &'static str) {
        let mut g = self.phase.lock().unwrap_or_else(|e| e.into_inner());
        if session_id.is_some() {
            g.0 = session_id;
        }
        g.1 = turn_state;
    }

    fn stop(&mut self) {
        {
            let mut stopped = self.stop.0.lock().unwrap_or_else(|e| e.into_inner());
            *stopped = true;
        }
        self.stop.1.notify_all();
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

impl Drop for HeartbeatGuard {
    fn drop(&mut self) {
        self.stop();
    }
}

#[allow(clippy::too_many_arguments)]
fn execute_turn_foreground(
    args: &TurnArgs,
    state_dir: &Path,
    default_workdir: &Path,
    turns_dir: &Path,
    callback_url: &str,
    callback_token: &str,
    prompt: &str,
) -> Result<(), String> {
    let file_cfg = load_guest_config(state_dir);
    let work_dir = args
        .workdir
        .clone()
        .or_else(|| file_cfg.as_ref().and_then(|c| c.workdir.clone()))
        .unwrap_or_else(|| default_workdir.to_path_buf());
    std::fs::create_dir_all(&work_dir)
        .map_err(|e| format!("cannot create work dir {}: {e}", work_dir.display()))?;

    let raw_harness = args
        .harness
        .clone()
        .or_else(|| std::env::var("ANYAGENTS_GUEST_HARNESS").ok())
        .or_else(|| file_cfg.as_ref().and_then(|c| c.harness.clone()))
        .unwrap_or_else(|| "claude-code".to_string());
    let harness_name = canonical_guest_harness(&raw_harness);
    let session = crate::workspace_delta::resolve_guest_session(
        state_dir,
        &harness_name,
        args.session.as_deref(),
        file_cfg.as_ref().and_then(|c| c.session_id.as_deref()),
    );
    let heartbeat_interval_ms = args
        .heartbeat_interval_ms
        .or_else(|| {
            std::env::var("ANYAGENTS_GUEST_HEARTBEAT_INTERVAL_MS")
                .ok()
                .and_then(|v| v.trim().parse::<u64>().ok())
        })
        .unwrap_or(DEFAULT_HEARTBEAT_INTERVAL_MS);
    let heartbeat_interval =
        (heartbeat_interval_ms > 0).then(|| Duration::from_millis(heartbeat_interval_ms));
    let repo_spec = file_cfg
        .as_ref()
        .and_then(|c| c.repo_spec.clone())
        .or_else(|| load_repo_spec(state_dir));

    let watchdog = TurnWatchdog::from_env();

    // Start independent background heartbeat ticker before repo clone and harness
    // launch so long blocking phases never trigger false coordinator heartbeat timeouts,
    // while bounding heartbeats by `TurnWatchdog` so a deadlocked child cannot mask itself.
    let mut hb_guard = HeartbeatGuard::start(
        callback_url,
        callback_token,
        &args.turn_id,
        &args.agent_id,
        &harness_name,
        session.clone(),
        if repo_spec.is_some() {
            "cloning"
        } else {
            "starting"
        },
        heartbeat_interval,
        state_dir,
        watchdog.clone(),
    );

    if let Some(ref spec) = repo_spec
        && let Err(err) =
            ensure_repo_checked_out(&work_dir, state_dir, spec, args.git_program.as_deref())
    {
        hb_guard.stop();
        clear_activity_snapshot(state_dir);
        let _ = send_turn_failed(
            callback_url,
            callback_token,
            &args.turn_id,
            &args.agent_id,
            &harness_name,
            None,
            &err,
            &TurnUsage::default(),
            turns_dir,
            &BTreeMap::new(),
        );
        return Err(err);
    }
    hb_guard.set_phase(None, "starting");

    let program = args
        .program
        .clone()
        .or_else(|| file_cfg.as_ref().and_then(|c| c.program.clone()))
        .unwrap_or_else(|| resolve_guest_program(&harness_name));
    let model = args
        .model
        .clone()
        .or_else(|| file_cfg.as_ref().and_then(|c| c.model.clone()));
    let mut extra_args = file_cfg
        .as_ref()
        .map(|c| c.extra_args.clone())
        .unwrap_or_default();
    if !args.extra_args.is_empty() {
        extra_args = args.extra_args.clone();
    }

    let env = build_guest_env(state_dir, &work_dir, file_cfg.as_ref());
    if harness_name == "agy" {
        prune_legacy_agy_tool_guard(state_dir);
    }
    let pre_turn_auth = snapshot_syncable_auth_files(state_dir);
    let mut transcript_relay = crate::transcript_relay::TranscriptRelay::new(
        state_dir,
        &harness_name,
        &args.turn_id,
        &args.agent_id,
        callback_url,
        callback_token,
    );

    let mut adapter = match launch_guest_harness(
        &harness_name,
        &program,
        model.as_deref(),
        session.as_deref(),
        &extra_args,
        state_dir,
        &work_dir,
        &env,
        file_cfg.as_ref(),
    ) {
        Ok(a) => a,
        Err(err) => {
            let msg = format!("failed to start guest harness `{harness_name}`: {err}");
            hb_guard.stop();
            clear_activity_snapshot(state_dir);
            let synced_auth = collect_changed_syncable_auth_files(state_dir, &pre_turn_auth);
            let _ = send_turn_failed(
                callback_url,
                callback_token,
                &args.turn_id,
                &args.agent_id,
                &harness_name,
                session.as_deref(),
                &msg,
                &TurnUsage::default(),
                turns_dir,
                &synced_auth,
            );
            return Err(msg);
        }
    };

    let wakeup = Arc::new((Mutex::new(false), Condvar::new()));
    {
        let w = Arc::clone(&wakeup);
        let wd = watchdog.clone();
        adapter.watch_activity(Arc::new(move || {
            wd.touch();
            signal_wake(&w);
        }));
    }

    let base_usage = adapter.served_usage();
    let mut last_usage_tokens = (
        base_usage.input_tokens,
        base_usage.output_tokens,
        base_usage.cache_read_tokens,
        base_usage.cache_write_tokens,
    );
    let base_turns = adapter.turns_served();

    let initial_sid = adapter.session_id().or(session);
    watchdog.touch();
    hb_guard.set_phase(initial_sid.clone(), "in_turn");

    let mut msg = resolve_outbound_message(args, prompt);
    msg.text = crate::media::hydrate_turn_media_refs(&msg.text, state_dir, &env, file_cfg.as_ref());
    if msg.context.workspace.is_none() {
        msg.context.workspace = crate::workspace_delta::capture_and_diff(
            &work_dir,
            state_dir,
            args.git_program.as_deref(),
        );
    }
    crate::knowledge_wake::enrich(&mut msg, callback_url, callback_token, &args.turn_id);
    let delivery_id = msg.id.clone();

    let mut delivered_done = false;
    let mut delivery_error: Option<String> = None;
    let deliver_deadline = Instant::now() + Duration::from_secs(60);
    loop {
        match adapter.deliver(&msg) {
            Ok(DeliverOutcome::Accepted { turn_finished }) => {
                delivered_done = turn_finished;
                break;
            }
            Ok(DeliverOutcome::InFlight) => break,
            Ok(DeliverOutcome::Duplicate) => {
                delivered_done = true;
                break;
            }
            Ok(DeliverOutcome::Deferred) => {
                if Instant::now() >= deliver_deadline {
                    delivery_error = Some("guest harness deferred turn past deadline".into());
                    break;
                }
                wait_wake(&wakeup, POLL_TICK);
            }
            Ok(DeliverOutcome::Quota { reset_at_ms }) => {
                delivery_error = Some(format!("quota exhausted (resets at {reset_at_ms:?})"));
                break;
            }
            Ok(DeliverOutcome::Rejected { reason, .. }) => {
                delivery_error = Some(reason);
                break;
            }
            Err(HarnessError::Dead) => {
                delivery_error = Some(
                    adapter
                        .exit_diagnostic()
                        .unwrap_or_else(|| "guest harness exited before accepting turn".into()),
                );
                break;
            }
            Err(err) => {
                delivery_error = Some(err.to_string());
                break;
            }
        }
    }

    if let Some(err_msg) = delivery_error {
        let sid = adapter.session_id().or(initial_sid);
        let usage = adapter
            .usage_for_id(&delivery_id)
            .unwrap_or_else(|| adapter.served_usage().difference(base_usage));
        hb_guard.stop();
        clear_activity_snapshot(state_dir);
        adapter.stop();
        transcript_relay.flush_all(sid.as_deref());
        let synced_auth = collect_changed_syncable_auth_files(state_dir, &pre_turn_auth);
        send_turn_failed(
            callback_url,
            callback_token,
            &args.turn_id,
            &args.agent_id,
            &harness_name,
            sid.as_deref(),
            &err_msg,
            &usage,
            turns_dir,
            &synced_auth,
        )?;
        return Err(err_msg);
    }

    let mut sent_text_len = 0usize;
    let mut delta_seq = 0u64;
    let mut err_override: Option<String> = None;
    let mut turn_finished_cleanly = delivered_done;
    let mut last_persisted_sid: Option<String> = None;
    let mut last_opencode_delta_poll = Instant::now() - OPENCODE_DELTA_POLL_INTERVAL;
    let mut progress_tracker =
        StateDirProgressTracker::new(state_dir, STATE_DIR_PROGRESS_PROBE_INTERVAL);
    let mut steer_pump = crate::steer::SteerPump::new(
        turns_dir,
        &args.turn_id,
        &args.agent_id,
        &harness_name,
        callback_url,
        callback_token,
    );

    if !delivered_done {
        loop {
            if steer_pump.tick(
                adapter.as_mut(),
                state_dir,
                &env,
                file_cfg.as_ref(),
                &watchdog,
            ) {
                watchdog.touch();
            }

            let health = adapter.health();
            let live_sid = adapter.session_id();
            if let Some(ref sid) = live_sid
                && last_persisted_sid.as_deref() != Some(sid.as_str())
            {
                let _ = persist_session(state_dir, &harness_name, sid);
                last_persisted_sid = Some(sid.clone());
                watchdog.touch();
            }
            let active_sid = live_sid.or_else(|| initial_sid.clone());
            hb_guard.set_phase(active_sid.clone(), turn_state_label(health.turn_state));

            // Stream incremental deltas if harness accumulated new output.
            // Throttle `opencode` delta polling because `answer_for` invokes `opencode api`.
            let should_poll_delta = if harness_name == "opencode" {
                if last_opencode_delta_poll.elapsed() >= OPENCODE_DELTA_POLL_INTERVAL {
                    last_opencode_delta_poll = Instant::now();
                    true
                } else {
                    false
                }
            } else {
                true
            };
            let current_answer = if should_poll_delta {
                steer_pump
                    .latest_answer(adapter.as_ref(), state_dir)
                    .or_else(|| adapter.answer_for(&delivery_id, state_dir))
            } else {
                None
            };
            if let Some(current_text) = current_answer
                && current_text.len() > sent_text_len
                && current_text.is_char_boundary(sent_text_len)
            {
                let chunk = &current_text[sent_text_len..];
                if !chunk.is_empty() {
                    watchdog.touch();
                    delta_seq += 1;
                    sent_text_len = current_text.len();
                    let _ = post_callback_with_retry(
                        callback_url,
                        callback_token,
                        &json!({
                            "event_type": "delta",
                            "turn_id": args.turn_id,
                            "agent_id": args.agent_id,
                            "harness": harness_name,
                            "seq": delta_seq,
                            "text": chunk,
                            "timestamp_ms": now_unix_ms(),
                        }),
                        1,
                        HEARTBEAT_TIMEOUT_SECS,
                    );
                }
            }

            let cur_usage = adapter.served_usage();
            let cur_tokens = (
                cur_usage.input_tokens,
                cur_usage.output_tokens,
                cur_usage.cache_read_tokens,
                cur_usage.cache_write_tokens,
            );
            if cur_tokens != last_usage_tokens {
                last_usage_tokens = cur_tokens;
                watchdog.touch();
            }
            if progress_tracker.poll_changed() {
                watchdog.touch();
            }
            if transcript_relay.tick(active_sid.as_deref()) {
                watchdog.touch();
            }

            let proven = adapter.proven(&delivery_id);
            let steers_active = steer_pump.has_active_work(adapter.as_ref(), health.turn_state);
            let done_by_proof = !steers_active
                && matches!(
                    proven,
                    Some(Proof::Taken {
                        turn_finished: true
                    }) | Some(Proof::Duplicate)
                );
            let done_by_turns = !steers_active
                && adapter.turns_served() > base_turns
                && health.turn_state == TurnState::Idle;
            let failure = adapter
                .failure_for_id(&delivery_id)
                .or_else(|| steer_pump.first_failure(adapter.as_ref()))
                .or_else(|| health.sign_in_fault.clone());

            if done_by_proof || done_by_turns || failure.is_some() || !health.process_alive {
                turn_finished_cleanly = (done_by_proof || done_by_turns) && failure.is_none();
                err_override = failure.or_else(|| {
                    (!health.process_alive && !done_by_proof && !done_by_turns).then(|| {
                        adapter
                            .exit_diagnostic()
                            .unwrap_or_else(|| "guest harness process exited mid-turn".into())
                    })
                });
                break;
            }

            if let Some(reason) = watchdog.expired_reason(true, &harness_name) {
                err_override = Some(reason);
                break;
            }

            let wait_dur = match heartbeat_interval {
                Some(iv) => POLL_TICK.min(iv.max(Duration::from_millis(10))),
                None => POLL_TICK,
            };
            wait_wake(&wakeup, wait_dur);
        }
    }

    // Drain any steer proofs emitted alongside the terminal turn frame.
    steer_pump.drain_final_proofs(adapter.as_ref());

    let sid = adapter
        .session_id()
        .or(initial_sid)
        .unwrap_or_else(|| format!("{harness_name}-guest-session"));
    let _ = persist_session(state_dir, &harness_name, &sid);

    let delta_usage = adapter.served_usage().difference(base_usage);
    let usage = if !steer_pump.consumed_steers().is_empty()
        && (delta_usage.input_tokens > 0 || delta_usage.output_tokens > 0)
    {
        delta_usage
    } else {
        adapter.usage_for_id(&delivery_id).unwrap_or(delta_usage)
    };
    let reply_opt = steer_pump
        .latest_answer(adapter.as_ref(), state_dir)
        .or_else(|| adapter.answer_for(&delivery_id, state_dir));
    let reply_empty = reply_opt.as_deref().is_none_or(|s| s.trim().is_empty());
    if err_override.is_none() && reply_empty {
        err_override = adapter
            .failure_for_id(&delivery_id)
            .or_else(|| steer_pump.first_failure(adapter.as_ref()))
            .or_else(|| {
                (!turn_finished_cleanly)
                    .then(|| adapter.exit_diagnostic())
                    .flatten()
            });
    }

    // Stop background heartbeat ticker before terminal snapshot/callbacks so no
    // stale heartbeat races after `turn_complete` or `turn_failed`.
    hb_guard.stop();
    // Capture live descendant snapshot right before stopping adapter so coordinator
    // knows whether any background child subprocesses outlive the turn.
    let final_snap =
        GuestActivitySnapshot::capture_current(&harness_name, Some(sid.clone()), "idle", 0, 0);
    clear_activity_snapshot(state_dir);
    adapter.stop();
    // Drain proofs once more after `adapter.stop()` flushed any trailing stdout frames,
    // and flush any trailing native transcript entries before terminal settlement.
    steer_pump.drain_final_proofs(adapter.as_ref());
    transcript_relay.flush_all(Some(&sid));
    let synced_auth = collect_changed_syncable_auth_files(state_dir, &pre_turn_auth);

    if let Some(err_msg) = err_override {
        send_turn_failed(
            callback_url,
            callback_token,
            &args.turn_id,
            &args.agent_id,
            &harness_name,
            Some(&sid),
            &err_msg,
            &usage,
            turns_dir,
            &synced_auth,
        )?;
        return Ok(());
    }

    let reply = reply_opt.filter(|s| !s.trim().is_empty());

    if let Some(ref reply_text) = reply
        && reply_text.len() > sent_text_len
        && reply_text.is_char_boundary(sent_text_len)
    {
        let chunk = &reply_text[sent_text_len..];
        if !chunk.is_empty() {
            delta_seq += 1;
            let _ = post_callback_with_retry(
                callback_url,
                callback_token,
                &json!({
                    "event_type": "delta",
                    "turn_id": args.turn_id,
                    "agent_id": args.agent_id,
                    "harness": harness_name,
                    "seq": delta_seq,
                    "text": chunk,
                    "timestamp_ms": now_unix_ms(),
                }),
                1,
                HEARTBEAT_TIMEOUT_SECS,
            );
        }
    }

    let mut audit_payload = json!({
        "event_type": "turn_complete",
        "turn_id": args.turn_id,
        "agent_id": args.agent_id,
        "harness": harness_name,
        "session_id": sid,
        "result_text": reply,
        "usage": usage_json(&usage),
        "inflight_children": final_snap.inflight_children,
        "timestamp_ms": now_unix_ms(),
    });
    if !steer_pump.consumed_steers().is_empty()
        && let Some(obj) = audit_payload.as_object_mut()
    {
        obj.insert(
            "consumed_steers".to_string(),
            json!(steer_pump.consumed_steers()),
        );
    }
    persist_turn_result_audit(turns_dir, &args.turn_id, &audit_payload);
    let mut callback_payload = audit_payload;
    if !synced_auth.is_empty()
        && let Some(obj) = callback_payload.as_object_mut()
    {
        obj.insert("synced_auth_files".to_string(), json!(synced_auth));
    }
    post_callback_with_retry(
        callback_url,
        callback_token,
        &callback_payload,
        DEFAULT_MAX_RETRIES,
        CALLBACK_TIMEOUT_SECS,
    )
}

#[allow(clippy::too_many_arguments)]
fn emit_heartbeat(
    callback_url: &str,
    callback_token: &str,
    turn_id: &str,
    agent_id: &str,
    harness_name: &str,
    session_id: Option<String>,
    turn_state: &str,
    active_turns: usize,
    state_dir: &Path,
) {
    let snap = GuestActivitySnapshot::capture_current(
        harness_name,
        session_id,
        turn_state,
        active_turns,
        0,
    );
    let _ = write_activity_snapshot(state_dir, &snap);
    let payload = json!({
        "event_type": "heartbeat",
        "turn_id": turn_id,
        "agent_id": agent_id,
        "harness": snap.harness,
        "pid": snap.pid,
        "timestamp_ms": snap.timestamp_ms,
        "session_id": snap.session_id,
        "turn_state": snap.turn_state,
        "active_turns": snap.active_turns,
        "queued_turns": snap.queued_turns,
        "inflight_children": snap.inflight_children,
        "descendant_processes": snap.descendant_processes,
        "busy": snap.busy,
    });
    let _ = post_callback_with_retry(
        callback_url,
        callback_token,
        &payload,
        1,
        HEARTBEAT_TIMEOUT_SECS,
    );
}

#[allow(clippy::too_many_arguments)]
fn send_turn_failed(
    callback_url: &str,
    callback_token: &str,
    turn_id: &str,
    agent_id: &str,
    harness_name: &str,
    session_id: Option<&str>,
    error_text: &str,
    usage: &TurnUsage,
    turns_dir: &Path,
    synced_auth: &BTreeMap<String, String>,
) -> Result<(), String> {
    let audit_payload = json!({
        "event_type": "turn_failed",
        "turn_id": turn_id,
        "agent_id": agent_id,
        "harness": harness_name,
        "session_id": session_id,
        "error_text": error_text,
        "usage": usage_json(usage),
        "timestamp_ms": now_unix_ms(),
    });
    persist_turn_result_audit(turns_dir, turn_id, &audit_payload);
    let mut callback_payload = audit_payload;
    if !synced_auth.is_empty()
        && let Some(obj) = callback_payload.as_object_mut()
    {
        obj.insert("synced_auth_files".to_string(), json!(synced_auth));
    }
    post_callback_with_retry(
        callback_url,
        callback_token,
        &callback_payload,
        DEFAULT_MAX_RETRIES,
        CALLBACK_TIMEOUT_SECS,
    )
}

fn snapshot_syncable_auth_files(state_dir: &Path) -> BTreeMap<&'static str, Vec<u8>> {
    let mut snap = BTreeMap::new();
    for &rel in SYNCABLE_AUTH_FILES {
        let path = state_dir.join(rel);
        if let Ok(meta) = std::fs::metadata(&path)
            && meta.is_file()
            && meta.len() <= MAX_SYNCABLE_AUTH_FILE_BYTES as u64
            && let Ok(bytes) = std::fs::read(&path)
        {
            snap.insert(rel, bytes);
        }
    }
    snap
}

fn collect_changed_syncable_auth_files(
    state_dir: &Path,
    before: &BTreeMap<&'static str, Vec<u8>>,
) -> BTreeMap<String, String> {
    let mut changed = BTreeMap::new();
    for &rel in SYNCABLE_AUTH_FILES {
        let path = state_dir.join(rel);
        let Ok(meta) = std::fs::metadata(&path) else {
            continue;
        };
        if !meta.is_file() || meta.len() == 0 || meta.len() > MAX_SYNCABLE_AUTH_FILE_BYTES as u64 {
            continue;
        }
        let Ok(bytes) = std::fs::read(&path) else {
            continue;
        };
        if bytes.is_empty() || bytes.len() > MAX_SYNCABLE_AUTH_FILE_BYTES {
            continue;
        }
        if before.get(rel).is_some_and(|prev| prev == &bytes) {
            continue;
        }
        if !serde_json::from_slice::<Value>(&bytes).is_ok_and(|v| v.is_object()) {
            continue;
        }
        changed.insert(rel.to_string(), encode_base64_standard(&bytes));
    }
    changed
}

fn prune_legacy_agy_tool_guard(state_dir: &Path) {
    let hooks_path = state_dir.join(".gemini").join("config").join("hooks.json");
    if let Ok(raw) = std::fs::read_to_string(&hooks_path)
        && let Ok(mut root) = serde_json::from_str::<Value>(&raw)
        && let Some(map) = root.as_object_mut()
    {
        let mut changed = map.remove("anyagents-native-tool-guard").is_some();
        if map.remove("anyagents-steer").is_some() {
            changed = true;
        }
        if changed {
            if map.is_empty() {
                let _ = std::fs::remove_file(&hooks_path);
            } else if let Ok(mut body) = serde_json::to_vec_pretty(&root) {
                body.push(b'\n');
                let _ = std::fs::write(&hooks_path, body);
            }
        }
    }

    let settings_path = state_dir
        .join(".gemini")
        .join("antigravity-cli")
        .join("settings.json");
    if let Ok(raw) = std::fs::read_to_string(&settings_path)
        && let Ok(mut root) = serde_json::from_str::<Value>(&raw)
        && let Some(deny_arr) = root
            .get_mut("permissions")
            .and_then(|p| p.get_mut("deny"))
            .and_then(Value::as_array_mut)
    {
        const LEGACY_FILE_SHELL_DENIES: &[&str] = &[
            "read_file",
            "view_file",
            "read_file(*)",
            "run_command",
            "manage_task",
            "write_file",
            "edit_file",
            "grep_search",
            "find_by_name",
            "list_dir",
            "command(*)",
            "write_file(*)",
        ];
        let before = deny_arr.len();
        deny_arr.retain(|v| {
            !v.as_str()
                .is_some_and(|s| LEGACY_FILE_SHELL_DENIES.contains(&s))
        });
        if deny_arr.len() != before
            && let Ok(body) = serde_json::to_vec_pretty(&root)
        {
            let _ = std::fs::write(&settings_path, body);
        }
    }
}

pub(crate) fn encode_base64_standard(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
        let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(TABLE[((triple >> 18) & 0x3f) as usize] as char);
        out.push(TABLE[((triple >> 12) & 0x3f) as usize] as char);
        if chunk.len() > 1 {
            out.push(TABLE[((triple >> 6) & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(TABLE[(triple & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

fn persist_turn_result_audit(turns_dir: &Path, turn_id: &str, payload: &Value) {
    if let Ok(bytes) = serde_json::to_vec_pretty(payload) {
        let path = turns_dir.join(format!("{turn_id}.result.json"));
        let _ = anyagents_harness::atomic_write_file(&path, &bytes, 0o600);
    }
}

/// POST a JSON callback event with `Authorization: Bearer <token>` and bounded
/// exponential-backoff retries on transient transport or 5xx/429/408 errors.
pub fn post_callback_with_retry(
    url: &str,
    token: &str,
    payload: &Value,
    max_retries: usize,
    timeout_secs: u64,
) -> Result<(), String> {
    let base_backoff_ms = std::env::var("ANYAGENTS_GUEST_CALLBACK_BACKOFF_MS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|&ms| ms > 0)
        .unwrap_or(DEFAULT_INITIAL_BACKOFF_MS);
    post_callback_with_backoff(
        url,
        token,
        payload,
        max_retries,
        timeout_secs,
        base_backoff_ms,
    )
}

fn post_callback_with_backoff(
    url: &str,
    token: &str,
    payload: &Value,
    max_retries: usize,
    timeout_secs: u64,
    initial_backoff_ms: u64,
) -> Result<(), String> {
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(timeout_secs.max(1))))
        .http_status_as_error(false)
        .build()
        .into();

    let mut backoff_ms = initial_backoff_ms.max(10);
    let mut last_err = String::new();
    for attempt in 0..=max_retries {
        let resp_res = agent
            .post(url)
            .header("content-type", "application/json")
            .header("authorization", &format!("Bearer {token}"))
            .send_json(payload);

        match resp_res {
            Ok(resp) => {
                let status = resp.status().as_u16();
                if (200..300).contains(&status) {
                    return Ok(());
                }
                let retryable = status == 408 || status == 429 || (500..=599).contains(&status);
                last_err = format!("callback `{url}` returned HTTP {status}");
                if !retryable || attempt == max_retries {
                    return Err(last_err);
                }
            }
            Err(err) => {
                last_err = format!("callback `{url}` transport error: {err}");
                if attempt == max_retries {
                    return Err(last_err);
                }
            }
        }
        std::thread::sleep(Duration::from_millis(backoff_ms));
        backoff_ms = backoff_ms.saturating_mul(2).min(5_000);
    }
    Err(last_err)
}

fn validate_turn_id(turn_id: &str) -> Result<(), String> {
    let trimmed = turn_id.trim();
    if trimmed.is_empty()
        || trimmed.len() > 128
        || trimmed.contains('/')
        || trimmed.contains('\\')
        || trimmed.contains("..")
        || trimmed.bytes().any(|b| b.is_ascii_control())
    {
        return Err(format!("invalid --turn-id `{turn_id}`"));
    }
    Ok(())
}

fn resolve_callback_token(args: &TurnArgs) -> Result<String, String> {
    if let Some(ref tok) = args.callback_token {
        let trimmed = tok.trim();
        if !trimmed.is_empty() {
            return Ok(trimmed.to_string());
        }
    }
    if let Some(ref path) = args.callback_token_file {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| format!("cannot read --callback-token-file {}: {e}", path.display()))?;
        let trimmed = raw.trim();
        if !trimmed.is_empty() {
            return Ok(trimmed.to_string());
        }
    }
    if let Ok(env_tok) = std::env::var("ANYAGENTS_CALLBACK_TOKEN") {
        let trimmed = env_tok.trim();
        if !trimmed.is_empty() {
            return Ok(trimmed.to_string());
        }
    }
    Err("missing callback token: pass --callback-token, --callback-token-file, or ANYAGENTS_CALLBACK_TOKEN".into())
}

fn materialize_env_message_file(turns_dir: &Path, turn_id: &str) -> Option<PathBuf> {
    let encoded = std::env::var(anyagents_harness::GUEST_MESSAGE_B64_ENV).ok()?;
    materialize_b64_message_file(turns_dir, turn_id, &encoded)
}

fn materialize_b64_message_file(turns_dir: &Path, turn_id: &str, encoded: &str) -> Option<PathBuf> {
    if encoded.trim().is_empty() {
        return None;
    }
    match anyagents_harness::decode_outbound_message_b64(encoded) {
        Ok(msg) => {
            let bytes = serde_json::to_vec(&msg).ok()?;
            let path = turns_dir.join(format!("{turn_id}.message.json"));
            if let Err(err) = anyagents_harness::atomic_write_file(&path, &bytes, 0o600) {
                eprintln!("anyagents-guest: cannot persist turn message file: {err}");
                return None;
            }
            Some(path)
        }
        Err(err) => {
            eprintln!(
                "anyagents-guest: ignoring invalid {}: {err}",
                anyagents_harness::GUEST_MESSAGE_B64_ENV
            );
            None
        }
    }
}

fn load_outbound_message_file(path: &Path) -> Option<OutboundMessage> {
    let raw = std::fs::read_to_string(path).ok()?;
    serde_json::from_str::<OutboundMessage>(&raw).ok()
}

fn resolve_outbound_message(args: &TurnArgs, prompt: &str) -> OutboundMessage {
    let parsed = args
        .message_file
        .as_deref()
        .and_then(load_outbound_message_file)
        .or_else(|| {
            std::env::var(anyagents_harness::GUEST_MESSAGE_B64_ENV)
                .ok()
                .filter(|s| !s.trim().is_empty())
                .and_then(|s| anyagents_harness::decode_outbound_message_b64(&s).ok())
        });
    if let Some(mut msg) = parsed {
        if msg.id.trim().is_empty() {
            msg.id = args.turn_id.clone();
        }
        return msg;
    }
    OutboundMessage {
        id: args.turn_id.clone(),
        seq: 1,
        text: prompt.to_string(),
        sender: None,
        reply_to: None,
        source_time_ms: None,
        context: Context::default(),
    }
}

fn read_prompt(args: &TurnArgs) -> Result<String, String> {
    if let Some(ref inline) = args.prompt {
        return Ok(inline.clone());
    }
    if let Some(ref path) = args.prompt_file {
        return std::fs::read_to_string(path)
            .map_err(|e| format!("cannot read --prompt-file {}: {e}", path.display()));
    }
    if args.prompt_stdin {
        let mut buf = String::new();
        std::io::stdin()
            .read_to_string(&mut buf)
            .map_err(|e| format!("cannot read prompt from stdin: {e}"))?;
        return Ok(buf);
    }
    if let Some(ref msg_path) = args.message_file
        && let Some(msg) = load_outbound_message_file(msg_path)
    {
        return Ok(msg.text);
    }
    Err("turn requires --prompt-file, --prompt-stdin, or --prompt".into())
}

fn open_log_file(path: &Path) -> Result<File, String> {
    let mut opts = OpenOptions::new();
    opts.create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        opts.mode(0o600);
    }
    opts.open(path)
        .map_err(|e| format!("cannot open turn log {}: {e}", path.display()))
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead as _, BufReader};
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn post_callback_retries_transient_503_and_delivers_with_bearer_token() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let attempts = Arc::new(AtomicUsize::new(0));
        let received = Arc::new(Mutex::new(Vec::<(String, Value)>::new()));

        let attempts_srv = Arc::clone(&attempts);
        let received_srv = Arc::clone(&received);
        let server = std::thread::spawn(move || {
            for _ in 0..3 {
                let (mut stream, _) = listener.accept().unwrap();
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut content_len = 0usize;
                let mut auth_hdr = String::new();
                let mut line = String::new();
                loop {
                    line.clear();
                    let n = reader.read_line(&mut line).unwrap();
                    if n == 0 || line == "\r\n" || line == "\n" {
                        break;
                    }
                    let lower = line.to_ascii_lowercase();
                    if let Some(v) = lower.strip_prefix("content-length:") {
                        content_len = v.trim().parse().unwrap_or(0);
                    } else if lower.starts_with("authorization:") {
                        auth_hdr = line.split_once(':').unwrap().1.trim().to_string();
                    }
                }
                let mut body = vec![0u8; content_len];
                reader.read_exact(&mut body).unwrap();
                let n = attempts_srv.fetch_add(1, Ordering::SeqCst) + 1;
                if n < 3 {
                    let _ = stream.write_all(
                        b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 4\r\n\r\ncold",
                    );
                } else {
                    let json_body: Value = serde_json::from_slice(&body).unwrap();
                    received_srv.lock().unwrap().push((auth_hdr, json_body));
                    let _ = stream.write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 11\r\n\r\n{\"ok\":true}",
                    );
                }
            }
        });

        let url = format!("http://{addr}/v1/internal/guest/events");
        post_callback_with_backoff(
            &url,
            "rtk_secret_test",
            &json!({"event_type": "turn_complete", "turn_id": "turn_retry_1"}),
            5,
            5,
            15,
        )
        .expect("callback must succeed after transient 503 cold-start retries");

        server.join().unwrap();
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
        let got = received.lock().unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].0, "Bearer rtk_secret_test");
        assert_eq!(got[0].1["event_type"], "turn_complete");
    }

    #[test]
    fn post_callback_does_not_retry_401_unauthorized() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_srv = Arc::clone(&attempts);
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut content_len = 0usize;
            let mut line = String::new();
            loop {
                line.clear();
                let n = reader.read_line(&mut line).unwrap();
                if n == 0 || line == "\r\n" || line == "\n" {
                    break;
                }
                if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                    content_len = v.trim().parse().unwrap_or(0);
                }
            }
            let mut body = vec![0u8; content_len];
            let _ = reader.read_exact(&mut body);
            attempts_srv.fetch_add(1, Ordering::SeqCst);
            let _ = stream
                .write_all(b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 12\r\n\r\nunauthorized");
        });

        let url = format!("http://{addr}/v1/internal/guest/events");
        let err = post_callback_with_backoff(
            &url,
            "bad-token",
            &json!({"event_type": "turn_complete", "turn_id": "turn_401"}),
            5,
            5,
            10,
        )
        .unwrap_err();
        server.join().unwrap();
        assert!(err.contains("401"), "unexpected error: {err}");
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn syncable_auth_files_detect_only_changed_valid_json_and_base64_encode() {
        assert_eq!(encode_base64_standard(b""), "");
        assert_eq!(encode_base64_standard(b"f"), "Zg==");
        assert_eq!(encode_base64_standard(b"fo"), "Zm8=");
        assert_eq!(encode_base64_standard(b"foo"), "Zm9v");
        assert_eq!(encode_base64_standard(b"foobar"), "Zm9vYmFy");

        let dir = tempfile::tempdir().unwrap();
        let agy_rel = ".gemini/antigravity-cli/antigravity-oauth-token";
        let agy_path = dir.path().join(agy_rel);
        std::fs::create_dir_all(agy_path.parent().unwrap()).unwrap();
        let old_tok = br#"{"token":{"access_token":"old","expiry":"2026-11-19T10:00:00Z"}}"#;
        std::fs::write(&agy_path, old_tok).unwrap();

        let before = snapshot_syncable_auth_files(dir.path());
        assert!(collect_changed_syncable_auth_files(dir.path(), &before).is_empty());

        let new_tok = br#"{"token":{"access_token":"fresh","expiry":"2026-11-19T22:00:00Z"}}"#;
        std::fs::write(&agy_path, new_tok).unwrap();
        // Non-JSON garbage in another allowlisted path must be ignored.
        std::fs::write(dir.path().join(".credentials.json"), b"not-json").unwrap();

        let changed = collect_changed_syncable_auth_files(dir.path(), &before);
        assert_eq!(changed.len(), 1);
        assert_eq!(
            changed.get(agy_rel).map(String::as_str),
            Some(encode_base64_standard(new_tok).as_str())
        );
    }

    #[test]
    fn prune_legacy_agy_tool_guard_cleans_persistent_volume_hooks_and_file_shell_denies() {
        let dir = tempfile::tempdir().unwrap();
        let hooks_path = dir.path().join(".gemini/config/hooks.json");
        let settings_path = dir.path().join(".gemini/antigravity-cli/settings.json");
        std::fs::create_dir_all(hooks_path.parent().unwrap()).unwrap();
        std::fs::create_dir_all(settings_path.parent().unwrap()).unwrap();

        std::fs::write(
            &hooks_path,
            serde_json::json!({
                "anyagents-native-tool-guard": { "enabled": true },
                "anyagents-steer": { "enabled": true }
            })
            .to_string(),
        )
        .unwrap();
        std::fs::write(
            &settings_path,
            serde_json::json!({
                "permissions": {
                    "deny": [
                        "invoke_subagent",
                        "search_web",
                        "run_command",
                        "write_file",
                        "edit_file",
                        "read_file(*)",
                        "command(*)",
                        "write_file(*)"
                    ]
                }
            })
            .to_string(),
        )
        .unwrap();

        prune_legacy_agy_tool_guard(dir.path());

        assert!(!hooks_path.exists(), "empty hooks.json should be removed");
        let updated: Value =
            serde_json::from_str(&std::fs::read_to_string(&settings_path).unwrap()).unwrap();
        assert_eq!(
            updated["permissions"]["deny"],
            serde_json::json!(["invoke_subagent", "search_web"])
        );
    }

    #[test]
    fn message_file_and_b64_roundtrip_preserves_outbound_message_and_falls_back() {
        let dir = tempfile::tempdir().unwrap();
        let rich = OutboundMessage {
            id: "msg_01realid".into(),
            seq: 42,
            text: "check deployment status".into(),
            sender: Some("telegram:user:1234".into()),
            reply_to: Some("telegram:chat:-1009:thread:5".into()),
            source_time_ms: Some(1_795_137_786_000),
            context: Context {
                sender: Some("telegram:user:1234".into()),
                chat_id: Some(-1009),
                chat_title: Some("Release Room".into()),
                thread_id: Some(5),
                destination: Some("telegram:chat:-1009:thread:5".into()),
                topology: Some("<topology event=\"initial\" self=\"agent:ops\" />".into()),
                timestamp: Some("2026-11-20T01:23:06Z".into()),
                ..Default::default()
            },
        };
        let encoded = anyagents_harness::encode_outbound_message_b64(&rich).unwrap();
        let msg_path =
            materialize_b64_message_file(dir.path(), "turn_abc", &encoded).expect("message file");
        assert!(msg_path.ends_with("turn_abc.message.json"));

        let args = TurnArgs {
            detached: false,
            #[cfg(target_os = "linux")]
            detached_worker: false,
            turn_id: "turn_abc".into(),
            agent_id: "agt_test".into(),
            harness: None,
            program: None,
            model: None,
            session: None,
            prompt_file: None,
            message_file: Some(msg_path),
            prompt_stdin: false,
            prompt: None,
            callback_url: "http://127.0.0.1:9999/v1/internal/guest/events".into(),
            callback_token: Some("tok".into()),
            callback_token_file: None,
            heartbeat_interval_ms: None,
            state_dir: None,
            workdir: None,
            git_program: None,
            extra_args: Vec::new(),
        };
        assert_eq!(read_prompt(&args).unwrap(), "check deployment status");
        let resolved = resolve_outbound_message(&args, "fallback prompt");
        assert_eq!(resolved, rich);
        let composed =
            anyagents_harness::compose_message(&resolved.id, &resolved.context, &resolved.text);
        assert!(composed.contains("id=\"msg_01realid\""));
        assert!(composed.contains("from=\"telegram:user:1234\""));
        assert!(composed.contains("chat_title=\"Release Room\""));
        assert!(composed.contains("topic=\"5\""));
        assert!(composed.contains("at=\"2026-11-20T01:23:06Z\""));
        assert!(composed.contains("<topology event=\"initial\" self=\"agent:ops\" />"));
        assert!(!composed.contains("from=\"unknown\""));
        assert!(!composed.contains("anyagents:reply:turn_"));

        // Fallback works cleanly when `--message-file` is absent.
        let mut legacy_args = args;
        legacy_args.message_file = None;
        let fallback = resolve_outbound_message(&legacy_args, "plain prompt");
        assert_eq!(fallback.id, "turn_abc");
        assert_eq!(fallback.text, "plain prompt");
    }

    #[test]
    fn heartbeat_guard_ticks_while_caller_blocks_and_stops_cleanly_on_drop() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();
        let stop_srv = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop_srv_bg = Arc::clone(&stop_srv);
        let events = Arc::new(Mutex::new(Vec::<Value>::new()));
        let events_bg = Arc::clone(&events);

        let srv = std::thread::spawn(move || {
            while !stop_srv_bg.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let _ = stream.set_nonblocking(false);
                        let mut reader = BufReader::new(stream.try_clone().unwrap());
                        let mut content_len = 0usize;
                        let mut line = String::new();
                        loop {
                            line.clear();
                            let n = reader.read_line(&mut line).unwrap_or(0);
                            if n == 0 || line == "\r\n" || line == "\n" {
                                break;
                            }
                            if let Some(v) =
                                line.to_ascii_lowercase().strip_prefix("content-length:")
                            {
                                content_len = v.trim().parse().unwrap_or(0);
                            }
                        }
                        let mut body = vec![0u8; content_len];
                        if reader.read_exact(&mut body).is_ok()
                            && let Ok(val) = serde_json::from_slice::<Value>(&body)
                        {
                            events_bg.lock().unwrap().push(val);
                        }
                        let _ = stream.write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 11\r\n\r\n{\"ok\":true}",
                        );
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }
        });

        let dir = tempfile::tempdir().unwrap();
        let url = format!("http://{addr}/v1/internal/guest/events");
        let guard = HeartbeatGuard::start(
            &url,
            "tok_hb",
            "turn_hb_1",
            "agt_hb",
            "codex",
            None,
            "cloning",
            Some(Duration::from_millis(40)),
            dir.path(),
            TurnWatchdog::new(Some(Duration::from_secs(60)), Some(Duration::from_secs(60))),
        );
        let wait_until = |pred: &dyn Fn(&[Value]) -> bool| {
            let deadline = Instant::now() + Duration::from_secs(5);
            while Instant::now() < deadline {
                if pred(&events.lock().unwrap()) {
                    return;
                }
                std::thread::sleep(Duration::from_millis(15));
            }
        };
        // Wait for at least one cloning heartbeat before transitioning phase so
        // slower macOS runners (`ps` process sampling) never race `drop(guard)`.
        wait_until(&|ev| ev.iter().any(|v| v["turn_state"] == "cloning"));
        guard.set_phase(Some("sess-9".into()), "in_turn");
        wait_until(&|ev| {
            ev.len() >= 2
                && ev
                    .iter()
                    .any(|v| v["turn_state"] == "in_turn" && v["session_id"] == "sess-9")
        });
        drop(guard);

        let count_after_drop = events.lock().unwrap().len();
        std::thread::sleep(Duration::from_millis(120));
        let count_later = events.lock().unwrap().len();
        stop_srv.store(true, Ordering::SeqCst);
        srv.join().unwrap();

        assert!(
            count_after_drop >= 2,
            "expected >= 2 heartbeats during blocking sleep, got {count_after_drop}"
        );
        assert_eq!(
            count_after_drop, count_later,
            "heartbeat guard must stop cleanly on drop with zero trailing ticks"
        );
        let recorded = events.lock().unwrap();
        assert!(recorded.iter().all(|v| v["event_type"] == "heartbeat"));
        assert!(recorded.iter().any(|v| v["turn_state"] == "cloning"));
        assert!(
            recorded
                .iter()
                .any(|v| v["turn_state"] == "in_turn" && v["session_id"] == "sess-9")
        );
    }

    #[test]
    fn watchdog_stops_heartbeats_on_silent_deadlock_and_respects_activity_and_max_duration() {
        assert_eq!(
            resolve_timeout_secs_from(None, 600),
            Some(Duration::from_secs(600))
        );
        assert_eq!(resolve_timeout_secs_from(Some("0"), 600), None);
        assert_eq!(
            resolve_timeout_secs_from(Some("120"), 600),
            Some(Duration::from_secs(120))
        );

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();
        let stop_srv = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop_srv_bg = Arc::clone(&stop_srv);
        let events = Arc::new(Mutex::new(Vec::<Value>::new()));
        let events_bg = Arc::clone(&events);

        let srv = std::thread::spawn(move || {
            while !stop_srv_bg.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let _ = stream.set_nonblocking(false);
                        let mut reader = BufReader::new(stream.try_clone().unwrap());
                        let mut content_len = 0usize;
                        let mut line = String::new();
                        loop {
                            line.clear();
                            let n = reader.read_line(&mut line).unwrap_or(0);
                            if n == 0 || line == "\r\n" || line == "\n" {
                                break;
                            }
                            if let Some(v) =
                                line.to_ascii_lowercase().strip_prefix("content-length:")
                            {
                                content_len = v.trim().parse().unwrap_or(0);
                            }
                        }
                        let mut body = vec![0u8; content_len];
                        if reader.read_exact(&mut body).is_ok()
                            && let Ok(val) = serde_json::from_slice::<Value>(&body)
                        {
                            events_bg.lock().unwrap().push(val);
                        }
                        let _ = stream.write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 11\r\n\r\n{\"ok\":true}",
                        );
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }
        });

        let dir = tempfile::tempdir().unwrap();
        let url = format!("http://{addr}/v1/internal/guest/events");
        let wd = TurnWatchdog::new(
            Some(Duration::from_millis(90)),
            Some(Duration::from_secs(30)),
        );
        let guard = HeartbeatGuard::start(
            &url,
            "tok_wd",
            "turn_wd_1",
            "agt_wd",
            "agy",
            None,
            "in_turn",
            Some(Duration::from_millis(30)),
            dir.path(),
            wd.clone(),
        );
        // Active touches keep the watchdog alive.
        std::thread::sleep(Duration::from_millis(50));
        wd.touch();
        assert!(wd.expired_reason(true, "agy").is_none());

        // Now go completely silent past silent_timeout while guard is STILL alive.
        std::thread::sleep(Duration::from_millis(180));
        let reason = wd
            .expired_reason(true, "agy")
            .expect("silent deadlock must trip watchdog");
        assert!(reason.contains("deadlock watchdog"), "{reason}");

        let count_after_expiry = events.lock().unwrap().len();
        std::thread::sleep(Duration::from_millis(120));
        let count_later = events.lock().unwrap().len();
        drop(guard);
        stop_srv.store(true, Ordering::SeqCst);
        srv.join().unwrap();

        assert_eq!(
            count_after_expiry, count_later,
            "heartbeat guard must stop posting once silent deadlock watchdog expires"
        );

        // Max turn duration enforces an absolute ceiling even when activity keeps arriving.
        let max_wd = TurnWatchdog::new(
            Some(Duration::from_secs(60)),
            Some(Duration::from_millis(40)),
        );
        std::thread::sleep(Duration::from_millis(60));
        max_wd.touch();
        let max_reason = max_wd
            .expired_reason(true, "agy")
            .expect("max turn duration must trip even with fresh touches");
        assert!(max_reason.contains("max turn duration"), "{max_reason}");
    }

    #[test]
    fn watchdog_resets_on_state_dir_tool_progress_and_expires_when_silent() {
        let dir = tempfile::tempdir().unwrap();
        let state_dir = dir.path();
        std::fs::create_dir_all(state_dir.join("turns")).unwrap();

        let agy_transcript = state_dir
            .join(".gemini/antigravity-cli/brain/cas_tool_loop/.system_generated/logs/transcript_full.jsonl");
        std::fs::create_dir_all(agy_transcript.parent().unwrap()).unwrap();
        std::fs::write(&agy_transcript, "{\"step\":1}\n").unwrap();

        let oc_wal = state_dir.join("data-home/opencode/opencode.db-wal");
        std::fs::create_dir_all(oc_wal.parent().unwrap()).unwrap();
        std::fs::write(&oc_wal, b"wal-init").unwrap();

        let mut tracker = StateDirProgressTracker::new(state_dir, Duration::from_millis(10));
        let wd = TurnWatchdog::new(
            Some(Duration::from_millis(110)),
            Some(Duration::from_secs(30)),
        );

        // Guest bookkeeping writes (heartbeats, session-id, turn logs, atomic temp files)
        // must NEVER look like harness tool progress.
        std::thread::sleep(Duration::from_millis(15));
        let snap = GuestActivitySnapshot::capture_current(
            "agy",
            Some("cas_tool_loop".into()),
            "in_turn",
            1,
            0,
        );
        write_activity_snapshot(state_dir, &snap).unwrap();
        std::fs::write(state_dir.join("agy-session-id"), "cas_tool_loop\n").unwrap();
        std::fs::write(state_dir.join("turns/turn_1.log"), "guest log line\n").unwrap();
        std::fs::write(
            state_dir.join(".guest-activity.json.1.1.anyagents-tmp"),
            b"tmp",
        )
        .unwrap();
        assert!(
            !tracker.poll_changed(),
            "guest bookkeeping writes must not reset silent-deadlock watchdog"
        );

        // Active multi-step tool-calling writes to deep agy transcript & opencode WAL
        // keep a healthy turn alive well past `silent_timeout` (110ms) with zero text/usage.
        for step in 2..=6u32 {
            std::thread::sleep(Duration::from_millis(35));
            if step % 2 == 0 {
                let mut f = OpenOptions::new()
                    .append(true)
                    .open(&agy_transcript)
                    .unwrap();
                writeln!(f, "{{\"step\":{step},\"tool\":\"run_command\"}}").unwrap();
            } else {
                let mut f = OpenOptions::new().append(true).open(&oc_wal).unwrap();
                writeln!(f, "wal-frame-{step}").unwrap();
            }
            assert!(
                tracker.poll_changed(),
                "step {step} transcript/WAL progress must be observed"
            );
            wd.touch();
            assert!(
                wd.expired_reason(true, "agy").is_none(),
                "healthy tool-calling progress at step {step} must keep watchdog alive"
            );
        }

        // Once tool progress stops, even continued heartbeat snapshot rewrites must NOT
        // prevent the silent-deadlock watchdog from expiring cleanly.
        for _ in 0..4 {
            std::thread::sleep(Duration::from_millis(35));
            let hb = GuestActivitySnapshot::capture_current(
                "agy",
                Some("cas_tool_loop".into()),
                "in_turn",
                1,
                0,
            );
            write_activity_snapshot(state_dir, &hb).unwrap();
            if tracker.poll_changed() {
                wd.touch();
            }
        }
        let reason = wd
            .expired_reason(true, "agy")
            .expect("truly silent deadlock must still expire watchdog");
        assert!(reason.contains("deadlock watchdog"), "{reason}");
    }
}
