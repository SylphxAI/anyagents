//! Detached serverless push-callback turn runner (`anyagents-guest turn`).
//!
//! Enables pure scale-to-zero coordinators:
//! - With `--detached`, daemonizes a child worker (`setsid`, stdout/stderr redirected to
//!   `<state-dir>/turns/<turn-id>.log`), prints `{"status":"accepted","turn_id":"...","pid":N}`,
//!   and exits `0` immediately (<100ms) so the coordinator HTTP request closes immediately.
//! - The worker drives any of the supported CLI harnesses (`claude`, `codex`, `opencode`,
//!   `gemini`, `omp`, `agy`, `muse`, `grok`, `dsh`, `spiron`) to completion, POSTing periodic
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
    load_guest_config, load_repo_spec, persist_session, read_persisted_session,
    resolve_default_state_dir, resolve_default_workdir, resolve_guest_program, signal_wake,
    turn_state_label, usage_json, wait_wake,
};

const POLL_TICK: Duration = Duration::from_millis(60);
const DEFAULT_MAX_RETRIES: usize = 5;
const DEFAULT_INITIAL_BACKOFF_MS: u64 = 200;
const CALLBACK_TIMEOUT_SECS: u64 = 15;
const HEARTBEAT_TIMEOUT_SECS: u64 = 5;

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
];
const MAX_SYNCABLE_AUTH_FILE_BYTES: usize = 64 * 1024;

/// CLI arguments for `anyagents-guest turn`.
#[derive(Args, Clone, Debug)]
pub struct TurnArgs {
    /// Daemonize into a detached background child, print accepted JSON, and exit immediately.
    #[arg(long)]
    pub detached: bool,
    /// Stable turn identifier assigned by the coordinator (`pending_remote_turns.turn_id`).
    #[arg(long)]
    pub turn_id: String,
    /// Target AnyAgents agent identifier (`agt_...`).
    #[arg(long)]
    pub agent_id: String,
    /// Target CLI harness (`claude`, `codex`, `opencode`, `gemini`, `omp`, `agy`, `muse`, `grok`, `dsh`, `spiron`).
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

    // Pass callback token via a 0600 file in `<state-dir>/turns/<turn-id>.token` rather
    // than exposing it on child argv (`/proc/<pid>/cmdline`).
    let token_file = turns_dir.join(format!("{}.token", args.turn_id));
    anyagents_harness::atomic_write_nonempty_file(
        &token_file,
        format!("{token}\n").as_bytes(),
        0o600,
    )
    .map_err(|e| format!("cannot persist turn callback token: {e}"))?;

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
    if !args.extra_args.is_empty() {
        cmd.arg("--");
        cmd.args(&args.extra_args);
    }

    cmd.stdin(Stdio::null())
        .stdout(Stdio::from(log_out))
        .stderr(Stdio::from(log_err));

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

    if let Some(spec) = file_cfg
        .as_ref()
        .and_then(|c| c.repo_spec.clone())
        .or_else(|| load_repo_spec(state_dir))
        && let Err(err) = ensure_repo_checked_out(&work_dir, &spec, args.git_program.as_deref())
    {
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
        .or_else(|| read_persisted_session(state_dir, &harness_name));
    let mut extra_args = file_cfg
        .as_ref()
        .map(|c| c.extra_args.clone())
        .unwrap_or_default();
    if !args.extra_args.is_empty() {
        extra_args = args.extra_args.clone();
    }

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

    let env = build_guest_env(state_dir, &work_dir, file_cfg.as_ref());
    let guest_exe =
        std::env::current_exe().unwrap_or_else(|_| PathBuf::from("/usr/local/bin/anyagents-guest"));
    let pre_turn_auth = snapshot_syncable_auth_files(state_dir);

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
        &guest_exe,
    ) {
        Ok(a) => a,
        Err(err) => {
            let msg = format!("failed to start guest harness `{harness_name}`: {err}");
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
        adapter.watch_activity(Arc::new(move || signal_wake(&w)));
    }

    let base_usage = adapter.served_usage();
    let base_turns = adapter.turns_served();

    // Emit initial active heartbeat immediately so coordinator records live PID + turn_state.
    let initial_sid = adapter.session_id().or(session);
    emit_heartbeat(
        callback_url,
        callback_token,
        &args.turn_id,
        &args.agent_id,
        &harness_name,
        initial_sid.clone(),
        "in_turn",
        1,
        state_dir,
    );

    let msg = OutboundMessage {
        id: args.turn_id.clone(),
        seq: 1,
        text: prompt.to_string(),
        sender: None,
        reply_to: None,
        source_time_ms: None,
        context: Context::default(),
    };

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
            .usage_for_id(&args.turn_id)
            .unwrap_or_else(|| adapter.served_usage().difference(base_usage));
        clear_activity_snapshot(state_dir);
        adapter.stop();
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

    let mut last_heartbeat = Instant::now();
    let mut sent_text_len = 0usize;
    let mut delta_seq = 0u64;
    let mut err_override: Option<String> = None;

    if !delivered_done {
        loop {
            let health = adapter.health();
            if let Some(sid) = adapter.session_id() {
                let _ = persist_session(state_dir, &harness_name, &sid);
            }

            // Stream incremental deltas if harness accumulated new output.
            if let Some(current_text) = adapter.answer_for(&args.turn_id, state_dir)
                && current_text.len() > sent_text_len
                && current_text.is_char_boundary(sent_text_len)
            {
                let chunk = &current_text[sent_text_len..];
                if !chunk.is_empty() {
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

            let proven = adapter.proven(&args.turn_id);
            let done_by_proof = matches!(
                proven,
                Some(Proof::Taken {
                    turn_finished: true
                }) | Some(Proof::Duplicate)
            );
            let done_by_turns =
                adapter.turns_served() > base_turns && health.turn_state == TurnState::Idle;
            let done_by_failure = adapter.failure_for_id(&args.turn_id).is_some();

            if done_by_proof || done_by_turns || done_by_failure || !health.process_alive {
                err_override = adapter.failure_for_id(&args.turn_id).or_else(|| {
                    (!health.process_alive && !done_by_proof && !done_by_turns).then(|| {
                        adapter
                            .exit_diagnostic()
                            .unwrap_or_else(|| "guest harness process exited mid-turn".into())
                    })
                });
                break;
            }

            if let Some(iv) = heartbeat_interval
                && last_heartbeat.elapsed() >= iv
            {
                emit_heartbeat(
                    callback_url,
                    callback_token,
                    &args.turn_id,
                    &args.agent_id,
                    &harness_name,
                    adapter.session_id().or_else(|| initial_sid.clone()),
                    turn_state_label(health.turn_state),
                    1,
                    state_dir,
                );
                last_heartbeat = Instant::now();
            }

            let wait_dur = match heartbeat_interval {
                Some(iv) => POLL_TICK.min(iv.max(Duration::from_millis(10))),
                None => POLL_TICK,
            };
            wait_wake(&wakeup, wait_dur);
        }
    }

    let sid = adapter
        .session_id()
        .or(initial_sid)
        .unwrap_or_else(|| format!("{harness_name}-guest-session"));
    let _ = persist_session(state_dir, &harness_name, &sid);

    let usage = adapter
        .usage_for_id(&args.turn_id)
        .unwrap_or_else(|| adapter.served_usage().difference(base_usage));
    let reply_opt = adapter.answer_for(&args.turn_id, state_dir);

    // Capture live descendant snapshot right before stopping adapter so coordinator
    // knows whether any background child subprocesses outlive the turn.
    let final_snap =
        GuestActivitySnapshot::capture_current(&harness_name, Some(sid.clone()), "idle", 0, 0);
    clear_activity_snapshot(state_dir);
    adapter.stop();
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

    let reply = reply_opt
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "completed".to_string());

    if reply.len() > sent_text_len && reply.is_char_boundary(sent_text_len) {
        let chunk = &reply[sent_text_len..];
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

    let audit_payload = json!({
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

fn encode_base64_standard(input: &[u8]) -> String {
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
}
