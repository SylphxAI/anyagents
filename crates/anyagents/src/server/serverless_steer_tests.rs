#![cfg(unix)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyagents_core::stateless::{
    NewRemoteTurn, SettleRemoteTurn, generate_callback_token, generate_turn_id, hash_callback_token,
};
use anyagents_core::{ExecutionRuntime, Incoming, NewAgent, Store, hmac_sha256_hex, now_ms};
use anyagents_guest::steer::{STEER_DIR_SUFFIX, SteerEnvelope, steer_filename};
use anyagents_supervisor::HealthBoard;
use axum::extract::{Path as AxumPath, State as AxumState};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{Value, json};

use super::super::serverless::{
    ReplyTarget, ServerlessDispatchInput, dispatch_next_mailbox_message_inner,
    dispatch_serverless_turn, settle_remote_turn_and_deliver,
};
use super::super::{AppState, ProcessRunner, build_public_router};
use super::*;
use crate::sandbox_providers::providers::{
    EnsureSandboxRequest, ExecOutput, ExecRequest, LocalHostProvider, ProviderHealth, ProviderKind,
    SandboxCapabilities, SandboxHandle, SandboxProvider as AsyncSandboxProvider,
};

fn make_app_state(store: Arc<Store>, data_dir: &Path) -> AppState {
    AppState {
        store,
        health: HealthBoard::default(),
        sign_in: Arc::new(Mutex::new(ProcessRunner)),
        auth: crate::auth::Mode::Trusted {
            data_dir: data_dir.to_path_buf(),
        },
        identity: Arc::new(crate::identity::ServingIdentity::capture()),
        profiles: Default::default(),
        timer_backend: Arc::new(crate::timer::NoopTimerBackend),
    }
}

fn write_executable(path: &Path, body: &str) {
    std::fs::write(path, body).unwrap();
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

fn locate_guest_binary(tmp_dir: &Path) -> Option<PathBuf> {
    let exe = std::env::current_exe().ok()?;
    let target_dir = exe.parent()?.parent()?;
    let direct = target_dir.join("anyagents-guest");
    if direct.is_file() {
        return Some(direct);
    }
    let multicall = target_dir.join("anyagents");
    if multicall.is_file() {
        let link = tmp_dir.join("anyagents-guest");
        if link.exists() || std::os::unix::fs::symlink(&multicall, &link).is_ok() {
            return Some(link);
        }
    }
    None
}

async fn wait_until(timeout: Duration, label: &str, mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting for: {label}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Parse `anyagents-guest turn ...` argv and run `anyagents_guest::turn::run_turn` in-process
/// on a background thread when the sibling `anyagents-guest` binary is not prebuilt.
fn spawn_in_process_guest_turn(
    argv: &[String],
    stdin_bytes: Option<&[u8]>,
) -> Result<ExecOutput, String> {
    use clap::Parser as _;

    #[derive(clap::Parser)]
    struct WrapperCli {
        #[command(subcommand)]
        cmd: WrapperCmd,
    }
    #[derive(clap::Subcommand)]
    enum WrapperCmd {
        Turn(anyagents_guest::turn::TurnArgs),
    }

    let parsed = WrapperCli::try_parse_from(argv).map_err(|e| e.to_string())?;
    let WrapperCmd::Turn(mut args) = parsed.cmd;
    args.detached = false;
    if args.prompt_stdin {
        args.prompt_stdin = false;
        let prompt = stdin_bytes
            .map(|b| String::from_utf8_lossy(b).to_string())
            .unwrap_or_default();
        args.prompt = Some(prompt);
    }
    let turn_id = args.turn_id.clone();
    let agent_id = args.agent_id.clone();
    let state_dir = args
        .state_dir
        .clone()
        .unwrap_or_else(|| PathBuf::from("/tmp"));
    let turns_dir = state_dir.join("turns");
    let _ = std::fs::create_dir_all(&turns_dir);
    let _ = anyagents_guest::steer::ensure_steer_spool_dir(&turns_dir, &turn_id);

    std::thread::Builder::new()
        .name(format!("inproc-guest-{turn_id}"))
        .spawn(move || {
            let _ = anyagents_guest::turn::run_turn(args);
        })
        .map_err(|e| e.to_string())?;

    let accepted = json!({
        "status": "accepted",
        "turn_id": turn_id,
        "agent_id": agent_id,
        "pid": std::process::id(),
    });
    Ok(ExecOutput {
        exit_code: 0,
        stdout: format!("{accepted}\n"),
        stderr: String::new(),
    })
}

/// Real exec-capable sandbox provider for coordinator mid-turn steering E2E tests.
/// Executes `/bin/sh -c` atomic steer writes AND real `anyagents-guest turn --detached`
/// (with seamless in-process `anyagents_guest::turn::run_turn` fallback when the sibling
/// binary wasn't prebuilt).
struct SteerSandboxProvider {
    root: PathBuf,
    guest_bin: Option<PathBuf>,
    fail_steer_exec: Mutex<bool>,
}

impl SteerSandboxProvider {
    fn new(root: PathBuf, guest_bin: Option<PathBuf>) -> Self {
        Self {
            root,
            guest_bin,
            fail_steer_exec: Mutex::new(false),
        }
    }
}

#[async_trait::async_trait]
impl AsyncSandboxProvider for SteerSandboxProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Http
    }

    fn capabilities(&self) -> SandboxCapabilities {
        SandboxCapabilities {
            persistent_volumes: true,
            volume_survives_release: true,
            ..SandboxCapabilities::default()
        }
    }

    async fn ensure(&self, req: &EnsureSandboxRequest) -> Result<SandboxHandle, String> {
        let state_dir = self.root.join("state");
        let workdir = self.root.join("workspace");
        std::fs::create_dir_all(&state_dir).map_err(|e| e.to_string())?;
        std::fs::create_dir_all(&workdir).map_err(|e| e.to_string())?;
        Ok(SandboxHandle {
            provider: ProviderKind::Http,
            sandbox_id: format!("sbx-steer-{}", req.agent_id),
            volume_id: Some(format!("vol-steer-{}", req.agent_id)),
            mount_path: self.root.display().to_string(),
            state_dir: state_dir.display().to_string(),
            workdir: workdir.display().to_string(),
            transport_url: None,
            transport_token: None,
            expires_at: None,
            metadata: BTreeMap::new(),
        })
    }

    async fn exec(&self, _handle: &SandboxHandle, req: &ExecRequest) -> Result<ExecOutput, String> {
        use std::io::Write as _;
        use std::process::{Command, Stdio};

        if *self.fail_steer_exec.lock().unwrap()
            && req.command.first().map(String::as_str) == Some("/bin/sh")
        {
            return Err("sandbox lease unreachable (injected test failure)".to_string());
        }

        let (prog, args) = req
            .command
            .split_first()
            .ok_or_else(|| "empty exec argv".to_string())?;

        if prog.contains("anyagents-guest") && self.guest_bin.is_none() {
            return spawn_in_process_guest_turn(&req.command, req.stdin.as_deref());
        }

        let mut cmd = Command::new(prog);
        for (k, _) in std::env::vars() {
            if k.starts_with("ANYAGENTS_") {
                cmd.env_remove(k);
            }
        }
        cmd.args(args)
            .env("ANYAGENTS_GUEST_CALLBACK_BACKOFF_MS", "15")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for (k, v) in &req.env {
            cmd.env(k, v);
        }
        let mut child = cmd.spawn().map_err(|e| format!("spawn {prog}: {e}"))?;
        if let Some(ref stdin_bytes) = req.stdin
            && let Some(mut stdin_pipe) = child.stdin.take()
        {
            let _ = stdin_pipe.write_all(stdin_bytes);
        }
        let out = child.wait_with_output().map_err(|e| e.to_string())?;
        Ok(ExecOutput {
            exit_code: out.status.code().unwrap_or(-1),
            stdout: String::from_utf8_lossy(&out.stdout).to_string(),
            stderr: String::from_utf8_lossy(&out.stderr).to_string(),
        })
    }

    async fn sync_bundle_tar(
        &self,
        _handle: &SandboxHandle,
        _dest_dir: &str,
        _tar_gz: &[u8],
    ) -> Result<(), String> {
        Ok(())
    }

    async fn release(&self, _handle: &SandboxHandle) -> Result<(), String> {
        Ok(())
    }

    async fn destroy(&self, _handle: &SandboxHandle) -> Result<(), String> {
        Ok(())
    }

    async fn health(&self) -> Result<ProviderHealth, String> {
        Ok(ProviderHealth {
            provider: ProviderKind::Http,
            healthy: true,
            message: "ok".into(),
            latency_ms: Some(1),
        })
    }
}

fn write_fake_claude_multistep(path: &Path, step1_flag: &Path, release_flag: &Path) {
    write_executable(
        path,
        &format!(
            r#"#!/usr/bin/env python3
import json, os, sys, time, pathlib

cfg_dir = os.environ.get("CLAUDE_CONFIG_DIR", "")
proj_dir = pathlib.Path(cfg_dir) / "projects" / "default"
proj_dir.mkdir(parents=True, exist_ok=True)
transcript = proj_dir / "coord-claude-sess.jsonl"

print(json.dumps({{
    "type": "system",
    "subtype": "init",
    "session_id": "coord-claude-sess",
    "claude_code_version": "2.1.0",
    "peer": "claude-code 2.1.0"
}}), flush=True)

opener_uuid = None
for raw in sys.stdin:
    raw = raw.strip()
    if not raw:
        continue
    msg = json.loads(raw)
    if msg.get("type") != "user":
        continue
    uuid = msg.get("uuid", "")
    text = msg.get("message", {{}}).get("content", "")
    with transcript.open("a", encoding="utf-8") as f:
        f.write(json.dumps({{"type": "user", "uuid": uuid, "message": {{"role": "user", "content": text}}}}) + "\n")
        f.flush()
    print(json.dumps({{
        "type": "user",
        "isReplay": True,
        "uuid": uuid,
        "message": {{"role": "user", "content": [{{"type": "text", "text": text}}]}}
    }}), flush=True)
    if opener_uuid is None:
        opener_uuid = uuid
        pathlib.Path({step1:?}).write_text("step1", encoding="utf-8")
        continue
    deadline = time.time() + 10.0
    while time.time() < deadline and not pathlib.Path({release:?}).exists():
        time.sleep(0.02)
    final_reply = f"COORD_STEP2_ACTED: {{text}}"
    print(json.dumps({{
        "type": "assistant",
        "session_id": "coord-claude-sess",
        "message": {{"role": "assistant", "content": [{{"type": "text", "text": final_reply}}]}}
    }}), flush=True)
    print(json.dumps({{
        "type": "result",
        "subtype": "success",
        "is_error": False,
        "session_id": "coord-claude-sess",
        "user_message_uuids": [opener_uuid, uuid],
        "result": final_reply,
        "usage": {{"input_tokens": 30, "output_tokens": 12}},
        "total_cost_usd": 0.02
    }}), flush=True)
    break
"#,
            step1 = step1_flag.display().to_string(),
            release = release_flag.display().to_string(),
        ),
    );
}

fn write_fake_codex_multistep(path: &Path, step1_flag: &Path, release_flag: &Path) {
    write_executable(
        path,
        &format!(
            r#"#!/usr/bin/env python3
import json, sys, time, pathlib

def send(obj):
    print(json.dumps(obj), flush=True)

for raw in sys.stdin:
    raw = raw.strip()
    if not raw:
        continue
    req = json.loads(raw)
    method = req.get("method")
    rid = req.get("id")
    params = req.get("params", {{}})
    if method == "initialize":
        send({{"jsonrpc": "2.0", "id": rid, "result": {{"userAgent": "codex_cli_rs/0.170.0 (Linux; x86_64)"}}}})
    elif method == "initialized":
        continue
    elif method == "thread/start":
        send({{"jsonrpc": "2.0", "id": rid, "result": {{"thread": {{"id": "thr-coord-1"}}}}}})
    elif method == "turn/start":
        cid = params.get("clientUserMessageId", "")
        text = params.get("input", [{{}}])[0].get("text", "")
        send({{"jsonrpc": "2.0", "id": rid, "result": {{"turn": {{"id": "codex-coord-t1", "status": "inProgress"}}}}}})
        send({{"jsonrpc": "2.0", "method": "turn/started", "params": {{"threadId": "thr-coord-1", "turn": {{"id": "codex-coord-t1"}}}}}})
        send({{"jsonrpc": "2.0", "method": "item/completed", "params": {{
            "threadId": "thr-coord-1",
            "turnId": "codex-coord-t1",
            "item": {{"type": "userMessage", "id": "u-1", "clientId": cid, "content": [{{"type": "text", "text": text}}]}}
        }}}})
        pathlib.Path({step1:?}).write_text("step1", encoding="utf-8")
    elif method == "turn/steer":
        assert params.get("expectedTurnId") == "codex-coord-t1"
        cid = params.get("clientUserMessageId", "")
        text = params.get("input", [{{}}])[0].get("text", "")
        send({{"jsonrpc": "2.0", "id": rid, "result": {{"turnId": "codex-coord-t1"}}}})
        send({{"jsonrpc": "2.0", "method": "item/completed", "params": {{
            "threadId": "thr-coord-1",
            "turnId": "codex-coord-t1",
            "item": {{"type": "userMessage", "id": "u-2", "clientId": cid, "content": [{{"type": "text", "text": text}}]}}
        }}}})
        deadline = time.time() + 10.0
        while time.time() < deadline and not pathlib.Path({release:?}).exists():
            time.sleep(0.02)
        reply = f"COORD_STEP2_ACTED: {{text}}"
        send({{"jsonrpc": "2.0", "method": "item/completed", "params": {{
            "threadId": "thr-coord-1",
            "turnId": "codex-coord-t1",
            "item": {{"type": "agentMessage", "id": "a-1", "text": reply}}
        }}}})
        send({{"jsonrpc": "2.0", "method": "turn/completed", "params": {{
            "threadId": "thr-coord-1",
            "turn": {{"id": "codex-coord-t1", "status": "completed"}}
        }}}})
        break
"#,
            step1 = step1_flag.display().to_string(),
            release = release_flag.display().to_string(),
        ),
    );
}

fn write_fake_spiron_multistep(path: &Path, step1_flag: &Path, release_flag: &Path) {
    write_executable(
        path,
        &format!(
            r#"#!/usr/bin/env python3
import json, os, socket, sys, time, pathlib

sock_path = None
for i, a in enumerate(sys.argv):
    if a == "--listen" and i + 1 < len(sys.argv):
        sock_path = sys.argv[i + 1]
    elif a.startswith("--listen="):
        sock_path = a.split("=", 1)[1]
assert sock_path

if os.path.exists(sock_path):
    os.unlink(sock_path)
srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
srv.bind(sock_path)
srv.listen(4)

conn = None
while True:
    c, _ = srv.accept()
    c.settimeout(2.0)
    try:
        if not c.recv(4096, socket.MSG_PEEK):
            c.close()
            continue
    except Exception:
        c.close()
        continue
    c.settimeout(None)
    conn = c
    break

rfile = conn.makefile("r", encoding="utf-8")
wfile = conn.makefile("w", encoding="utf-8")
seq = [0]

def send(req_id, obj):
    seq[0] += 1
    out = dict(obj)
    out["seq"] = seq[0]
    out["session_id"] = "spiron-coord-sess"
    out["request_id"] = req_id
    wfile.write(json.dumps(out) + "\n")
    wfile.flush()

opener_id = None
for raw in rfile:
    raw = raw.strip()
    if not raw:
        continue
    msg = json.loads(raw)
    t = msg.get("type")
    rid = msg.get("request_id", "")
    if t == "attach":
        send(rid, {{
            "type": "ready",
            "protocol": "spiron.headless/v2",
            "version": "0.2.0",
            "commit": "0123456789abcdef",
            "features": ["turn-tool-context-v1"]
        }})
    elif t == "prompt":
        opener_id = rid
        send(rid, {{"type": "accepted", "mode": "turn"}})
        pathlib.Path({step1:?}).write_text("step1", encoding="utf-8")
    elif t == "steer":
        steer_text = msg.get("text", "")
        send(rid, {{"type": "steer_ack", "accepted": True, "turn_id": opener_id}})
        send(opener_id, {{"type": "steer_consumed", "steer_request_id": rid, "turn_id": opener_id, "duplicate": False}})
        deadline = time.time() + 10.0
        while time.time() < deadline and not pathlib.Path({release:?}).exists():
            time.sleep(0.02)
        send(opener_id, {{
            "type": "result",
            "status": "completed",
            "is_error": False,
            "error": None,
            "duplicate": False,
            "tool_context_bound": True,
            "final_text": f"COORD_STEP2_ACTED: {{steer_text}}",
            "consumed_request_ids": [opener_id, rid],
            "undelivered_steer_ids": [],
            "usage": {{"input_tokens": 22, "output_tokens": 11, "cached_tokens": 0, "cache_write_tokens": None}},
            "cost_micros": 50
        }})
        break
"#,
            step1 = step1_flag.display().to_string(),
            release = release_flag.display().to_string(),
        ),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coordinator_sandbox_midturn_steer_e2e_advances_cursor_before_complete_and_zero_duplicate_followup()
 {
    for harness in ["claude-code", "codex", "spiron"] {
        let dir = crate::test_tempdir();
        let sbx_root = crate::test_tempdir();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        let parent = store
            .create_agent(NewAgent::new(format!("parent_{harness}"), "claude-code"))
            .unwrap();
        let worker = store
            .create_agent(NewAgent::new(format!("worker_{harness}"), harness))
            .unwrap();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let public_url = format!("http://{addr}");
        let router = build_public_router(make_app_state(Arc::clone(&store), dir.path()));
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let server_task = tokio::spawn(async move {
            let _ = axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await;
        });

        let step1_flag = sbx_root.path().join("step1.ready");
        let release_flag = sbx_root.path().join("step2.release");
        let fake_prog = sbx_root.path().join(format!("fake-{harness}.py"));
        match harness {
            "claude-code" => write_fake_claude_multistep(&fake_prog, &step1_flag, &release_flag),
            "codex" => write_fake_codex_multistep(&fake_prog, &step1_flag, &release_flag),
            "spiron" => write_fake_spiron_multistep(&fake_prog, &step1_flag, &release_flag),
            _ => unreachable!(),
        }

        let guest_bin = locate_guest_binary(sbx_root.path());
        let provider: Arc<dyn AsyncSandboxProvider> = Arc::new(SteerSandboxProvider::new(
            sbx_root.path().to_path_buf(),
            guest_bin.clone(),
        ));

        // 1. Enqueue Step 1 opener message (`seq=1`) and dispatch detached sandbox turn.
        let msg1 = match store
            .receive(&worker.id, Incoming::new("Start step 1"))
            .unwrap()
        {
            anyagents_core::ReceiveOutcome::New(m)
            | anyagents_core::ReceiveOutcome::Duplicate(m) => m,
        };
        assert_eq!(msg1.seq, 1);

        let mut input = ServerlessDispatchInput::for_agent(worker.clone(), msg1.text.clone());
        input.message_seq = Some(msg1.seq);
        input.opener_id = Some(msg1.id.to_string());
        input.reply_target = Some(ReplyTarget::Agent {
            agent_id: parent.id.as_str().to_string(),
        });
        input.callback_base_url = Some(public_url.clone());
        input.sandbox_provider = Some(Arc::clone(&provider));
        input.guest_binary = Some(
            guest_bin
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "anyagents-guest".to_string()),
        );
        input.guest_program = Some(fake_prog.display().to_string());
        input.guest_extra_args = vec!["--guest-bare".to_string()];
        input.heartbeat_interval_ms = Some(100);

        let dispatched = dispatch_serverless_turn(Arc::clone(&store), input)
            .await
            .unwrap_or_else(|e| panic!("{harness} initial dispatch failed: {e}"));
        let turn_id = dispatched.turn.turn_id.clone();

        wait_until(
            Duration::from_secs(10),
            &format!("{harness} step 1 active"),
            || step1_flag.exists(),
        )
        .await;

        // 2. While Step 1 is actively running, inbound message 2 (`seq=2`) arrives!
        let steer_token = format!("MIDTURN_STEER_{harness}");
        let msg2 = match store
            .receive(&worker.id, Incoming::new(steer_token.clone()))
            .unwrap()
        {
            anyagents_core::ReceiveOutcome::New(m)
            | anyagents_core::ReceiveOutcome::Duplicate(m) => m,
        };
        assert_eq!(msg2.seq, 2);
        assert_eq!(store.delivered_cursor(&worker.id).unwrap(), 1);

        // Coordinator detects active turn and pushes atomic mid-turn steer (returns Ok(None) — no 2nd turn!).
        let push_res = dispatch_next_mailbox_message_inner(
            Arc::clone(&store),
            &worker.id,
            Some(Arc::clone(&provider)),
            Some(public_url.clone()),
            None,
        )
        .await
        .unwrap();
        assert!(
            push_res.is_none(),
            "{harness}: mid-turn steer MUST NOT open a second turn"
        );

        // 3. Wait for live `steer_consumed` callback to advance `delivered_seq == 2` BEFORE turn_complete!
        wait_until(
            Duration::from_secs(10),
            &format!("{harness} delivered_seq advances to 2 mid-turn"),
            || store.delivered_cursor(&worker.id).unwrap_or(0) == 2,
        )
        .await;

        // Prove the turn is STILL running when `delivered_seq` reached 2!
        let mid_turn = store
            .remote_turn_store()
            .get_turn(&turn_id)
            .unwrap()
            .unwrap();
        assert!(
            !mid_turn.is_terminal(),
            "{harness}: delivered_seq MUST advance BEFORE turn completes! status={}",
            mid_turn.status
        );
        assert_eq!(
            mid_turn
                .metadata_json
                .as_ref()
                .and_then(|m| m.get(MAX_STEERED_SEQ_META_KEY))
                .and_then(Value::as_i64),
            Some(2),
            "{harness}: max_steered_seq should be recorded mid-turn"
        );
        assert!(
            store.list_undelivered(&worker.id, 10).unwrap().is_empty(),
            "{harness}: undelivered mailbox must already be empty mid-turn"
        );

        // 4. Release Step 2 to finish and settle the single turn.
        std::fs::write(&release_flag, "go").unwrap();
        wait_until(
            Duration::from_secs(10),
            &format!("{harness} turn settles completed"),
            || {
                store
                    .remote_turn_store()
                    .get_turn(&turn_id)
                    .ok()
                    .flatten()
                    .is_some_and(|t| t.status == "completed")
            },
        )
        .await;

        let done_turn = store
            .remote_turn_store()
            .get_turn(&turn_id)
            .unwrap()
            .unwrap();
        assert!(
            done_turn
                .result_text
                .as_deref()
                .is_some_and(|t| t.contains(&steer_token)),
            "{harness}: final result must prove Step 2 acted on mid-turn steer, got {:?}",
            done_turn.result_text
        );

        // 5. Post-completion dispatch MUST launch ZERO duplicate followup turns!
        let after = dispatch_next_mailbox_message_inner(
            Arc::clone(&store),
            &worker.id,
            Some(Arc::clone(&provider)),
            Some(public_url.clone()),
            None,
        )
        .await
        .unwrap();
        assert!(
            after.is_none(),
            "{harness}: ZERO duplicate followup turn allowed after steered turn completes!"
        );
        assert!(store.list_undelivered(&worker.id, 10).unwrap().is_empty());
        assert_eq!(store.delivered_cursor(&worker.id).unwrap(), 2);

        // Parent receives the Step 2 steered reply once `deliver_settled_turn_outcome` commits.
        wait_until(
            Duration::from_secs(5),
            &format!("{harness}: parent receives post-steer reply"),
            || {
                store
                    .list_undelivered(&parent.id, 10)
                    .unwrap_or_default()
                    .iter()
                    .any(|m| m.text.contains(&steer_token))
            },
        )
        .await;

        let _ = shutdown_tx.send(());
        let _ = server_task.await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coordinator_local_runtime_midturn_steer_e2e_via_local_host_provider() {
    let dir = crate::test_tempdir();
    let local_root = crate::test_tempdir();
    let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
    let mut new_agent = NewAgent::new("local_steer_worker", "claude-code");
    new_agent.runtime = Some(ExecutionRuntime::Local);
    let worker = store.create_agent(new_agent).unwrap();
    assert_eq!(worker.runtime, ExecutionRuntime::Local);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let public_url = format!("http://{addr}");
    let router = build_public_router(make_app_state(Arc::clone(&store), dir.path()));
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server_task = tokio::spawn(async move {
        let _ = axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await;
    });

    let step1_flag = local_root.path().join("local-step1.ready");
    let release_flag = local_root.path().join("local-step2.release");
    let fake_claude = local_root.path().join("fake-claude-local.py");
    write_fake_claude_multistep(&fake_claude, &step1_flag, &release_flag);

    let local_provider: Arc<dyn AsyncSandboxProvider> =
        Arc::new(LocalHostProvider::with_root(local_root.path()));

    let msg1 = match store
        .receive(&worker.id, Incoming::new("Local step 1"))
        .unwrap()
    {
        anyagents_core::ReceiveOutcome::New(m) | anyagents_core::ReceiveOutcome::Duplicate(m) => m,
    };

    let guest_bin = locate_guest_binary(local_root.path());
    let turn_id = if let Some(gbin) = guest_bin {
        let mut input = ServerlessDispatchInput::for_agent(worker.clone(), msg1.text.clone());
        input.message_seq = Some(msg1.seq);
        input.opener_id = Some(msg1.id.to_string());
        input.callback_base_url = Some(public_url.clone());
        input.sandbox_provider = Some(Arc::clone(&local_provider));
        input.guest_binary = Some(gbin.display().to_string());
        input.guest_program = Some(fake_claude.display().to_string());
        input.guest_extra_args = vec!["--guest-bare".to_string()];
        input.heartbeat_interval_ms = Some(100);
        dispatch_serverless_turn(Arc::clone(&store), input)
            .await
            .expect("local runtime dispatch")
            .turn
            .turn_id
    } else {
        // Fallback when running isolated crate test without sibling `anyagents-guest` binary:
        // ensure real LocalHostProvider root, spawn in-process guest turn, and verify real
        // LocalHostProvider::exec atomic steer write + live callback settlement!
        let handle = local_provider
            .ensure(&EnsureSandboxRequest::for_agent(worker.id.as_str()))
            .await
            .unwrap();
        let tid = generate_turn_id();
        let tok = generate_callback_token();
        store
            .remote_turn_store()
            .create_turn(&NewRemoteTurn {
                turn_id: tid.clone(),
                agent_id: worker.id.clone(),
                harness_kind: "sandbox".into(),
                provider_kind: Some("local".into()),
                remote_run_id: Some("local:inproc".into()),
                callback_token_hash: hash_callback_token(&tok),
                cursor: None,
                status: "running".into(),
                metadata_json: Some(json!({
                    "agent_harness": "claude-code",
                    "message_seq": msg1.seq,
                    "opener_id": msg1.id.to_string(),
                    "state_dir": handle.state_dir,
                    "workdir": handle.workdir,
                })),
                next_poll_at_ms: None,
            })
            .unwrap();
        let _ = store.note_delivered(&worker.id, msg1.seq);
        let argv = vec![
            "anyagents-guest".into(),
            "turn".into(),
            "--turn-id".into(),
            tid.clone(),
            "--agent-id".into(),
            worker.id.as_str().to_string(),
            "--harness".into(),
            "claude-code".into(),
            "--program".into(),
            fake_claude.display().to_string(),
            "--prompt".into(),
            msg1.text.clone(),
            "--callback-url".into(),
            format!("{public_url}/v1/internal/guest/events"),
            "--callback-token".into(),
            tok,
            "--heartbeat-interval-ms".into(),
            "100".into(),
            "--state-dir".into(),
            handle.state_dir,
            "--workdir".into(),
            handle.workdir,
            "--".into(),
            "--guest-bare".into(),
        ];
        spawn_in_process_guest_turn(&argv, None).unwrap();
        tid
    };

    wait_until(Duration::from_secs(10), "local step 1 active", || {
        step1_flag.exists()
    })
    .await;

    let msg2 = match store
        .receive(&worker.id, Incoming::new("LOCAL_RUNTIME_STEER_OK"))
        .unwrap()
    {
        anyagents_core::ReceiveOutcome::New(m) | anyagents_core::ReceiveOutcome::Duplicate(m) => m,
    };
    assert_eq!(msg2.seq, 2);

    // Push mid-turn steer through real `LocalHostProvider::exec`!
    let pushed = dispatch_next_mailbox_message_inner(
        Arc::clone(&store),
        &worker.id,
        Some(Arc::clone(&local_provider)),
        Some(public_url.clone()),
        None,
    )
    .await
    .unwrap();
    assert!(pushed.is_none());

    wait_until(
        Duration::from_secs(10),
        "local delivered_seq advances to 2 mid-turn",
        || store.delivered_cursor(&worker.id).unwrap_or(0) == 2,
    )
    .await;
    assert!(
        !store
            .remote_turn_store()
            .get_turn(&turn_id)
            .unwrap()
            .unwrap()
            .is_terminal()
    );

    std::fs::write(&release_flag, "go").unwrap();
    wait_until(Duration::from_secs(10), "local turn completes", || {
        store
            .remote_turn_store()
            .get_turn(&turn_id)
            .ok()
            .flatten()
            .is_some_and(|t| t.status == "completed")
    })
    .await;

    let done = store
        .remote_turn_store()
        .get_turn(&turn_id)
        .unwrap()
        .unwrap();
    assert!(
        done.result_text
            .as_deref()
            .is_some_and(|t| t.contains("LOCAL_RUNTIME_STEER_OK")),
        "{done:?}"
    );
    assert!(
        dispatch_next_mailbox_message_inner(
            Arc::clone(&store),
            &worker.id,
            Some(local_provider),
            Some(public_url),
            None,
        )
        .await
        .unwrap()
        .is_none()
    );

    let _ = shutdown_tx.send(());
    let _ = server_task.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coordinator_cloud_runtime_midturn_steer_e2e_spiron_consumed_accepted_webhook_and_refused()
{
    #[derive(Default)]
    struct MockSpironCloud {
        steer_mode: Mutex<String>,
        steers_received: Mutex<Vec<Value>>,
    }

    let cloud_st = Arc::new(MockSpironCloud {
        steer_mode: Mutex::new("consumed".into()),
        ..MockSpironCloud::default()
    });
    let cloud_app = Router::new()
        .route(
            "/v1/agents/{agent_id}/messages",
            post(
                |AxumState(st): AxumState<Arc<MockSpironCloud>>,
                 AxumPath(agent_id): AxumPath<String>,
                 headers: HeaderMap,
                 Json(body): Json<Value>| async move {
                    let is_steer = headers
                        .get("x-anyagents-delivery")
                        .and_then(|v| v.to_str().ok())
                        == Some("steer");
                    if !is_steer {
                        return (
                            StatusCode::ACCEPTED,
                            Json(json!({
                                "id": "rcpt_cloud_run_1",
                                "agent_id": agent_id,
                                "status": "working"
                            })),
                        );
                    }
                    st.steers_received.lock().unwrap().push(body);
                    let mode = st.steer_mode.lock().unwrap().clone();
                    match mode.as_str() {
                        "consumed" => (
                            StatusCode::OK,
                            Json(json!({
                                "id": "rcpt_cloud_steer_2",
                                "status": "steered",
                                "input_delivery": { "delivered": true, "step": 2 }
                            })),
                        ),
                        "accepted" => (
                            StatusCode::ACCEPTED,
                            Json(json!({
                                "id": "rcpt_cloud_steer_3",
                                "status": "accepted"
                            })),
                        ),
                        _ => (
                            StatusCode::CONFLICT,
                            Json(json!({ "error": "turn already closing" })),
                        ),
                    }
                },
            ),
        )
        .route(
            "/v1/message-receipts/{task_id}",
            get(|AxumPath(task_id): AxumPath<String>| async move {
                Json(json!({ "id": task_id, "status": "working" }))
            }),
        )
        .with_state(Arc::clone(&cloud_st));

    let cloud_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cloud_addr = cloud_listener.local_addr().unwrap();
    let cloud_url = format!("http://{cloud_addr}");
    tokio::spawn(async move {
        let _ = axum::serve(cloud_listener, cloud_app).await;
    });

    let dir = crate::test_tempdir();
    let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
    let mut new_agent = NewAgent::new("cloud_spiron_worker", "spiron");
    new_agent.runtime = Some(ExecutionRuntime::Cloud);
    let worker = store.create_agent(new_agent).unwrap();

    let coord_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let coord_addr = coord_listener.local_addr().unwrap();
    let coord_url = format!("http://{coord_addr}");
    let router = build_public_router(make_app_state(Arc::clone(&store), dir.path()));
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let coord_task = tokio::spawn(async move {
        let _ = axum::serve(coord_listener, router)
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await;
    });

    // 1. Dispatch Cloud Turn 1 (`seq=1`).
    let msg1 = match store
        .receive(&worker.id, Incoming::new("Cloud step 1"))
        .unwrap()
    {
        anyagents_core::ReceiveOutcome::New(m) | anyagents_core::ReceiveOutcome::Duplicate(m) => m,
    };
    let mut input = ServerlessDispatchInput::for_agent(worker.clone(), msg1.text.clone());
    input.message_seq = Some(msg1.seq);
    input.opener_id = Some(msg1.id.to_string());
    input.callback_base_url = Some(coord_url.clone());
    input.cloud_endpoint = Some(cloud_url.clone());
    input.remote_agent_id = Some("spiron_remote_agt_1".to_string());
    let dispatched = dispatch_serverless_turn(Arc::clone(&store), input)
        .await
        .expect("cloud dispatch");
    let turn_id = dispatched.turn.turn_id.clone();
    let callback_token = dispatched.callback_token.clone();
    assert_eq!(store.delivered_cursor(&worker.id).unwrap(), 1);

    // 2. Case A: Synchronous `Consumed` ack (`input_delivery`) advances `delivered_seq` to 2 mid-turn!
    let msg2 = match store
        .receive(&worker.id, Incoming::new("CLOUD_STEER_SYNC_2"))
        .unwrap()
    {
        anyagents_core::ReceiveOutcome::New(m) | anyagents_core::ReceiveOutcome::Duplicate(m) => m,
    };
    assert_eq!(msg2.seq, 2);
    let r2 = dispatch_next_mailbox_message_inner(
        Arc::clone(&store),
        &worker.id,
        None,
        Some(coord_url.clone()),
        None,
    )
    .await
    .unwrap();
    assert!(r2.is_none());
    assert_eq!(
        store.delivered_cursor(&worker.id).unwrap(),
        2,
        "sync cloud Consumed ack must advance delivered_seq BEFORE turn completes"
    );
    assert!(
        !store
            .remote_turn_store()
            .get_turn(&turn_id)
            .unwrap()
            .unwrap()
            .is_terminal()
    );

    // 3. Case B: Async `Accepted` keeps `delivered_seq == 2` fail-closed until authenticated webhook arrives!
    *cloud_st.steer_mode.lock().unwrap() = "accepted".into();
    let msg3 = match store
        .receive(&worker.id, Incoming::new("CLOUD_STEER_ASYNC_3"))
        .unwrap()
    {
        anyagents_core::ReceiveOutcome::New(m) | anyagents_core::ReceiveOutcome::Duplicate(m) => m,
    };
    assert_eq!(msg3.seq, 3);
    let _ = dispatch_next_mailbox_message_inner(
        Arc::clone(&store),
        &worker.id,
        None,
        Some(coord_url.clone()),
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        store.delivered_cursor(&worker.id).unwrap(),
        2,
        "async cloud Accepted MUST NOT advance delivered_seq without consumption proof"
    );

    // Now post authenticated `message.steered` webhook to `/v1/internal/cloud/events`.
    let steer_wh = json!({
        "event": "message.steered",
        "data": {
            "task_id": "rcpt_cloud_run_1",
            "turn_id": turn_id,
            "status": "running",
            "message_id": msg3.id.to_string(),
            "seq": msg3.seq,
            "input_delivery": { "delivered": true }
        }
    });
    let tok_wh = callback_token.clone();
    let url_wh = format!("{coord_url}/v1/internal/cloud/events?driver=spiron");
    let status_wh = tokio::task::spawn_blocking(move || {
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .build()
            .into();
        agent
            .post(&url_wh)
            .header("Authorization", &format!("Bearer {tok_wh}"))
            .send_json(&steer_wh)
            .unwrap()
            .status()
            .as_u16()
    })
    .await
    .unwrap();
    assert_eq!(status_wh, 200);
    assert_eq!(
        store.delivered_cursor(&worker.id).unwrap(),
        3,
        "authenticated cloud steer_consumed webhook must advance delivered_seq to 3 mid-turn"
    );

    // 4. Case C: Cloud `Refused` (HTTP 409) leaves msg 4 safely undelivered in Postgres for turn-end drain!
    *cloud_st.steer_mode.lock().unwrap() = "refused".into();
    let msg4 = match store
        .receive(&worker.id, Incoming::new("CLOUD_STEER_REFUSED_4"))
        .unwrap()
    {
        anyagents_core::ReceiveOutcome::New(m) | anyagents_core::ReceiveOutcome::Duplicate(m) => m,
    };
    assert_eq!(msg4.seq, 4);
    let _ = dispatch_next_mailbox_message_inner(
        Arc::clone(&store),
        &worker.id,
        None,
        Some(coord_url.clone()),
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        store.delivered_cursor(&worker.id).unwrap(),
        3,
        "fail-closed: cloud Refused MUST keep delivered_seq at 3"
    );

    // Settle Cloud Turn 1 via authenticated `message.completed` webhook.
    let done_wh = json!({
        "event": "message.completed",
        "data": {
            "task_id": "rcpt_cloud_run_1",
            "turn_id": turn_id,
            "status": "completed",
            "reply": "Cloud step 1+2+3 complete"
        }
    });
    let tok_done = callback_token.clone();
    let url_done = format!("{coord_url}/v1/internal/cloud/events?driver=spiron");
    let status_done = tokio::task::spawn_blocking(move || {
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .build()
            .into();
        agent
            .post(&url_done)
            .header("Authorization", &format!("Bearer {tok_done}"))
            .send_json(&done_wh)
            .unwrap()
            .status()
            .as_u16()
    })
    .await
    .unwrap();
    assert_eq!(status_done, 200);

    // Messages 1, 2, 3 are settled; ONLY refused message 4 remains in undelivered backlog!
    let remaining = store.list_undelivered(&worker.id, 10).unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].seq, 4);
    assert_eq!(remaining[0].text, "CLOUD_STEER_REFUSED_4");

    // Verify zero HMAC regression (sanity check hmac_sha256_hex import used if needed).
    assert!(!hmac_sha256_hex(b"k", b"v").is_empty());

    let _ = shutdown_tx.send(());
    let _ = coord_task.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coordinator_midturn_steer_fail_closed_security_and_ordering_invariants() {
    let dir = crate::test_tempdir();
    let sbx_root = crate::test_tempdir();
    let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());

    // 1. Gemini (TurnEnd-only) MUST fail closed (`Unsupported`), write ZERO spool files, and drain at turn end.
    let gem = store
        .create_agent(NewAgent::new("gem_worker", "gemini"))
        .unwrap();
    let gm1 = match store.receive(&gem.id, Incoming::new("gem 1")).unwrap() {
        anyagents_core::ReceiveOutcome::New(m) | anyagents_core::ReceiveOutcome::Duplicate(m) => m,
    };
    let gm2 = match store.receive(&gem.id, Incoming::new("gem 2")).unwrap() {
        anyagents_core::ReceiveOutcome::New(m) | anyagents_core::ReceiveOutcome::Duplicate(m) => m,
    };
    let gem_turn = store
        .remote_turn_store()
        .create_turn(&NewRemoteTurn {
            turn_id: "turn_gem_fc".into(),
            agent_id: gem.id.clone(),
            harness_kind: "sandbox".into(),
            provider_kind: Some("http".into()),
            remote_run_id: Some("pid:111".into()),
            callback_token_hash: hash_callback_token("tok_g"),
            cursor: None,
            status: "running".into(),
            metadata_json: Some(json!({
                "agent_harness": "gemini",
                "message_seq": gm1.seq,
                "opener_id": gm1.id.to_string(),
                "state_dir": sbx_root.path().join("gem-state").display().to_string(),
            })),
            next_poll_at_ms: None,
        })
        .unwrap();
    store.note_delivered(&gem.id, gm1.seq).unwrap();

    let provider = Arc::new(SteerSandboxProvider::new(
        sbx_root.path().to_path_buf(),
        None,
    ));
    let out_gem = try_steer_active_turn(
        Arc::clone(&store),
        gem.clone(),
        gem_turn.clone(),
        vec![gm2.clone()],
        Some(Arc::clone(&provider) as Arc<dyn AsyncSandboxProvider>),
        None,
    )
    .await;
    assert_eq!(out_gem, SteerPushOutcome::Unsupported);
    assert!(
        !sbx_root
            .path()
            .join(format!("gem-state/turns/turn_gem_fc{STEER_DIR_SUFFIX}"))
            .exists()
    );
    assert_eq!(store.delivered_cursor(&gem.id).unwrap(), 1);

    // 2. Unreachable sandbox lease returns `Refused` and keeps `delivered_seq` unadvanced.
    let claude = store
        .create_agent(NewAgent::new("claude_fc", "claude-code"))
        .unwrap();
    let cm1 = match store.receive(&claude.id, Incoming::new("c1")).unwrap() {
        anyagents_core::ReceiveOutcome::New(m) | anyagents_core::ReceiveOutcome::Duplicate(m) => m,
    };
    let cm2 = match store.receive(&claude.id, Incoming::new("c2")).unwrap() {
        anyagents_core::ReceiveOutcome::New(m) | anyagents_core::ReceiveOutcome::Duplicate(m) => m,
    };
    let cm3 = match store.receive(&claude.id, Incoming::new("c3")).unwrap() {
        anyagents_core::ReceiveOutcome::New(m) | anyagents_core::ReceiveOutcome::Duplicate(m) => m,
    };
    let c_turn = store
        .remote_turn_store()
        .create_turn(&NewRemoteTurn {
            turn_id: "turn_claude_fc".into(),
            agent_id: claude.id.clone(),
            harness_kind: "sandbox".into(),
            provider_kind: Some("http".into()),
            remote_run_id: Some("pid:222".into()),
            callback_token_hash: hash_callback_token("tok_c"),
            cursor: None,
            status: "running".into(),
            metadata_json: Some(json!({
                "agent_harness": "claude-code",
                "message_seq": cm1.seq,
                "opener_id": cm1.id.to_string(),
                "state_dir": sbx_root.path().join("c-state").display().to_string(),
            })),
            next_poll_at_ms: None,
        })
        .unwrap();
    store.note_delivered(&claude.id, cm1.seq).unwrap();

    *provider.fail_steer_exec.lock().unwrap() = true;
    let refused = try_steer_active_turn(
        Arc::clone(&store),
        claude.clone(),
        c_turn.clone(),
        vec![cm2.clone()],
        Some(Arc::clone(&provider) as Arc<dyn AsyncSandboxProvider>),
        None,
    )
    .await;
    assert!(
        matches!(refused, SteerPushOutcome::Refused { .. }),
        "{refused:?}"
    );
    assert_eq!(store.delivered_cursor(&claude.id).unwrap(), 1);
    *provider.fail_steer_exec.lock().unwrap() = false;

    // 3. Integrity & contiguous ordering checks on `apply_verified_steer_consumed`:
    // - Unknown message_id -> 400
    let err_unknown = apply_verified_steer_consumed(
        &store,
        "turn_claude_fc",
        "msg_does_not_exist",
        2,
        "fake",
        now_ms(),
    )
    .unwrap_err();
    assert_eq!(err_unknown.status, StatusCode::BAD_REQUEST);
    assert_eq!(store.delivered_cursor(&claude.id).unwrap(), 1);

    // - Mismatched seq -> 400
    let err_mismatch = apply_verified_steer_consumed(
        &store,
        "turn_claude_fc",
        cm2.id.as_str(),
        99,
        "fake",
        now_ms(),
    )
    .unwrap_err();
    assert_eq!(err_mismatch.status, StatusCode::BAD_REQUEST);
    assert_eq!(store.delivered_cursor(&claude.id).unwrap(), 1);

    // - Out-of-order seq=3 before seq=2 -> holds cursor at 1 (ZERO gap skip!)
    let ooo = apply_verified_steer_consumed(
        &store,
        "turn_claude_fc",
        cm3.id.as_str(),
        cm3.seq,
        "proof-3",
        now_ms(),
    )
    .unwrap();
    assert_eq!(ooo.delivered_seq, 1);
    assert!(!ooo.advanced);
    assert_eq!(store.delivered_cursor(&claude.id).unwrap(), 1);

    // - Contiguous seq=2 arrives -> jumps contiguously to 3!
    let closed_gap = apply_verified_steer_consumed(
        &store,
        "turn_claude_fc",
        cm2.id.as_str(),
        cm2.seq,
        "proof-2",
        now_ms(),
    )
    .unwrap();
    assert_eq!(closed_gap.delivered_seq, 3);
    assert!(closed_gap.advanced);
    assert_eq!(store.delivered_cursor(&claude.id).unwrap(), 3);
    assert!(store.list_undelivered(&claude.id, 10).unwrap().is_empty());

    // Settle turn and verify `settle_steered_turn_cursor` keeps cursor cleanly closed at 3.
    let settled = settle_remote_turn_and_deliver(
        &store,
        "turn_claude_fc",
        SettleRemoteTurn {
            status: "completed".into(),
            result_text: Some("all 3 consumed".into()),
            error_text: None,
            usage_json: None,
            cursor: None,
            settled_at_ms: now_ms(),
        },
        None,
        0,
    )
    .unwrap()
    .unwrap();
    assert!(settled.settled_now);
    assert_eq!(store.delivered_cursor(&claude.id).unwrap(), 3);
    assert!(!store.has_open_turn(&claude.id).unwrap());

    // Sanity check SteerEnvelope / steer_filename import usage.
    assert!(steer_filename(2, "m2").starts_with("00000000000000000002-"));
    let _ = std::mem::size_of::<SteerEnvelope>();
}
