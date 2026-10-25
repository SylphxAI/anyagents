//! DeepSeek Harness (`dsh`) native Web/API daemon adapter (`dsh --profile web --no-open --port 0`).
//!
//! Connects to `dsh`'s built-in `SessionController` HTTP gateway (`127.0.0.1:<port>`),
//! authenticates with its one-time launch token (`/?token=...` -> `dsh-auth-*` cookie),
//! creates/resumes the session via `session/create`, and delivers both initial prompts
//! and mid-turn steering via `session/prompt` with `mode: "steer"` (`agent.steer(msg)`).

use std::collections::HashSet;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Stdio};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use anyagents_harness::{
    DeliverOutcome, DeliveryMode, Harness, HarnessError, HarnessHealth, OutboundMessage,
    SteerSupport, TurnState, TurnUsage, compose_message, marked_message, spawn_command,
};
use serde_json::{Value, json};

pub struct DshHarness {
    environment: Vec<(String, String)>,
    work_dir: PathBuf,
    shared: Arc<Mutex<State>>,
    wake: Arc<Condvar>,
    reader: Option<JoinHandle<()>>,
    poller: Option<JoinHandle<()>>,
}

struct State {
    child: Option<Child>,
    alive: bool,
    epoch: u64,
    port: Option<u16>,
    launch_token: Option<String>,
    cookie: Option<String>,
    session: String,
    open: Vec<String>,
    seen: HashSet<String>,
    written: Vec<String>,
    finished: HashSet<String>,
    turns_started: usize,
    usages: Vec<(String, TurnUsage)>,
    quota_reset_at_ms: Option<i64>,
}

impl DshHarness {
    pub fn start(
        program: impl Into<PathBuf>,
        patch_file: Option<&Path>,
        session_id: Option<&str>,
        environment: &[(&str, &str)],
        work_dir: &Path,
    ) -> Result<Self, HarnessError> {
        let program = program.into();
        let env_vec: Vec<(String, String)> = environment
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        let session = session_id
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .unwrap_or_else(uuid_v4);
        let mut args = vec![
            "--profile".to_string(),
            "web".to_string(),
            "--no-open".to_string(),
        ];
        if let Some(patch) = patch_file {
            args.push("--patch".to_string());
            args.push(patch.display().to_string());
        }
        args.push("--port".to_string());
        args.push("0".to_string());

        let mut cmd = spawn_command(&program, &env_vec);
        cmd.args(&args)
            .current_dir(work_dir)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        for (k, v) in &env_vec {
            cmd.env(k, v);
        }
        let mut child = cmd
            .spawn()
            .map_err(|err| HarnessError::Failed(format!("spawn dsh: {err}")))?;
        let Some(stdout) = child.stdout.take() else {
            anyagents_harness::kill_child(&mut child, &env_vec);
            let _ = child.wait();
            return Err(HarnessError::Failed("dsh stdout missing".into()));
        };
        let shared = Arc::new(Mutex::new(State {
            child: Some(child),
            alive: true,
            epoch: 0,
            port: None,
            launch_token: None,
            cookie: None,
            session,
            open: Vec::new(),
            seen: HashSet::new(),
            written: Vec::new(),
            finished: HashSet::new(),
            turns_started: 0,
            usages: Vec::new(),
            quota_reset_at_ms: None,
        }));
        let wake = Arc::new(Condvar::new());
        let reader_shared = Arc::clone(&shared);
        let reader_wake = Arc::clone(&wake);
        let reader = thread::Builder::new()
            .name("anyagents-dsh-stdout".into())
            .spawn(move || {
                for line in BufReader::new(stdout).lines() {
                    let Ok(line) = line else { break };
                    if let Some((port, token)) = parse_dsh_web_url(&line) {
                        let mut st = lock(&reader_shared);
                        st.port = Some(port);
                        st.launch_token = Some(token);
                        st.epoch = st.epoch.wrapping_add(1);
                        drop(st);
                        reader_wake.notify_all();
                    }
                }
                let mut st = lock(&reader_shared);
                st.alive = false;
                if let Some(child) = st.child.as_mut() {
                    let _ = child.try_wait();
                }
                st.epoch = st.epoch.wrapping_add(1);
                drop(st);
                reader_wake.notify_all();
            })
            .map_err(|err| HarnessError::Failed(err.to_string()))?;

        let poll_shared = Arc::clone(&shared);
        let poll_wake = Arc::clone(&wake);
        let poller = thread::Builder::new()
            .name("anyagents-dsh-poll".into())
            .spawn(move || poll_dsh_turns(poll_shared, poll_wake))
            .map_err(|err| HarnessError::Failed(err.to_string()))?;

        Ok(Self {
            environment: env_vec,
            work_dir: work_dir.to_path_buf(),
            shared,
            wake,
            reader: Some(reader),
            poller: Some(poller),
        })
    }

    pub fn session_id(&self) -> Option<String> {
        let st = lock(&self.shared);
        (!st.session.is_empty()).then(|| st.session.clone())
    }

    fn ensure_connected(&self) -> Result<(u16, String, String), HarnessError> {
        let deadline = Instant::now() + Duration::from_secs(30);
        let (port, token, existing_cookie, session) = {
            let mut st = lock(&self.shared);
            while st.alive && (st.port.is_none() || st.launch_token.is_none()) {
                let now = Instant::now();
                if now >= deadline {
                    return Err(HarnessError::Failed(
                        "timed out waiting for dsh web port".into(),
                    ));
                }
                let (guard, _) = self
                    .wake
                    .wait_timeout(st, deadline - now)
                    .unwrap_or_else(|e| e.into_inner());
                st = guard;
            }
            if !st.alive {
                return Err(HarnessError::Dead);
            }
            (
                st.port.expect("checked"),
                st.launch_token.clone().expect("checked"),
                st.cookie.clone(),
                st.session.clone(),
            )
        };
        if let Some(cookie) = existing_cookie {
            return Ok((port, cookie, session));
        }
        let cookie = dsh_exchange_token(port, &token).map_err(HarnessError::Failed)?;
        let create_args = json!({
            "sessionId": session,
            "cwd": self.work_dir.display().to_string(),
        });
        dsh_rpc(port, &cookie, "session/create", "init", &create_args)
            .map_err(HarnessError::Failed)?;
        lock(&self.shared).cookie = Some(cookie.clone());
        Ok((port, cookie, session))
    }
}

impl Harness for DshHarness {
    fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        {
            let st = lock(&self.shared);
            if !st.alive {
                return Err(HarnessError::Dead);
            }
            if st.seen.contains(&message.id) {
                return Ok(DeliverOutcome::Accepted {
                    turn_finished: st.finished.contains(&message.id),
                });
            }
            if let Some(reset_at_ms) = st.quota_reset_at_ms {
                return Ok(DeliverOutcome::Quota { reset_at_ms });
            }
        }
        let (port, cookie, session) = self.ensure_connected()?;
        let text = compose_text(message);
        let args = json!({
            "sessionId": session,
            "requestId": message.id,
            "content": [{"type": "text", "text": text}],
            "mode": "steer",
        });
        dsh_rpc(port, &cookie, "session/prompt", &message.id, &args)
            .map_err(HarnessError::Failed)?;
        let mut st = lock(&self.shared);
        if st.open.is_empty() {
            st.turns_started += 1;
        }
        if !st.open.contains(&message.id) {
            st.open.push(message.id.clone());
        }
        st.seen.insert(message.id.clone());
        st.written.push(message.id.clone());
        st.epoch = st.epoch.wrapping_add(1);
        let finished = st.finished.contains(&message.id);
        drop(st);
        self.wake.notify_all();
        Ok(DeliverOutcome::Accepted {
            turn_finished: finished,
        })
    }

    fn background(&mut self) -> Result<(), HarnessError> {
        if lock(&self.shared).alive {
            Ok(())
        } else {
            Err(HarnessError::Dead)
        }
    }

    fn resume(&mut self, session: &str) -> Result<(), HarnessError> {
        let (port, cookie, _) = self.ensure_connected()?;
        let args = json!({
            "sessionId": session,
            "cwd": self.work_dir.display().to_string(),
        });
        dsh_rpc(port, &cookie, "session/create", "resume", &args).map_err(HarnessError::Failed)?;
        let mut st = lock(&self.shared);
        st.session = session.to_string();
        st.open.clear();
        Ok(())
    }

    fn cancel(&mut self) -> Result<(), HarnessError> {
        let (port, cookie, session) = self.ensure_connected()?;
        let args = json!({ "sessionId": session });
        dsh_rpc(port, &cookie, "session/cancel", "cancel", &args).map_err(HarnessError::Failed)?;
        let mut st = lock(&self.shared);
        st.open.clear();
        st.epoch = st.epoch.wrapping_add(1);
        drop(st);
        self.wake.notify_all();
        Ok(())
    }

    fn health(&self) -> HarnessHealth {
        let st = lock(&self.shared);
        HarnessHealth {
            process_alive: st.alive,
            turn_state: if st.open.is_empty() {
                TurnState::Idle
            } else {
                TurnState::InTurn
            },
            delivery_mode: DeliveryMode::Steer,
            steer_support: SteerSupport::Yes,
            quota_reset_at_ms: st.quota_reset_at_ms,
            sign_in_fault: None,
        }
    }

    fn clear_quota(&mut self) {
        lock(&self.shared).quota_reset_at_ms = None;
    }

    fn process_id(&self) -> Option<u32> {
        lock(&self.shared).child.as_ref().map(Child::id)
    }

    fn turns_served(&self) -> u32 {
        lock(&self.shared).finished.len() as u32
    }

    fn watch_activity(&self, notify: Arc<dyn Fn() + Send + Sync>) {
        let shared = Arc::clone(&self.shared);
        let wake = Arc::clone(&self.wake);
        let _ = thread::Builder::new()
            .name("anyagents-dsh-watch".into())
            .spawn(move || {
                let mut st = lock(&shared);
                let mut seen = 0u64;
                loop {
                    if !st.alive {
                        drop(st);
                        notify();
                        break;
                    }
                    if st.epoch != seen {
                        seen = st.epoch;
                        drop(st);
                        notify();
                        st = lock(&shared);
                        continue;
                    }
                    st = wake.wait(st).unwrap_or_else(|e| e.into_inner());
                }
            });
    }
}

impl Drop for DshHarness {
    fn drop(&mut self) {
        let child = {
            let mut st = lock(&self.shared);
            st.alive = false;
            st.child.take()
        };
        self.wake.notify_all();
        if let Some(mut child) = child {
            anyagents_harness::kill_child(&mut child, &self.environment);
            let _ = child.wait();
        }
        if let Some(handle) = self.reader.take() {
            let _ = handle.join();
        }
        if let Some(handle) = self.poller.take() {
            let _ = handle.join();
        }
    }
}

fn poll_dsh_turns(shared: Arc<Mutex<State>>, wake: Arc<Condvar>) {
    loop {
        let (port, cookie, session) = {
            let st = lock(&shared);
            let st = wake
                .wait_timeout_while(st, Duration::from_millis(250), |s| {
                    s.alive && (s.open.is_empty() || s.port.is_none() || s.cookie.is_none())
                })
                .unwrap_or_else(|e| e.into_inner())
                .0;
            if !st.alive {
                return;
            }
            if st.open.is_empty() {
                continue;
            }
            let (Some(port), Some(cookie)) = (st.port, st.cookie.clone()) else {
                continue;
            };
            (port, cookie, st.session.clone())
        };
        thread::sleep(Duration::from_millis(250));
        let Ok(resp) = dsh_rpc(port, &cookie, "session/list", "poll", &json!({})) else {
            continue;
        };
        let running = resp
            .pointer("/payload/items")
            .and_then(Value::as_array)
            .and_then(|items| {
                items
                    .iter()
                    .find(|item| item.get("sessionId").and_then(Value::as_str) == Some(&session))
            })
            .and_then(|item| item.get("running"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if !running {
            let mut st = lock(&shared);
            if !st.open.is_empty() {
                let drained: Vec<String> = std::mem::take(&mut st.open);
                for id in drained {
                    st.finished.insert(id.clone());
                    st.usages.push((id, TurnUsage::default()));
                }
                st.epoch = st.epoch.wrapping_add(1);
                drop(st);
                wake.notify_all();
            }
        }
    }
}

pub fn write_dsh_mcp_patch(
    config_dir: &Path,
    model: Option<&str>,
    command: &Path,
    args: &[String],
) -> Result<PathBuf, anyagents_core::Error> {
    let dsh_home = config_dir.join(".dsh");
    std::fs::create_dir_all(&dsh_home)?;
    let patch_path = config_dir.join("dsh-mcp-patch.yml");
    let args_yaml = args
        .iter()
        .map(|a| format!("      - '{}'\n", a.replace('\'', "''")))
        .collect::<String>();
    let mut yaml = String::from("- id: agent-instructions\n  disabled: false\n");
    if let Some(raw_model) = model.map(str::trim).filter(|m| !m.is_empty()) {
        let clean = raw_model
            .strip_prefix("deepseek/")
            .unwrap_or(raw_model)
            .strip_prefix("deepseek-official/")
            .unwrap_or(raw_model);
        yaml.push_str(&format!(
            "- id: agent-default-model\n  config:\n    provider: deepseek-official\n    model: '{clean}'\n"
        ));
    }
    yaml.push_str(&format!(
        "- insert:\n    - id: mcp-anyagents\n      name: '@deepseek-ai/dsh-mcp-client'\n      config:\n        serverName: anyagents\n        transport: stdio\n        command: '{}'\n        args:\n{args_yaml}",
        command.display().to_string().replace('\'', "''")
    ));
    let tmp = patch_path.with_extension("yml.new");
    std::fs::write(&tmp, yaml)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let _ = std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o664));
    }
    std::fs::rename(&tmp, &patch_path)?;
    Ok(patch_path)
}

fn parse_dsh_web_url(line: &str) -> Option<(u16, String)> {
    let idx = line.find("http://127.0.0.1:")?;
    let rest = &line[idx + "http://127.0.0.1:".len()..];
    let (port_str, query_part) = rest.split_once("/?token=")?;
    let port = port_str.trim().parse::<u16>().ok()?;
    let token = query_part.split_whitespace().next()?.trim().to_string();
    (!token.is_empty()).then_some((port, token))
}

fn dsh_exchange_token(port: u16, token: &str) -> Result<String, String> {
    let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port));
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(3))
        .map_err(|e| format!("connect dsh 127.0.0.1:{port}: {e}"))?;
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));
    let req = format!(
        "GET /?token={token} HTTP/1.0\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n"
    );
    stream
        .write_all(req.as_bytes())
        .and_then(|_| stream.flush())
        .map_err(|e| format!("write dsh auth: {e}"))?;
    let mut resp = String::new();
    stream
        .read_to_string(&mut resp)
        .map_err(|e| format!("read dsh auth: {e}"))?;
    for line in resp.lines() {
        let lower = line.to_ascii_lowercase();
        if lower.starts_with("set-cookie:")
            && let Some((_, val)) = line.split_once(':')
            && let Some(cookie_pair) = val.trim().split(';').next()
            && cookie_pair.trim().starts_with("dsh-auth-")
        {
            return Ok(cookie_pair.trim().to_string());
        }
    }
    Err("dsh auth response missing Set-Cookie".into())
}

fn dsh_rpc(
    port: u16,
    cookie: &str,
    method: &str,
    rpc_id: &str,
    args: &Value,
) -> Result<Value, String> {
    let payload = json!({
        "type": "client-request",
        "rpcId": rpc_id,
        "method": method,
        "payload": {
            "args": args
        }
    });
    let body = serde_json::to_vec(&payload).map_err(|e| e.to_string())?;
    let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port));
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(3))
        .map_err(|e| format!("connect dsh 127.0.0.1:{port}: {e}"))?;
    let _ = stream.set_read_timeout(Some(Duration::from_secs(10)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(10)));
    let mut req = format!(
        "POST /api/{method} HTTP/1.0\r\n\
         Host: 127.0.0.1:{port}\r\n\
         Origin: http://127.0.0.1:{port}\r\n\
         Cookie: {cookie}\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    req.extend_from_slice(&body);
    stream
        .write_all(&req)
        .and_then(|_| stream.flush())
        .map_err(|e| format!("write dsh {method}: {e}"))?;
    let mut resp = String::new();
    stream
        .read_to_string(&mut resp)
        .map_err(|e| format!("read dsh {method}: {e}"))?;
    let (head, resp_body) = resp
        .split_once("\r\n\r\n")
        .ok_or_else(|| format!("malformed HTTP response from dsh {method}"))?;
    let status = head
        .lines()
        .next()
        .unwrap_or_default()
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(0);
    if !(200..300).contains(&status) {
        return Err(format!("dsh {method} HTTP {status}: {}", resp_body.trim()));
    }
    let parsed: Value =
        serde_json::from_str(resp_body.trim()).map_err(|e| format!("decode dsh {method}: {e}"))?;
    if parsed.get("ok").and_then(Value::as_bool) == Some(false) {
        let msg = parsed
            .pointer("/error/message")
            .and_then(Value::as_str)
            .unwrap_or("dsh rpc error");
        return Err(format!("dsh {method}: {msg}"));
    }
    Ok(parsed)
}

fn compose_text(message: &OutboundMessage) -> String {
    let context = &message.context;
    let known = context.sender.is_some()
        || context.chat_id.is_some()
        || context.message_id.is_some()
        || context.destination.is_some();
    if !known {
        return marked_message(&message.id, &message.text);
    }
    compose_message(&message.id, context, &message.text)
}

fn uuid_v4() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let pid = u128::from(std::process::id());
    let a = nanos ^ (pid << 64);
    let b = nanos.wrapping_mul(0x9E37_79B9_7F4A_7C15_F39C_C060_5CED_C834);
    let raw = (a & 0xFFFF_FFFF_FFFF_0FFF_FFFF_FFFF_FFFF_FFFF)
        | (0x4u128 << 76)
        | ((b & 0x3FFF_FFFF_FFFF_FFFF) | (0x2u128 << 62));
    let hex = format!("{raw:032x}");
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}
