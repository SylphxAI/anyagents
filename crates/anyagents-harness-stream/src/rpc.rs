//! One long-lived vendor child that speaks JSON-RPC over its stdin and stdout,
//! one JSON object per line.
//!
//! The adapter decides what a notification means. This crate owns the process:
//! spawning it, sending a request and waiting for its own answer, raising a
//! notification, reading every line, and answering the vendor's own server
//! requests with what the adapter returns.
//!
//! A line with an `id` and no `method` is an answer to one of our requests. A
//! line with a `method` is a notification. A line with both is a request from
//! the vendor: the adapter's `on_request` returns the result, or `None` to
//! answer nothing.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use anyagents_harness::HarnessError;
use serde_json::{Value, json};

/// What an adapter makes of one notification.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NotificationEffect {
    /// A rate limit with a reset time, in epoch milliseconds.
    pub quota_reset_at_ms: Option<i64>,
}

/// How to start one vendor child.
pub struct RpcLaunch {
    /// Thread name, for the reader and watcher.
    pub name: String,
    pub program: PathBuf,
    pub args: Vec<String>,
    pub environment: Vec<(String, String)>,
    pub work_dir: Option<PathBuf>,
}

/// One vendor child speaking JSON-RPC.
pub struct RpcChild {
    program: PathBuf,
    args: Vec<String>,
    environment: Vec<(String, String)>,
    work_dir: Option<PathBuf>,
    shared: Arc<Mutex<State>>,
    stdin: Arc<Mutex<Option<ChildStdin>>>,
    wake: Arc<Condvar>,
    reader: Option<JoinHandle<()>>,
}

#[derive(Default)]
struct State {
    child: Option<Child>,
    alive: bool,
    /// Notifications the current launch printed, for a restart's outcome.
    lines: usize,
    epoch: u64,
    next_id: i64,
    pending: HashMap<i64, Answer>,
    quota_reset_at_ms: Option<i64>,
    /// Handed to the reader thread once the spawn has returned.
    stdout: Option<ChildStdout>,
}

enum Answer {
    Waiting,
    Done(Value),
    Err(Value),
}

/// Why one request did not come back with a result.
#[derive(Debug)]
pub enum RpcFail {
    Dead,
    TimedOut,
    Failed(Value),
}

impl RpcFail {
    pub fn reason(&self) -> String {
        match self {
            Self::Dead => "harness process is not running".to_string(),
            Self::TimedOut => "the vendor did not answer".to_string(),
            Self::Failed(error) => {
                let message = error
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("request failed");
                match error.get("code") {
                    Some(code) if !code.is_null() => format!("{code} {message}"),
                    _ => message.to_string(),
                }
            }
        }
    }
}

/// Answers one JSON-RPC request a vendor child sends; `None` leaves it unanswered.
pub type RequestHandler = Arc<dyn Fn(&str, &Value) -> Option<Value> + Send + Sync>;

impl RpcChild {
    /// Spawn the child. Every line it prints goes to `on_line`; a request it
    /// sends goes to `on_request`.
    pub fn launch(
        launch: RpcLaunch,
        on_line: Arc<dyn Fn(&Value) -> NotificationEffect + Send + Sync>,
        on_request: RequestHandler,
    ) -> Result<Self, HarnessError> {
        let mut child = Self {
            program: launch.program,
            args: launch.args,
            environment: launch.environment,
            work_dir: launch.work_dir,
            shared: Arc::new(Mutex::new(State::default())),
            stdin: Arc::new(Mutex::new(None)),
            wake: Arc::new(Condvar::new()),
            reader: None,
        };
        child.spawn(&on_line, &on_request)?;
        Ok(child)
    }

    /// Start the child again, reading it exactly as the first launch did. Only
    /// a child that is no longer running starts; a running one is never doubled.
    pub fn restart(
        &mut self,
        on_line: &Arc<dyn Fn(&Value) -> NotificationEffect + Send + Sync>,
        on_request: &RequestHandler,
    ) -> Result<(), HarnessError> {
        self.spawn(on_line, on_request)
    }

    fn spawn(
        &mut self,
        on_line: &Arc<dyn Fn(&Value) -> NotificationEffect + Send + Sync>,
        on_request: &RequestHandler,
    ) -> Result<(), HarnessError> {
        if self.alive() {
            return Err(HarnessError::Failed(
                "the child is still running; it is not started twice".into(),
            ));
        }
        let mut command = Command::new(&self.program);
        crate::close_inherited_files(&mut command);
        command
            .args(&self.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        if let Some(dir) = &self.work_dir {
            command.current_dir(dir);
        }
        for (key, value) in &self.environment {
            command.env(key, value);
        }
        let mut child = command
            .spawn()
            .map_err(|err| HarnessError::Failed(err.to_string()))?;
        let Some(child_stdin) = child.stdin.take() else {
            let _ = child.kill();
            let _ = child.wait();
            return Err(HarnessError::Failed("child stdin missing".into()));
        };
        let Some(stdout) = child.stdout.take() else {
            let _ = child.kill();
            let _ = child.wait();
            return Err(HarnessError::Failed("child stdout missing".into()));
        };
        {
            let mut state = lock(&self.shared);
            state.child = Some(child);
            state.stdout = Some(stdout);
            state.alive = true;
            state.lines = 0;
        }
        *lock(&self.stdin) = Some(child_stdin);
        let shared = Arc::clone(&self.shared);
        let wake = Arc::clone(&self.wake);
        let stdin = Arc::clone(&self.stdin);
        let line = Arc::clone(on_line);
        let request = Arc::clone(on_request);
        let name = format!("{}-reader", self.program.display());
        let stdout = lock(&self.shared).take_stdout();
        match thread::Builder::new()
            .name(name)
            .spawn(move || read_stdout(stdout, shared, wake, stdin, line, request))
        {
            Ok(handle) => self.reader = Some(handle),
            Err(err) => {
                self.stop_child();
                return Err(HarnessError::Failed(err.to_string()));
            }
        }
        Ok(())
    }

    pub fn program(&self) -> &Path {
        &self.program
    }

    pub fn args_record(&self) -> &[String] {
        &self.args
    }

    pub fn environment(&self) -> &[(String, String)] {
        &self.environment
    }

    pub fn alive(&self) -> bool {
        lock(&self.shared).alive
    }

    pub fn epoch(&self) -> u64 {
        lock(&self.shared).epoch
    }

    /// Notifications the current launch printed. An answer to one of our own
    /// requests is not counted: only the vendor speaking on its own proves it
    /// took the session it was given.
    pub fn lines(&self) -> usize {
        lock(&self.shared).lines
    }

    pub fn process_id(&self) -> Option<u32> {
        lock(&self.shared).child.as_ref().map(Child::id)
    }

    /// Send one request and wait for the answer to that id.
    pub fn request(&self, method: &str, params: Value, wait: Duration) -> Result<Value, RpcFail> {
        let id = {
            let mut state = lock(&self.shared);
            if !state.alive {
                return Err(RpcFail::Dead);
            }
            state.next_id += 1;
            let id = state.next_id;
            state.pending.insert(id, Answer::Waiting);
            id
        };
        let line = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
        if write_stdin(&self.stdin, &line).is_err() {
            lock(&self.shared).pending.remove(&id);
            self.mark_dead();
            return Err(RpcFail::Dead);
        }
        self.await_answer(id, wait)
    }

    /// Send one notification and do not wait.
    pub fn notify(&self, method: &str, params: Value) -> Result<(), RpcFail> {
        let line = json!({"jsonrpc": "2.0", "method": method, "params": params});
        if write_stdin(&self.stdin, &line).is_err() {
            self.mark_dead();
            Err(RpcFail::Dead)
        } else {
            Ok(())
        }
    }

    /// Answer a request the vendor sent.
    pub fn answer(&self, id: &Value, result: Value) -> Result<(), RpcFail> {
        let line = json!({"jsonrpc": "2.0", "id": id, "result": result});
        if write_stdin(&self.stdin, &line).is_err() {
            self.mark_dead();
            Err(RpcFail::Dead)
        } else {
            Ok(())
        }
    }

    /// Wait for the answer to `id` that was already sent.
    pub fn await_answer(&self, id: i64, wait: Duration) -> Result<Value, RpcFail> {
        let deadline = Instant::now() + wait;
        let mut state = lock(&self.shared);
        loop {
            match state.pending.get(&id) {
                Some(Answer::Done(_)) => {
                    let Some(Answer::Done(value)) = state.pending.remove(&id) else {
                        return Err(RpcFail::TimedOut);
                    };
                    return Ok(value);
                }
                Some(Answer::Err(_)) => {
                    let Some(Answer::Err(error)) = state.pending.remove(&id) else {
                        return Err(RpcFail::TimedOut);
                    };
                    return Err(RpcFail::Failed(error));
                }
                Some(Answer::Waiting) => {}
                None => return Err(RpcFail::TimedOut),
            }
            if !state.alive {
                state.pending.remove(&id);
                return Err(RpcFail::Dead);
            }
            let now = Instant::now();
            if now >= deadline {
                state.pending.remove(&id);
                return Err(RpcFail::TimedOut);
            }
            let (guard, _) = self
                .wake
                .wait_timeout(state, deadline - now)
                .unwrap_or_else(|err| err.into_inner());
            state = guard;
        }
    }

    /// Wait until `read` answers `true`, or `timeout` passes. It runs whenever
    /// the child prints a line, and stops early when the child exits.
    pub fn wait_until(&self, timeout: Duration, mut read: impl FnMut() -> bool) -> bool {
        let mut state = lock(&self.shared);
        let deadline = Instant::now() + timeout;
        loop {
            if read() {
                return true;
            }
            let now = Instant::now();
            if now >= deadline || !state.alive {
                return false;
            }
            let (guard, _) = self
                .wake
                .wait_timeout(state, deadline - now)
                .unwrap_or_else(|err| err.into_inner());
            state = guard;
        }
    }

    /// The quota wait the child last reported, when it reported one.
    pub fn quota_reset_at_ms(&self) -> Option<i64> {
        lock(&self.shared).quota_reset_at_ms
    }

    pub fn set_quota(&self, reset_at_ms: Option<i64>) {
        lock(&self.shared).quota_reset_at_ms = reset_at_ms;
    }

    /// Call `notify` after the vendor prints a line and once when it exits.
    ///
    /// The callback runs on a background thread. It must not call back into
    /// this child.
    pub fn watch_activity(&self, notify: Arc<dyn Fn() + Send + Sync>) {
        let shared = Arc::clone(&self.shared);
        let wake = Arc::clone(&self.wake);
        let _ = thread::Builder::new()
            .name("anyagents-rpc-watch".to_string())
            .spawn(move || {
                let mut state = lock(&shared);
                let mut seen = 0u64;
                loop {
                    if !state.alive {
                        drop(state);
                        notify();
                        break;
                    }
                    if state.epoch != seen {
                        seen = state.epoch;
                        drop(state);
                        notify();
                        state = lock(&shared);
                        continue;
                    }
                    state = wake.wait(state).unwrap_or_else(|err| err.into_inner());
                }
            });
    }

    pub fn mark_dead(&self) {
        lock(&self.shared).alive = false;
        self.wake.notify_all();
    }

    pub fn stop_child(&mut self) {
        *lock(&self.stdin) = None;
        {
            let mut state = lock(&self.shared);
            state.alive = false;
            if let Some(child) = state.child.as_mut() {
                let _ = child.kill();
            }
        }
        self.wake.notify_all();
        if let Some(handle) = self.reader.take() {
            let _ = handle.join();
        }
        let mut state = lock(&self.shared);
        if let Some(mut child) = state.child.take() {
            let _ = child.wait();
        }
        state.alive = false;
    }
}

impl Drop for RpcChild {
    fn drop(&mut self) {
        self.stop_child();
    }
}

impl State {
    pub fn take_stdout(&mut self) -> ChildStdout {
        self.stdout
            .take()
            .expect("the reader owns stdout from its thread's start")
    }
}

fn read_stdout(
    stdout: ChildStdout,
    shared: Arc<Mutex<State>>,
    wake: Arc<Condvar>,
    stdin: Arc<Mutex<Option<ChildStdin>>>,
    on_line: Arc<dyn Fn(&Value) -> NotificationEffect + Send + Sync>,
    on_request: RequestHandler,
) {
    for line in BufReader::new(stdout).lines() {
        let Ok(line) = line else { break };
        if line.trim().is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let method = value.get("method").and_then(Value::as_str);
        let id = value.get("id");
        match (method, id) {
            (Some(method), Some(id)) => {
                if let Some(result) = on_request(method, &value) {
                    let answer = json!({"jsonrpc": "2.0", "id": id, "result": result});
                    let _ = write_stdin(&stdin, &answer);
                }
            }
            (Some(method), None) => {
                let params = value.get("params").cloned().unwrap_or(Value::Null);
                let effect = on_line(&json!({"method": method, "params": params}));
                if let Some(reset_at_ms) = effect.quota_reset_at_ms {
                    lock(&shared).quota_reset_at_ms = Some(reset_at_ms);
                }
            }
            (None, Some(id)) => {
                let Some(id) = id.as_i64() else { continue };
                let mut state = lock(&shared);
                if !matches!(state.pending.get(&id), Some(Answer::Waiting)) {
                    continue;
                }
                if let Some(error) = value.get("error") {
                    state.pending.insert(id, Answer::Err(error.clone()));
                } else {
                    let result = value.get("result").cloned().unwrap_or(Value::Null);
                    state.pending.insert(id, Answer::Done(result));
                }
            }
            (None, None) => continue,
        }
        {
            let mut state = lock(&shared);
            if method.is_some() && value.get("id").is_none() {
                state.lines += 1;
            }
            state.epoch = state.epoch.wrapping_add(1);
        }
        wake.notify_all();
    }
    {
        let mut state = lock(&shared);
        state.alive = false;
        state.epoch = state.epoch.wrapping_add(1);
    }
    wake.notify_all();
}

fn write_stdin(slot: &Mutex<Option<ChildStdin>>, value: &Value) -> std::io::Result<()> {
    let mut guard = lock(slot);
    let Some(stdin) = guard.as_mut() else {
        return Err(std::io::Error::other("stdin closed"));
    };
    writeln!(stdin, "{value}")?;
    stdin.flush()
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|err| err.into_inner())
}
