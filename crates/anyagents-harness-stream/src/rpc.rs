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
//!
//! The child is gone only once its stdout reached its end: a line the host
//! could not write (its pipe broke) was not written, and is answered
//! [`RpcFail::Dead`] to its caller, but every line the child printed before it
//! went is still read, so no answer or proof it gave is lost.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Stdio};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use anyagents_harness::{HarnessError, spawn_command};
use serde_json::{Value, json};

/// What an adapter makes of one notification.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NotificationEffect {
    /// A rate limit with a reset time, in epoch milliseconds.
    pub quota_reset_at_ms: Option<i64>,
    /// The line changed nothing the host reads through the adapter (a
    /// streamed delta or a progress note), so the activity watcher stays
    /// asleep. The default wakes it.
    pub unobserved: bool,
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
    /// When the child was seen to have exited while its stdout stayed open
    /// (a descendant holds it).
    exited_at: Option<Instant>,
    /// Bumped by every line the child prints.
    epoch: u64,
    /// Bumped only by lines the adapter says the host may read, by a quota
    /// and by exit: what the activity watcher waits on.
    changes: u64,
    next_id: i64,
    pending: HashMap<i64, Answer>,
    quota_reset_at_ms: Option<i64>,
    /// Handed to the reader thread once the spawn has returned.
    stdout: Option<ChildStdout>,
}

enum Answer {
    Waiting,
    Async,
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
        child.spawn()?;
        let shared = Arc::clone(&child.shared);
        let wake = Arc::clone(&child.wake);
        let stdin = Arc::clone(&child.stdin);
        let name = launch.name;
        let stdout = lock(&child.shared).take_stdout();
        match thread::Builder::new()
            .name(name)
            .spawn(move || read_stdout(stdout, shared, wake, stdin, on_line, on_request))
        {
            Ok(handle) => child.reader = Some(handle),
            Err(err) => {
                child.stop_child();
                return Err(HarnessError::Failed(err.to_string()));
            }
        }
        Ok(child)
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

    /// Alive until the reader reached stdout's end, or until a child that
    /// exited has kept its stdout open (a descendant holding it) for
    /// [`crate::EXIT_GRACE`]: past that no answer can still arrive from it.
    pub fn alive(&self) -> bool {
        let mut state = lock(&self.shared);
        if state.alive {
            if state.exited_at.is_none()
                && let Some(child) = state.child.as_mut()
                && let Ok(Some(_)) = child.try_wait()
            {
                state.exited_at = Some(Instant::now());
            }
            if state
                .exited_at
                .is_some_and(|exited| exited.elapsed() >= crate::EXIT_GRACE)
            {
                state.alive = false;
                state.changes = state.changes.wrapping_add(1);
                drop(state);
                self.wake.notify_all();
                return false;
            }
        }
        state.alive
    }

    pub fn epoch(&self) -> u64 {
        lock(&self.shared).epoch
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
            return Err(RpcFail::Dead);
        }
        self.await_answer(id, wait)
    }

    /// Send one request without waiting for the answer. When the vendor replies
    /// to that `id`, `on_line` receives a synthetic `"$rpc/response"` event.
    pub fn send_request(&self, method: &str, params: Value) -> Result<i64, RpcFail> {
        let id = {
            let mut state = lock(&self.shared);
            if !state.alive {
                return Err(RpcFail::Dead);
            }
            state.next_id += 1;
            let id = state.next_id;
            state.pending.insert(id, Answer::Async);
            id
        };
        let line = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
        if write_stdin(&self.stdin, &line).is_err() {
            lock(&self.shared).pending.remove(&id);
            return Err(RpcFail::Dead);
        }
        Ok(id)
    }

    /// Send one notification and do not wait.
    pub fn notify(&self, method: &str, params: Value) -> Result<(), RpcFail> {
        let line = json!({"jsonrpc": "2.0", "method": method, "params": params});
        if write_stdin(&self.stdin, &line).is_err() {
            Err(RpcFail::Dead)
        } else {
            Ok(())
        }
    }

    /// Answer a request the vendor sent.
    pub fn answer(&self, id: &Value, result: Value) -> Result<(), RpcFail> {
        let line = json!({"jsonrpc": "2.0", "id": id, "result": result});
        if write_stdin(&self.stdin, &line).is_err() {
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
                Some(Answer::Waiting) | Some(Answer::Async) => {}
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

    /// Call `notify` once at start, after the vendor prints a line the
    /// adapter did not mark [`NotificationEffect::unobserved`], and once when
    /// it exits.
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
                let mut seen = None;
                loop {
                    if !state.alive {
                        drop(state);
                        notify();
                        break;
                    }
                    if seen != Some(state.changes) {
                        seen = Some(state.changes);
                        drop(state);
                        notify();
                        state = lock(&shared);
                        continue;
                    }
                    state = wake.wait(state).unwrap_or_else(|err| err.into_inner());
                }
            });
    }

    /// Stop the child and read what it printed to the end: the reader has
    /// handed every line to the adapter before this returns.
    pub fn stop_child(&mut self) {
        *lock(&self.stdin) = None;
        let mut child = lock(&self.shared).child.take();
        if let Some(child) = child.as_mut() {
            anyagents_harness::kill_child(child, &self.environment);
        }
        if let Some(handle) = self.reader.take() {
            let _ = handle.join();
        }
        if let Some(mut child) = child {
            let _ = child.wait();
        }
        lock(&self.shared).alive = false;
        self.wake.notify_all();
    }

    fn spawn(&mut self) -> Result<(), HarnessError> {
        let mut command = spawn_command(&self.program, &self.environment)?;
        crate::close_inherited_files(&mut command);
        command
            .args(&self.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        if let Some(dir) = &self.work_dir {
            command.current_dir(dir);
        }
        let mut child = command
            .spawn()
            .map_err(|err| HarnessError::Failed(err.to_string()))?;
        let Some(child_stdin) = child.stdin.take() else {
            anyagents_harness::kill_child(&mut child, &self.environment);
            let _ = child.wait();
            return Err(HarnessError::Failed("child stdin missing".into()));
        };
        let Some(stdout) = child.stdout.take() else {
            anyagents_harness::kill_child(&mut child, &self.environment);
            let _ = child.wait();
            return Err(HarnessError::Failed("child stdout missing".into()));
        };
        {
            let mut state = lock(&self.shared);
            state.child = Some(child);
            state.stdout = Some(stdout);
            state.alive = true;
            state.exited_at = None;
        }
        *lock(&self.stdin) = Some(child_stdin);
        Ok(())
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
        let mut observed = false;
        match (method, id) {
            (Some(method), Some(id)) => {
                if let Some(result) = on_request(method, &value) {
                    let answer = json!({"jsonrpc": "2.0", "id": id, "result": result});
                    // An answer that cannot be written leaves the request
                    // unanswered; what the child prints after it is still
                    // read, and its stdout's end reports it gone.
                    let _ = write_stdin(&stdin, &answer);
                }
            }
            (Some(method), None) => {
                let params = value.get("params").cloned().unwrap_or(Value::Null);
                let effect = on_line(&json!({"method": method, "params": params}));
                observed = !effect.unobserved || effect.quota_reset_at_ms.is_some();
                if let Some(reset_at_ms) = effect.quota_reset_at_ms {
                    lock(&shared).quota_reset_at_ms = Some(reset_at_ms);
                }
            }
            (None, Some(id)) => {
                let Some(id) = id.as_i64() else { continue };
                let response = decode_response(&value);
                let is_async = {
                    let mut state = lock(&shared);
                    match state.pending.get(&id) {
                        Some(Answer::Waiting) => {
                            state.pending.insert(
                                id,
                                match &response {
                                    Ok(result) => Answer::Done(result.clone()),
                                    Err(error) => Answer::Err(error.clone()),
                                },
                            );
                            false
                        }
                        Some(Answer::Async) => {
                            state.pending.remove(&id);
                            true
                        }
                        _ => continue,
                    }
                };
                if is_async {
                    let (result, error) = match response {
                        Ok(result) => (result, Value::Null),
                        Err(error) => (Value::Null, error),
                    };
                    let effect = on_line(&json!({
                        "method": "$rpc/response",
                        "params": { "id": id, "result": result, "error": error }
                    }));
                    observed = !effect.unobserved || effect.quota_reset_at_ms.is_some();
                    if let Some(reset_at_ms) = effect.quota_reset_at_ms {
                        lock(&shared).quota_reset_at_ms = Some(reset_at_ms);
                    }
                }
            }
            (None, None) => continue,
        }
        {
            let mut state = lock(&shared);
            state.epoch = state.epoch.wrapping_add(1);
            if observed {
                state.changes = state.changes.wrapping_add(1);
            }
        }
        wake.notify_all();
    }
    {
        let mut state = lock(&shared);
        state.alive = false;
        if let Some(child) = state.child.as_mut() {
            let _ = child.try_wait();
        }
        state.epoch = state.epoch.wrapping_add(1);
        state.changes = state.changes.wrapping_add(1);
    }
    wake.notify_all();
}

fn decode_response(value: &Value) -> Result<Value, Value> {
    match (value.get("result"), value.get("error")) {
        (Some(result), None) => Ok(result.clone()),
        (None, Some(error))
            if error.get("code").and_then(Value::as_i64).is_some()
                && error.get("message").and_then(Value::as_str).is_some() =>
        {
            Err(error.clone())
        }
        _ => Err(
            json!({"code": -32600, "message": "malformed JSON-RPC response: expected exactly one result or valid error"}),
        ),
    }
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

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    fn launch(
        script: String,
        on_line: Arc<dyn Fn(&Value) -> NotificationEffect + Send + Sync>,
    ) -> RpcChild {
        launch_with_request_handler(
            script,
            on_line,
            Arc::new(|_, _| Some(json!({"approved": true}))),
        )
    }

    fn launch_with_request_handler(
        script: String,
        on_line: Arc<dyn Fn(&Value) -> NotificationEffect + Send + Sync>,
        on_request: RequestHandler,
    ) -> RpcChild {
        RpcChild::launch(
            RpcLaunch {
                name: "rpc-fixture".into(),
                program: "/bin/sh".into(),
                args: vec!["-c".into(), script],
                environment: Vec::new(),
                work_dir: None,
            },
            on_line,
            on_request,
        )
        .unwrap()
    }

    fn response_child(response: Value) -> RpcChild {
        launch(
            format!("IFS= read -r request; printf '%s\\n' '{response}'; exec cat >/dev/null"),
            Arc::new(|_| NotificationEffect::default()),
        )
    }

    #[test]
    fn valid_result_including_explicit_null_and_structured_errors_complete_requests() {
        for result in [Value::Null, json!({"ok": true})] {
            let child = response_child(json!({"jsonrpc": "2.0", "id": 1, "result": result}));
            assert_eq!(
                child
                    .request("cancel", json!({}), Duration::from_secs(5))
                    .unwrap(),
                result
            );
        }
        let child = response_child(json!({"id": 1, "error": {"code": 41, "message": "refused"}}));
        assert!(
            matches!(child.request("cancel", json!({}), Duration::from_secs(5)),
            Err(RpcFail::Failed(ref error)) if error["code"] == 41)
        );
    }

    #[test]
    fn missing_conflicting_or_malformed_response_fields_fail_instead_of_returning_null() {
        for fields in [
            json!({}),
            json!({"result": null, "error": {"code": 1, "message": "bad"}}),
            json!({"error": null}),
            json!({"error": "bad"}),
            json!({"error": {"message": "bad"}}),
        ] {
            let mut response = fields;
            response["id"] = json!(1);
            let child = response_child(response);
            assert!(
                matches!(child.request("cancel", json!({}), Duration::from_secs(5)),
                Err(RpcFail::Failed(ref error)) if error["code"] == -32600)
            );
        }
    }

    #[test]
    fn malformed_async_response_is_reported_to_the_adapter_as_an_error() {
        let (tx, rx) = std::sync::mpsc::channel();
        let child = launch(
            "IFS= read -r request; printf '%s\n' '{\"id\":1}'; exec cat >/dev/null".into(),
            Arc::new(move |line| {
                tx.send(line.clone()).unwrap();
                NotificationEffect::default()
            }),
        );
        child.send_request("turn", json!({})).unwrap();
        let event = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(event["method"], "$rpc/response");
        assert_eq!(event["params"]["error"]["code"], -32600);
    }

    /// The host cannot write its answer to the vendor's request: the child
    /// is not reported gone, and what it prints after it is still read (here
    /// the answer to the host's own request), until its stdout ends.
    #[test]
    fn a_reply_that_cannot_be_written_keeps_the_child_read_to_its_end() {
        let (stdin_tx, stdin_rx) =
            std::sync::mpsc::sync_channel::<Arc<Mutex<Option<ChildStdin>>>>(1);
        let stdin_rx = Mutex::new(stdin_rx);
        let (closed_tx, closed_rx) = std::sync::mpsc::channel::<()>();
        let closed_tx = Mutex::new(closed_tx);
        let child = launch_with_request_handler(
            "IFS= read -r request; printf '%s\n' '{\"id\":99,\"method\":\"approval/request\"}'; sleep 0.3; printf '%s\n' '{\"id\":1,\"result\":\"proved\"}'; sleep 0.3".into(),
            Arc::new(|_| NotificationEffect::default()),
            Arc::new(move |_, _| {
                // Close the host writer while handling the vendor's request.
                // A shell closing fd 0 does not guarantee EPIPE while another
                // process still holds an inherited copy of the pipe's read end.
                let stdin = lock(&stdin_rx).recv_timeout(Duration::from_secs(5)).unwrap();
                *lock(&stdin) = None;
                let _ = lock(&closed_tx).send(());
                Some(json!({"approved": true}))
            }),
        );
        stdin_tx.send(Arc::clone(&child.stdin)).unwrap();
        assert_eq!(
            child
                .request("turn", json!({}), Duration::from_secs(5))
                .unwrap(),
            json!("proved")
        );
        closed_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        // The host cannot write any more: the call says so, the child is
        // still reported running until its stdout ends.
        assert!(matches!(
            child.notify("cancel", json!({})),
            Err(RpcFail::Dead)
        ));
        assert!(child.alive());
        let deadline = Instant::now() + Duration::from_secs(5);
        while child.alive() {
            assert!(Instant::now() < deadline, "the child's end was never read");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// A request the host cannot write was not written: its caller hears
    /// `Dead`, but the child is reported gone only once its stdout ends,
    /// after every line it printed is read.
    #[test]
    fn a_request_that_cannot_be_written_does_not_end_the_child() {
        let (tx, rx) = std::sync::mpsc::channel();
        let child = launch(
            "sleep 0.3; printf '%s\n' '{\"method\":\"proof\",\"params\":{\"id\":\"m1\"}}'; sleep 0.2".into(),
            Arc::new(move |line| {
                tx.send(line.clone()).unwrap();
                NotificationEffect::default()
            }),
        );
        *lock(&child.stdin) = None;
        assert!(matches!(
            child.request("turn", json!({}), Duration::from_secs(5)),
            Err(RpcFail::Dead)
        ));
        assert!(child.alive());
        let line = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(line["params"]["id"], "m1");
        let deadline = Instant::now() + Duration::from_secs(5);
        while child.alive() {
            assert!(Instant::now() < deadline, "the child's end was never read");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn successful_required_reply_allows_the_pending_request_to_finish() {
        let child = launch("IFS= read -r request; printf '%s\n' '{\"id\":99,\"method\":\"approval/request\"}'; IFS= read -r reply; printf '%s\n' '{\"id\":1,\"result\":true}'; exec cat >/dev/null".into(),
            Arc::new(|_| NotificationEffect::default()));
        assert_eq!(
            child
                .request("turn", json!({}), Duration::from_secs(5))
                .unwrap(),
            json!(true)
        );
    }
}
