//! In-process stream-json harness host for `HarnessHost::InProcess` under
//! `ANYAGENTS_SANDBOX_MODE=remote_required`.
//!
//! Runs `cloud_harness::run_with_io` or `sandbox_exec::run_with_io` directly
//! inside the coordinator process on a lightweight worker thread over an
//! in-memory pipe — spawning ZERO local OS shim processes (`anyagents
//! cloud-harness`, `anyagents sandbox-exec`, or `anyagents keeper`) on the
//! coordinator pod.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use anyagents_harness::{
    DeliverOutcome, Delivery, Harness, HarnessError, HarnessHealth, OutboundMessage, Proof,
    TurnState, TurnUsage, compose_message, message_id_in_text,
};
use anyagents_harness_claude::{CLOUD_DELIVERY, DELIVERY as CLAUDE_DELIVERY, uuid_for};
use anyagents_keeper::Hosted;
use serde_json::{Value, json};

/// In-memory byte pipe connecting `InProcessRemoteHarness` to a stream-json worker thread.
struct PipeInner {
    buf: VecDeque<u8>,
    closed: bool,
}

struct PipeChannel {
    state: Mutex<PipeInner>,
    cv: Condvar,
}

struct PipeReader {
    ch: Arc<PipeChannel>,
}

struct PipeWriter {
    ch: Arc<PipeChannel>,
}

fn mem_pipe() -> (PipeWriter, PipeReader) {
    let ch = Arc::new(PipeChannel {
        state: Mutex::new(PipeInner {
            buf: VecDeque::new(),
            closed: false,
        }),
        cv: Condvar::new(),
    });
    (
        PipeWriter {
            ch: Arc::clone(&ch),
        },
        PipeReader { ch },
    )
}

impl PipeWriter {
    fn close(&self) {
        let mut st = self.ch.state.lock().unwrap_or_else(|e| e.into_inner());
        st.closed = true;
        self.ch.cv.notify_all();
    }
}

impl Drop for PipeWriter {
    fn drop(&mut self) {
        self.close();
    }
}

impl Write for PipeWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut st = self.ch.state.lock().unwrap_or_else(|e| e.into_inner());
        if st.closed {
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "pipe closed"));
        }
        st.buf.extend(buf);
        self.ch.cv.notify_all();
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Read for PipeReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let mut st = self.ch.state.lock().unwrap_or_else(|e| e.into_inner());
        while st.buf.is_empty() && !st.closed {
            st = self.ch.cv.wait(st).unwrap_or_else(|e| e.into_inner());
        }
        if st.buf.is_empty() {
            return Ok(0);
        }
        let n = st.buf.len().min(out.len());
        for slot in out.iter_mut().take(n) {
            *slot = st.buf.pop_front().unwrap();
        }
        Ok(n)
    }
}

struct SharedState {
    alive: bool,
    open: Vec<String>,
    finished: HashSet<String>,
    consumed: HashSet<String>,
    in_flight: HashSet<String>,
    uuid_ids: HashMap<String, String>,
    session_id: Option<String>,
    usage: TurnUsage,
    turns_served: u32,
    request_seq: u64,
    exit_diag: Option<String>,
    watchers: Vec<Arc<dyn Fn() + Send + Sync>>,
}

impl SharedState {
    fn notify_watchers(&self) {
        for cb in &self.watchers {
            cb();
        }
    }
}

/// In-process stream-json harness driving cloud or remote-sandbox execution
/// on a Rust thread without spawning any local OS child process.
pub struct InProcessRemoteHarness {
    shared: Arc<Mutex<SharedState>>,
    stdin: Arc<Mutex<Option<PipeWriter>>>,
    wake: Arc<Condvar>,
    delivery: Delivery,
    worker: Option<JoinHandle<()>>,
    reader: Option<JoinHandle<()>>,
}

impl InProcessRemoteHarness {
    pub fn start_with_worker<F>(
        cloud: bool,
        label: &'static str,
        worker_fn: F,
    ) -> Result<Self, String>
    where
        F: FnOnce(
                BufReader<Box<dyn Read + Send>>,
                Box<dyn Write + Send>,
            ) -> Result<(), anyagents_core::Error>
            + Send
            + 'static,
    {
        let (in_tx, in_rx) = mem_pipe();
        let (out_tx, out_rx) = mem_pipe();

        let shared = Arc::new(Mutex::new(SharedState {
            alive: true,
            open: Vec::new(),
            finished: HashSet::new(),
            consumed: HashSet::new(),
            in_flight: HashSet::new(),
            uuid_ids: HashMap::new(),
            session_id: None,
            usage: TurnUsage::default(),
            turns_served: 0,
            request_seq: 0,
            exit_diag: None,
            watchers: Vec::new(),
        }));
        let wake = Arc::new(Condvar::new());

        let worker_shared = Arc::clone(&shared);
        let worker_wake = Arc::clone(&wake);
        let worker = thread::Builder::new()
            .name(format!("anyagents-inproc-{label}"))
            .spawn(move || {
                let reader: BufReader<Box<dyn Read + Send>> = BufReader::new(Box::new(in_rx));
                let writer: Box<dyn Write + Send> = Box::new(out_tx);
                let result = worker_fn(reader, writer);
                let mut st = worker_shared.lock().unwrap_or_else(|e| e.into_inner());
                if let Err(err) = result {
                    st.exit_diag = Some(err.to_string());
                }
                st.alive = false;
                st.notify_watchers();
                worker_wake.notify_all();
            })
            .map_err(|e| format!("failed to spawn in-process {label} worker: {e}"))?;

        let reader_shared = Arc::clone(&shared);
        let reader_wake = Arc::clone(&wake);
        let reader = thread::Builder::new()
            .name(format!("anyagents-inproc-{label}-reader"))
            .spawn(move || {
                let br = BufReader::new(out_rx);
                for line in br.lines() {
                    let Ok(line) = line else { break };
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    let Ok(val) = serde_json::from_str::<Value>(trimmed) else {
                        continue;
                    };
                    let mut st = reader_shared.lock().unwrap_or_else(|e| e.into_inner());
                    ingest_event(&mut st, &val);
                    st.notify_watchers();
                    reader_wake.notify_all();
                }
                let mut st = reader_shared.lock().unwrap_or_else(|e| e.into_inner());
                st.alive = false;
                st.notify_watchers();
                reader_wake.notify_all();
            })
            .map_err(|e| format!("failed to spawn in-process {label} reader: {e}"))?;

        Ok(Self {
            shared,
            stdin: Arc::new(Mutex::new(Some(in_tx))),
            wake,
            delivery: if cloud {
                CLOUD_DELIVERY
            } else {
                CLAUDE_DELIVERY
            },
            worker: Some(worker),
            reader: Some(reader),
        })
    }

    pub fn start_cloud(args: crate::cloud_harness::CloudHarnessArgs) -> Result<Self, String> {
        Self::start_with_worker(true, "cloud", move |reader, writer| {
            crate::cloud_harness::run_with_io(args, reader, writer)
        })
    }

    pub fn start_sandbox(args: crate::sandbox_exec::SandboxExecArgs) -> Result<Self, String> {
        Self::start_with_worker(false, "sandbox", move |reader, writer| {
            crate::sandbox_exec::run_with_io(args, reader, writer)
        })
    }

    fn write_line(&self, line: &str) -> Result<(), HarnessError> {
        let mut guard = self.stdin.lock().unwrap_or_else(|e| e.into_inner());
        let Some(writer) = guard.as_mut() else {
            return Err(HarnessError::Dead);
        };
        writeln!(writer, "{line}").map_err(|_| HarnessError::Dead)?;
        writer.flush().map_err(|_| HarnessError::Dead)?;
        Ok(())
    }
}

fn ingest_event(st: &mut SharedState, val: &Value) {
    if let Some(sid) = val
        .get("session_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        st.session_id = Some(sid.to_string());
    }
    let kind = val.get("type").and_then(Value::as_str).unwrap_or("");
    match kind {
        "user" => {
            if let Some(uuid) = val.get("uuid").and_then(Value::as_str)
                && let Some(id) = st.uuid_ids.get(uuid).cloned()
            {
                st.in_flight.remove(&id);
                st.consumed.insert(id);
            } else if let Some(text) = val
                .pointer("/message/content")
                .and_then(Value::as_str)
                .and_then(message_id_in_text)
            {
                let id = text.to_string();
                st.in_flight.remove(&id);
                st.consumed.insert(id);
            }
        }
        "result" => {
            let drained: Vec<String> = st.open.drain(..).collect();
            for id in drained {
                st.in_flight.remove(&id);
                st.consumed.insert(id.clone());
                st.finished.insert(id);
            }
            if let Some(usage) = val.get("usage") {
                st.usage.input_tokens = st.usage.input_tokens.saturating_add(
                    usage
                        .get("input_tokens")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                );
                st.usage.output_tokens = st.usage.output_tokens.saturating_add(
                    usage
                        .get("output_tokens")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                );
                st.usage.cache_read_tokens = st.usage.cache_read_tokens.saturating_add(
                    usage
                        .get("cache_read_input_tokens")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                );
                st.usage.cache_write_tokens = st.usage.cache_write_tokens.saturating_add(
                    usage
                        .get("cache_creation_input_tokens")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                );
            }
            st.turns_served = st.turns_served.saturating_add(1);
        }
        _ => {}
    }
}

impl Drop for InProcessRemoteHarness {
    fn drop(&mut self) {
        self.stop();
    }
}

impl Hosted for InProcessRemoteHarness {
    fn session_id(&self) -> Option<String> {
        self.shared
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .session_id
            .clone()
    }
}

impl Harness for InProcessRemoteHarness {
    fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        {
            let st = self.shared.lock().unwrap_or_else(|e| e.into_inner());
            if st.finished.contains(&message.id) {
                return Ok(DeliverOutcome::Accepted {
                    turn_finished: true,
                });
            }
            if st.consumed.contains(&message.id) {
                return Ok(DeliverOutcome::Accepted {
                    turn_finished: false,
                });
            }
            if !st.alive {
                return Err(HarnessError::Dead);
            }
        }
        let msg_uuid = uuid_for(&message.id);
        let content = compose_message(&message.id, &message.context, &message.text);
        let line = json!({
            "type": "user",
            "uuid": msg_uuid,
            "message": {
                "role": "user",
                "content": content,
            }
        })
        .to_string();
        {
            let mut st = self.shared.lock().unwrap_or_else(|e| e.into_inner());
            st.uuid_ids.insert(msg_uuid, message.id.clone());
            st.in_flight.insert(message.id.clone());
            if !st.open.contains(&message.id) {
                st.open.push(message.id.clone());
            }
        }
        self.write_line(&line)?;

        // Wait briefly for replay echo / immediate completion (matching ClaudeHarness).
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut st = self.shared.lock().unwrap_or_else(|e| e.into_inner());
        loop {
            if st.finished.contains(&message.id) {
                return Ok(DeliverOutcome::Accepted {
                    turn_finished: true,
                });
            }
            if st.consumed.contains(&message.id) {
                return Ok(DeliverOutcome::Accepted {
                    turn_finished: false,
                });
            }
            if !st.alive {
                return Err(HarnessError::Dead);
            }
            let now = Instant::now();
            if now >= deadline {
                return Ok(DeliverOutcome::InFlight);
            }
            st = self
                .wake
                .wait_timeout(st, deadline - now)
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
    }

    fn proven(&self, message_id: &str) -> Option<Proof> {
        let st = self.shared.lock().unwrap_or_else(|e| e.into_inner());
        if st.finished.contains(message_id) {
            return Some(Proof::Taken {
                turn_finished: true,
            });
        }
        if st.consumed.contains(message_id) {
            return Some(Proof::Taken {
                turn_finished: false,
            });
        }
        None
    }

    fn stop(&mut self) {
        if let Some(writer) = self.stdin.lock().unwrap_or_else(|e| e.into_inner()).take() {
            writer.close();
        }
        if let Some(handle) = self.worker.take() {
            let _ = handle.join();
        }
        if let Some(handle) = self.reader.take() {
            let _ = handle.join();
        }
    }

    fn stop_verified(&mut self) -> Result<(), HarnessError> {
        self.stop();
        Ok(())
    }

    fn background(&mut self) -> Result<(), HarnessError> {
        Ok(())
    }

    fn resume(&mut self, session: &str) -> Result<(), HarnessError> {
        let mut st = self.shared.lock().unwrap_or_else(|e| e.into_inner());
        st.session_id = Some(session.to_string());
        Ok(())
    }

    fn cancel(&mut self) -> Result<(), HarnessError> {
        let req_id = {
            let mut st = self.shared.lock().unwrap_or_else(|e| e.into_inner());
            st.request_seq += 1;
            format!("inproc-cancel-{}", st.request_seq)
        };
        let line = json!({
            "type": "control_request",
            "request_id": req_id,
            "request": { "subtype": "interrupt" }
        })
        .to_string();
        self.write_line(&line)
    }

    fn health(&self) -> HarnessHealth {
        let st = self.shared.lock().unwrap_or_else(|e| e.into_inner());
        HarnessHealth {
            process_alive: st.alive,
            turn_state: if st.open.is_empty() {
                TurnState::Idle
            } else {
                TurnState::InTurn
            },
            delivery: self.delivery.clone(),
            quota_reset_at_ms: None,
            sign_in_fault: None,
        }
    }

    fn served_usage(&self) -> TurnUsage {
        self.shared
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .usage
            .clone()
    }

    fn turns_served(&self) -> u32 {
        self.shared
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .turns_served
    }

    fn exit_diagnostic(&self) -> Option<String> {
        self.shared
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .exit_diag
            .clone()
    }

    fn process_id(&self) -> Option<u32> {
        // Runs in-process with zero local OS child shim process.
        None
    }

    fn watch_activity(&self, notify: Arc<dyn Fn() + Send + Sync>) {
        let mut st = self.shared.lock().unwrap_or_else(|e| e.into_inner());
        st.watchers.push(notify);
    }
}
